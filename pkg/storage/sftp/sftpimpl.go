package sftp

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	"github.com/yandex-cloud/k8s-csi-s3/pkg/mounter"
	"github.com/yandex-cloud/k8s-csi-s3/pkg/storage/internal/util"
	"github.com/yandex-cloud/k8s-csi-s3/pkg/storage/registry"
	"github.com/yandex-cloud/k8s-csi-s3/pkg/storage/types"
)

func init() {
	registry.Register("sftp", create)
}

func create(m map[string]string) (types.Storage, error) {
	conf := &sftpConfig{}
	err := util.ConvertConfig(m, conf)
	if err != nil {
		return nil, err
	}
	if conf.Port == 0 {
		conf.Port = 22
	}
	// TODO(x.zhou): verify fields
	return &sftpStorage{
		config: conf,
	}, nil
}

type sftpConfig struct {
	util.RcloneOptions `json:",inline"`
	Host               string  `json:"host"`
	Port               int     `json:"port,string"`
	User               string  `json:"user"`
	Password           *string `json:"password,omitempty"`
	PrivateKey         *string `json:"private_key,omitempty"`
	PathPrefix         string  `json:"path_prefix"`
}

type sftpStorage struct {
	config *sftpConfig
}

func rootPath(name string, prefix string) string {
	var root string
	if prefix == "" {
		root = "/" + name
	} else {
		root = path.Join(prefix, name)
	}
	return root
}

func (s *sftpStorage) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (volumeID string, err error) {
	name := util.SanitizeVolumeID(req.GetName())
	prefix := s.config.PathPrefix
	if prefix != "" {
		volumeID = path.Join(name, prefix)
	}

	client, err := newClient(s.config)
	if err != nil {
		return volumeID, err
	}

	root := rootPath(name, prefix)
	client.MkdirAll(root) // ignore errors
	if info, err := client.Stat(root); err != nil {
		return volumeID, fmt.Errorf("failed to check root: %s", err)
	} else {
		if !info.IsDir() {
			return volumeID, fmt.Errorf("root path %s is not a directory", root)
		}
	}

	return volumeID, nil
}

func (s *sftpStorage) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) error {
	client, err := newClient(s.config)
	if err != nil {
		return err
	}
	name, prefix := util.VolumeIDToBucketPrefix(req.GetVolumeId())
	root := rootPath(name, prefix)
	err = client.RemoveAll(root)
	if err != nil {
		return err
	}
	glog.V(4).Infof("Root path %s removed", root)
	return nil
}

func (s *sftpStorage) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) error {
	client, err := newClient(s.config)
	if err != nil {
		return err
	}
	name, prefix := util.VolumeIDToBucketPrefix(req.GetVolumeId())
	root := rootPath(name, prefix)
	info, err := client.Stat(root)
	if err != nil {
		// path not exists or error
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("root path of volume with id %s is not a directory", req.GetVolumeId())
	}
	return nil
}

func (s *sftpStorage) MountStageVolume(ctx context.Context,
	volumeID string, stagingTargetPath string, volumeContext map[string]string) error {
	name, prefix := util.VolumeIDToBucketPrefix(volumeID)
	root := rootPath(name, prefix)

	fn := func(target string) (args []string, envs map[string]string) {
		args = []string{
			"mount",
			fmt.Sprintf(":sftp:%s", root),
			target,
			"--daemon",
			fmt.Sprintf("--sftp-host=%s", s.config.Host),
			fmt.Sprintf("--sftp-user=%s", s.config.User),
			fmt.Sprintf("--sftp-port=%d", s.config.Port),
			"--allow-other",
			"--vfs-cache-mode=minimal",
			"-vv",
		}
		if s.config.Options != "" {
			extraArgs := util.ParseOptions(s.config.Options)
			args = util.MergeRcloneOptions(args, extraArgs)
		}

		envs = map[string]string{}
		if s.config.Password != nil {
			envs["RCLONE_SFTP_PASS"] = *s.config.Password
		}
		if s.config.PrivateKey != nil {
			envs["RCLONE_SFTP_KEY_PEM"] = strings.ReplaceAll(*s.config.PrivateKey, "\n", "\\n")
		}
		return
	}
	mounter, err := mounter.NewRcloneMounter(fn)
	if err != nil {
		return err
	}
	if err := mounter.Mount(stagingTargetPath, volumeID); err != nil {
		return err
	}
	return nil
}
