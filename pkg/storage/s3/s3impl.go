package s3

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	"github.com/yandex-cloud/k8s-csi-s3/pkg/mounter"
	"github.com/yandex-cloud/k8s-csi-s3/pkg/s3"
	"github.com/yandex-cloud/k8s-csi-s3/pkg/storage/registry"
	"github.com/yandex-cloud/k8s-csi-s3/pkg/storage/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	registry.Register("s3", create)
}

func create(config map[string]string) (types.Storage, error) {
	// TODO(x.zhou): check config fields
	return &s3Storage{
		config: config,
	}, nil
}

type s3Storage struct {
	config map[string]string
}

func (s *s3Storage) mergedConfig(params map[string]string) map[string]string {
	ret := map[string]string{}
	for k, v := range s.config {
		ret[k] = v
	}
	for k, v := range params {
		ret[k] = v
	}
	return ret
}

func (s *s3Storage) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (volumeID string, err error) {
	params := s.mergedConfig(req.GetParameters())
	volumeID = sanitizeVolumeID(req.GetName())
	bucketName := volumeID
	prefix := ""

	// check if bucket name is overridden
	if params[mounter.BucketKey] != "" {
		bucketName = params[mounter.BucketKey]
		prefix = volumeID
		volumeID = path.Join(bucketName, prefix)
	}

	client, err := s3.NewClientFromSecret(s.config)
	if err != nil {
		return volumeID, fmt.Errorf("failed to initialize S3 client: %s", err)
	}

	exists, err := client.BucketExists(bucketName)
	if err != nil {
		return volumeID, fmt.Errorf("failed to check if bucket %s exists: %v", volumeID, err)
	}

	if !exists {
		if err = client.CreateBucket(bucketName); err != nil {
			return volumeID, fmt.Errorf("failed to create bucket %s: %v", bucketName, err)
		}
	}

	if err = client.CreatePrefix(bucketName, prefix); err != nil {
		return volumeID, fmt.Errorf("failed to create prefix %s: %v", prefix, err)
	}
	return volumeID, nil
}

func (s *s3Storage) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) error {
	volumeID := req.GetVolumeId()
	bucketName, prefix := volumeIDToBucketPrefix(volumeID)

	client, err := s3.NewClientFromSecret(s.config)
	if err != nil {
		return fmt.Errorf("failed to initialize S3 client: %s", err)
	}

	var deleteErr error
	if prefix == "" {
		// prefix is empty, we delete the whole bucket
		if err := client.RemoveBucket(bucketName); err != nil && err.Error() != "The specified bucket does not exist" {
			deleteErr = err
		}
		glog.V(4).Infof("Bucket %s removed", bucketName)
	} else {
		if err := client.RemovePrefix(bucketName, prefix); err != nil {
			deleteErr = fmt.Errorf("unable to remove prefix: %w", err)
		}
		glog.V(4).Infof("Prefix %s removed", prefix)
	}

	if deleteErr != nil {
		return deleteErr
	}

	return nil
}

func (s *s3Storage) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) error {
	bucketName, _ := volumeIDToBucketPrefix(req.GetVolumeId())

	client, err := s3.NewClientFromSecret(s.config)
	if err != nil {
		return fmt.Errorf("failed to initialize S3 client: %s", err)
	}
	exists, err := client.BucketExists(bucketName)
	if err != nil {
		return err
	}

	if !exists {
		// return an error if the bucket of the requested volume does not exist
		return status.Error(codes.NotFound, fmt.Sprintf("bucket of volume with id %s does not exist", req.GetVolumeId()))
	}
	return nil
}

func (s *s3Storage) MountStageVolume(ctx context.Context,
	volumeID string, stagingTargetPath string, volumeContext map[string]string) error {
	bucketName, prefix := volumeIDToBucketPrefix(volumeID)

	// glog.Infof("Mounting volume %s, staging target path: %s",
	// 	volumeID, stagingTargetPath)

	client, err := s3.NewClientFromSecret(s.config)
	if err != nil {
		return fmt.Errorf("failed to initialize S3 client: %s", err)
	}

	meta := getMeta(bucketName, prefix, volumeContext)
	mounter, err := mounter.New(meta, client.Config)
	if err != nil {
		return err
	}
	if err := mounter.Mount(stagingTargetPath, volumeID); err != nil {
		return err
	}
	return nil
}

func sanitizeVolumeID(volumeID string) string {
	volumeID = strings.ToLower(volumeID)
	if len(volumeID) > 63 {
		h := sha1.New()
		io.WriteString(h, volumeID)
		volumeID = hex.EncodeToString(h.Sum(nil))
	}
	return volumeID
}

// volumeIDToBucketPrefix returns the bucket name and prefix based on the volumeID.
// Prefix is empty if volumeID does not have a slash in the name.
func volumeIDToBucketPrefix(volumeID string) (string, string) {
	// if the volumeID has a slash in it, this volume is
	// stored under a certain prefix within the bucket.
	splitVolumeID := strings.SplitN(volumeID, "/", 2)
	if len(splitVolumeID) > 1 {
		return splitVolumeID[0], splitVolumeID[1]
	}

	return volumeID, ""
}

func getMeta(bucketName, prefix string, context map[string]string) *s3.FSMeta {
	mountOptions := make([]string, 0)
	mountOptStr := context[mounter.OptionsKey]
	if mountOptStr != "" {
		re, _ := regexp.Compile(`([^\s"]+|"([^"\\]+|\\")*")+`)
		re2, _ := regexp.Compile(`"([^"\\]+|\\")*"`)
		re3, _ := regexp.Compile(`\\(.)`)
		for _, opt := range re.FindAll([]byte(mountOptStr), -1) {
			// Unquote options
			opt = re2.ReplaceAllFunc(opt, func(q []byte) []byte {
				return re3.ReplaceAll(q[1:len(q)-1], []byte("$1"))
			})
			mountOptions = append(mountOptions, string(opt))
		}
	}
	capacity, _ := strconv.ParseInt(context["capacity"], 10, 64)
	return &s3.FSMeta{
		BucketName:    bucketName,
		Prefix:        prefix,
		Mounter:       context[mounter.TypeKey],
		MountOptions:  mountOptions,
		CapacityBytes: capacity,
	}
}
