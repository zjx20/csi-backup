package mounter

import (
	"fmt"
	"os"
	"path"

	"github.com/yandex-cloud/k8s-csi-s3/pkg/s3"
)

type RcloneMountArgumentsFn func(target string) (args []string, envs map[string]string)

// Implements Mounter
type rcloneMounter struct {
	fn RcloneMountArgumentsFn
}

const (
	rcloneCmd = "rclone"
)

func newRcloneMounter(meta *s3.FSMeta, cfg *s3.Config) (Mounter, error) {
	fn := func(target string) (args []string, envs map[string]string) {
		args = []string{
			"mount",
			fmt.Sprintf(":s3:%s", path.Join(meta.BucketName, meta.Prefix)),
			fmt.Sprintf("%s", target),
			"--daemon",
			"--s3-provider=AWS",
			"--s3-env-auth=true",
			fmt.Sprintf("--s3-endpoint=%s", cfg.Endpoint),
			"--allow-other",
			"--vfs-cache-mode=writes",
		}
		if cfg.Region != "" {
			args = append(args, fmt.Sprintf("--s3-region=%s", cfg.Region))
		}
		args = append(args, meta.MountOptions...)
		envs = map[string]string{
			"AWS_ACCESS_KEY_ID":     cfg.AccessKeyID,
			"AWS_SECRET_ACCESS_KEY": cfg.SecretAccessKey,
		}
		return
	}
	return NewRcloneMounter(fn)
}

func (rclone *rcloneMounter) Mount(target, volumeID string) error {
	args, envs := rclone.fn(target)
	for k, v := range envs {
		// TODO(x.zhou): don't use os.Setenv(), set envs to exec.Cmd
		os.Setenv(k, v)
	}
	return fuseMount(target, rcloneCmd, args)
}

func NewRcloneMounter(fn RcloneMountArgumentsFn) (Mounter, error) {
	return &rcloneMounter{
		fn: fn,
	}, nil
}
