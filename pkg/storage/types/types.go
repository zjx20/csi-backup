package types

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

type Storage interface {
	CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (volumeID string, err error)
	DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) error
	ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) error
	MountStageVolume(ctx context.Context, volumeID string, stagingTargetPath string, volumeContext map[string]string) error
}
