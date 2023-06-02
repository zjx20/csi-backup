package storage

import (
	"github.com/yandex-cloud/k8s-csi-s3/pkg/storage/registry"
	"github.com/yandex-cloud/k8s-csi-s3/pkg/storage/types"

	_ "github.com/yandex-cloud/k8s-csi-s3/pkg/storage/s3"
)

func GetStorage(config map[string]string) (types.Storage, error) {
	return registry.GetStorage(config)
}
