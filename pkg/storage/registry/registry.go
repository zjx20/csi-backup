package registry

import (
	"fmt"
	"strings"
	"sync"

	"github.com/yandex-cloud/k8s-csi-s3/pkg/storage/types"
)

var (
	mu       sync.Mutex
	creators = map[string]Creator{}
)

const (
	storageType = "storage"
)

type Creator func(config map[string]string) (types.Storage, error)

func Register(name string, fn Creator) {
	mu.Lock()
	defer mu.Unlock()
	creators[strings.ToLower(name)] = fn
}

func GetStorage(config map[string]string) (types.Storage, error) {
	mu.Lock()
	defer mu.Unlock()
	st := config[storageType]
	fn := creators[strings.ToLower(st)]
	if fn == nil {
		return nil, fmt.Errorf("creator for storage \"%s\" not found", st)
	}
	return fn(config)
}
