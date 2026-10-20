//go:build !linux && !darwin

package health

import "errors"

func freeBytes(path string) (uint64, error) { return 0, errors.New("LINUX_ISOLATION_REQUIRED") }
