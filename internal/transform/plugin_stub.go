//go:build !linux && !darwin

package transform

import "fmt"

func LoadPlugins(paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	return fmt.Errorf("plugins are only supported on linux and darwin")
}
