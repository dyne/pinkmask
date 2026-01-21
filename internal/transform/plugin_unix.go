//go:build linux || darwin

package transform

import (
	"fmt"
	"os"
	"path/filepath"
	"plugin"
	"runtime"
	"sort"
	"strings"
)

func LoadPlugins(paths []string) error {
	for _, path := range paths {
		if path == "" {
			continue
		}
		resolved, err := resolvePluginPaths(path)
		if err != nil {
			return err
		}
		for _, pluginPath := range resolved {
			p, err := plugin.Open(pluginPath)
			if err != nil {
				return fmt.Errorf("open plugin %s: %w", pluginPath, err)
			}
			sym, err := p.Lookup("Transformers")
			if err != nil {
				return fmt.Errorf("plugin %s: missing Transformers symbol", pluginPath)
			}
			if err := registerPluginSymbol(pluginPath, sym); err != nil {
				return err
			}
		}
	}
	return nil
}

func resolvePluginPaths(path string) ([]string, error) {
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return resolvePluginsInDir(path)
	}
	if fileExists(path) {
		return []string{path}, nil
	}
	base := path
	ext := filepath.Ext(path)
	if ext == ".so" {
		base = strings.TrimSuffix(path, ext)
	}
	candidates := []string{
		fmt.Sprintf("%s.%s.%s.so", base, runtime.GOOS, runtime.GOARCH),
		fmt.Sprintf("%s.%s.so", base, runtime.GOARCH),
	}
	for _, cand := range candidates {
		if fileExists(cand) {
			return []string{cand}, nil
		}
	}
	return nil, fmt.Errorf("plugin not found: %s (tried %s)", path, strings.Join(candidates, ", "))
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func resolvePluginsInDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read plugin dir %s: %w", dir, err)
	}
	var matches []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".so") {
			continue
		}
		if isCompatiblePlugin(name) {
			matches = append(matches, filepath.Join(dir, name))
		}
	}
	sort.Strings(matches)
	if len(matches) == 0 {
		return nil, fmt.Errorf("no compatible plugins found in %s", dir)
	}
	return matches, nil
}

func isCompatiblePlugin(name string) bool {
	archSuffix := "." + runtime.GOARCH + ".so"
	osArchSuffix := "." + runtime.GOOS + "." + runtime.GOARCH + ".so"
	return strings.HasSuffix(name, osArchSuffix) || strings.HasSuffix(name, archSuffix) || strings.HasSuffix(name, ".so")
}

func registerPluginSymbol(path string, sym any) error {
	switch v := sym.(type) {
	case map[string]func(any, map[string]any) (any, error):
		for name, fn := range v {
			registerPlugin(name, fn)
		}
		return nil
	case *map[string]func(any, map[string]any) (any, error):
		for name, fn := range *v {
			registerPlugin(name, fn)
		}
		return nil
	default:
		return fmt.Errorf("plugin %s: Transformers has incompatible type", path)
	}
}
