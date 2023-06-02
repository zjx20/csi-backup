package util

import (
	"regexp"
	"strings"
)

type RcloneOptions struct {
	Options string `json:"rclone_options"`
}

func ParseOptions(options string) []string {
	mountOptions := make([]string, 0)
	if options != "" {
		re, _ := regexp.Compile(`([^\s"]+|"([^"\\]+|\\")*")+`)
		re2, _ := regexp.Compile(`"([^"\\]+|\\")*"`)
		re3, _ := regexp.Compile(`\\(.)`)
		for _, opt := range re.FindAll([]byte(options), -1) {
			// Unquote options
			opt = re2.ReplaceAllFunc(opt, func(q []byte) []byte {
				return re3.ReplaceAll(q[1:len(q)-1], []byte("$1"))
			})
			mountOptions = append(mountOptions, string(opt))
		}
	}
	return mountOptions
}

func MergeRcloneOptions(base []string, extra []string) []string {
	m := map[string]string{}
	var add []string
	for _, opt := range extra {
		pos := strings.Index(opt, "=")
		if pos != -1 {
			m[opt[:pos]] = opt
		} else {
			add = append(add, opt)
		}
	}
	args := append([]string(nil), base...)
	for idx, arg := range args {
		pos := strings.Index(arg, "=")
		if pos != -1 {
			key := arg[:pos]
			if overwrite, ok := m[key]; ok {
				args[idx] = overwrite
				delete(m, key)
			}
		}
	}
	for _, opt := range m {
		args = append(args, opt)
	}
	args = append(args, add...)
	return args
}
