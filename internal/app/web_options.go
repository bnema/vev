package app

import (
	"fmt"
	"strings"

	"github.com/bnema/vev/internal/adapters/config"
	"github.com/bnema/vev/internal/adapters/webterm"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/platform"
)

type webOptions struct {
	listen string
	origin string
}

func parseWebArgs(args []string) (command, error) {
	cmd := command{kind: kindWebDaemon}
	if args[0] == "--web-serve" {
		cmd.kind = kindWebServe
	}
	for i := 1; i < len(args); i++ {
		key, value, inline := strings.Cut(args[i], "=")
		if key != "--web-listen" && key != "--web-origin" {
			return command{}, usagef("unexpected web argument %q", key)
		}
		if !inline {
			i++
			if i == len(args) {
				return command{}, usagef("%s requires a value", key)
			}
			value = args[i]
		}
		if value == "" || strings.HasPrefix(value, "--") {
			return command{}, usagef("%s requires a value", key)
		}
		if key == "--web-listen" {
			cmd.web.listen = value
		} else {
			cmd.web.origin = value
		}
	}
	return cmd, nil
}

func loadWebSettings(options webOptions) (webterm.Settings, []domain.Warning, error) {
	cfg, warnings, err := config.Load(platform.ConfigPath())
	if err != nil {
		return webterm.Settings{}, nil, fmt.Errorf("vev: loading web configuration: %w", err)
	}
	if options.listen != "" {
		cfg.WebListen = options.listen
	}
	if options.origin != "" {
		cfg.WebOrigin = options.origin
	}
	settings, err := webterm.ParseSettings(cfg.WebListen, cfg.WebOrigin)
	if err != nil {
		return webterm.Settings{}, warnings, err
	}
	return settings, warnings, nil
}
