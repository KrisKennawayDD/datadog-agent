// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package config

import (
	"strings"
	"testing"
)

var (
	isConfigMocked = false
)

// MockConfig should only be used in tests
type MockConfig struct {
	Config
}

// Set is used for setting configuration in tests
func (c *MockConfig) Set(key string, value interface{}) {
	c.Config.Set(key, value)
}

// Mock is creating and returning a mock config
func Mock() *MockConfig {
	// Configure Datadog global configuration
	Datadog = NewConfig("datadog", "DD", strings.NewReplacer(".", "_"))
	// Configuration defaults
	InitConfig(Datadog)
	return &MockConfig{Datadog}
}

// MockAndClean is creating and returning a mock config. This also register a cleanup function to reset the
// configuration after the test.
func MockAndClean(t *testing.T) *MockConfig {
	if isConfigMocked {
		// The configuration is already mocked.
		return &MockConfig{Datadog}
	}

	isConfigMocked = true

	oldDatadogConfig := Datadog // keep a ref on the original configuration
	t.Cleanup(func() {
		Datadog = oldDatadogConfig
		isConfigMocked = false
	})

	// Configure Datadog global configuration
	Datadog = NewConfig("datadog", "DD", strings.NewReplacer(".", "_"))
	// Configuration defaults
	InitConfig(Datadog)
	return &MockConfig{Datadog}
}
