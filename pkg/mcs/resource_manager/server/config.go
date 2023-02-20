// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"go.uber.org/zap"
)

const (
	defaultName              = "Resource Manager"
	defaultBackendEndpoints  = "http://127.0.0.1:2379"
	defaultListenAddr        = "http://127.0.0.1:2382"
	defaultEnableGRPCGateway = true

	defaultLogFormat           = "text"
	defaultDisableErrorVerbose = true
)

// Config is the configuration for the resource manager.
type Config struct {
	BackendEndpoints  string `toml:"backend-endpoints" json:"backend-endpoints"`
	ListenAddr        string `toml:"listen-addr" json:"listen-addr"`
	Name              string `toml:"name" json:"name"`
	DataDir           string `toml:"data-dir" json:"data-dir"`
	EnableGRPCGateway bool   `json:"enable-grpc-gateway"`

	Metric metricutil.MetricConfig `toml:"metric" json:"metric"`

	// WarningMsgs contains all warnings during parsing.
	WarningMsgs []string

	// Log related config.
	Log log.Config `toml:"log" json:"log"`

	Logger   *zap.Logger
	LogProps *log.ZapProperties
	Security configutil.SecurityConfig `toml:"security" json:"security"`
}

// NewConfig creates a new config.
func NewConfig() *Config {
	return &Config{}
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(flagSet *pflag.FlagSet) error {
	// Load config file if specified.
	var (
		meta *toml.MetaData
		err  error
	)
	if configFile, _ := flagSet.GetString("config"); configFile != "" {
		meta, err = configutil.ConfigFromFile(c, configFile)
		if err != nil {
			return err
		}
	}

	// Ignore the error check here
	configutil.AdjustCommandlineString(flagSet, &c.Log.Level, "log-level")
	configutil.AdjustCommandlineString(flagSet, &c.Log.File.Filename, "log-file")
	configutil.AdjustCommandlineString(flagSet, &c.Metric.PushAddress, "metrics-addr")
	configutil.AdjustCommandlineString(flagSet, &c.Security.CAPath, "cacert")
	configutil.AdjustCommandlineString(flagSet, &c.Security.CertPath, "cert")
	configutil.AdjustCommandlineString(flagSet, &c.Security.KeyPath, "key")
	configutil.AdjustCommandlineString(flagSet, &c.BackendEndpoints, "backend-endpoints")
	configutil.AdjustCommandlineString(flagSet, &c.ListenAddr, "listen-addr")

	return c.Adjust(meta, false)
}

// Adjust is used to adjust the PD configurations.
func (c *Config) Adjust(meta *toml.MetaData, reloading bool) error {
	configMetaData := configutil.NewConfigMetadata(meta)
	if err := configMetaData.CheckUndecoded(); err != nil {
		c.WarningMsgs = append(c.WarningMsgs, err.Error())
	}

	if c.Name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return err
		}
		configutil.AdjustString(&c.Name, fmt.Sprintf("%s-%s", defaultName, hostname))
	}
	configutil.AdjustString(&c.DataDir, fmt.Sprintf("default.%s", c.Name))
	adjustPath(&c.DataDir)

	if err := c.Validate(); err != nil {
		return err
	}

	configutil.AdjustString(&c.BackendEndpoints, defaultBackendEndpoints)
	configutil.AdjustString(&c.ListenAddr, defaultListenAddr)

	if !configMetaData.IsDefined("enable-grpc-gateway") {
		c.EnableGRPCGateway = defaultEnableGRPCGateway
	}

	c.adjustLog(configMetaData.Child("log"))
	c.Security.Encryption.Adjust()

	if len(c.Log.Format) == 0 {
		c.Log.Format = defaultLogFormat
	}

	return nil
}

func adjustPath(p *string) {
	absPath, err := filepath.Abs(*p)
	if err == nil {
		*p = absPath
	}
}

func (c *Config) adjustLog(meta *configutil.ConfigMetaData) {
	if !meta.IsDefined("disable-error-verbose") {
		c.Log.DisableErrorVerbose = defaultDisableErrorVerbose
	}
}

// GetTLSConfig returns the TLS config.
func (c *Config) GetTLSConfig() *grpcutil.TLSConfig {
	return &c.Security.TLSConfig
}

// Validate is used to validate if some configurations are right.
func (c *Config) Validate() error {
	dataDir, err := filepath.Abs(c.DataDir)
	if err != nil {
		return errors.WithStack(err)
	}
	logFile, err := filepath.Abs(c.Log.File.Filename)
	if err != nil {
		return errors.WithStack(err)
	}
	rel, err := filepath.Rel(dataDir, filepath.Dir(logFile))
	if err != nil {
		return errors.WithStack(err)
	}
	if !strings.HasPrefix(rel, "..") {
		return errors.New("log directory shouldn't be the subdirectory of data directory")
	}

	return nil
}
