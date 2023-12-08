/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"os"
)

// 程序配置文件
type Config struct {
	*flag.FlagSet `json:"-"`
	AppConfig     AppConfig    `toml:"app" json:"app"`
	OracleConfig  OracleConfig `toml:"oracle" json:"oracle"`
	MySQLConfig   MySQLConfig  `toml:"mysql" json:"mysql"`
	MetaConfig    MetaConfig   `toml:"meta" json:"meta"`
	LogConfig     LogConfig    `toml:"log" json:"log"`
	ConfigFile    string       `json:"config-file"`
}

type AppConfig struct {
	BatchSize   int    `toml:"batch-size" json:"batch-size"`
	InitThread  int    `toml:"init-thread" json:"init-thread"`
	TableThread int    `toml:"table-thread" json:"table-thread"`
	SQLThread   int    `toml:"sql-thread" json:"sql-thread"`
	ChunkSize   int    `toml:"chunk-size" json:"chunk-size"`
	SQLHint     string `toml:"sql-hint" json:"sql-hint"`
	CallTimeout int64  `toml:"call-timeout" json:"call-timeout"`
	SkipInit    bool   `toml:"skip-init" json:"skip-init"`
	SkipSplit   bool   `toml:"skip-split" json:"skip-split"`
}

type OracleConfig struct {
	Username      string   `toml:"username" json:"username"`
	Password      string   `toml:"password" json:"password"`
	Host          string   `toml:"host" json:"host"`
	Port          int      `toml:"port" json:"port"`
	ServiceName   string   `toml:"service-name" json:"service-name"`
	PDBName       string   `toml:"pdb-name" json:"pdb-name"`
	Charset       string   `toml:"charset" json:"charset"`
	ConnectParams string   `toml:"connect-params" json:"connect-params"`
	SessionParams []string `toml:"session-params" json:"session-params"`
	Schema        string   `toml:"schema" json:"schema"`
}

type MySQLConfig struct {
	Username      string `toml:"username" json:"username"`
	Password      string `toml:"password" json:"password"`
	Host          string `toml:"host" json:"host"`
	Port          int    `toml:"port" json:"port"`
	Charset       string `toml:"charset" json:"charset"`
	ConnectParams string `toml:"connect-params" json:"connect-params"`
	Schema        string `toml:"schema" json:"schema"`
}

type MetaConfig struct {
	Username      string `toml:"username" json:"username"`
	Password      string `toml:"password" json:"password"`
	Host          string `toml:"host" json:"host"`
	Port          int    `toml:"port" json:"port"`
	SlowThreshold int    `toml:"slow-threshold" json:"slow-threshold"`
	MetaSchema    string `toml:"meta-schema" json:"meta-schema"`
}

type LogConfig struct {
	LogLevel   string `toml:"log-level" json:"log-level"`
	LogFile    string `toml:"log-file" json:"log-file"`
	MaxSize    int    `toml:"max-size" json:"max-size"`
	MaxDays    int    `toml:"max-days" json:"max-days"`
	MaxBackups int    `toml:"max-backups" json:"max-backups"`
}

func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("transferdb", flag.ContinueOnError)
	fs := cfg.FlagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of transferdb:")
		fs.
			PrintDefaults()
	}
	fs.StringVar(&cfg.ConfigFile, "config", "./config.toml", "path to the configuration file")
	return cfg
}

func (c *Config) Parse(args []string) error {
	err := c.FlagSet.Parse(args)
	switch err {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		os.Exit(2)
	}

	if c.ConfigFile != "" {
		if err = c.configFromFile(c.ConfigFile); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("no config file")
	}

	return nil
}

// 加载配置文件并解析
func (c *Config) configFromFile(file string) error {
	if _, err := toml.DecodeFile(file, c); err != nil {
		return fmt.Errorf("failed decode toml config file %s: %v", file, err)
	}
	return nil
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}
