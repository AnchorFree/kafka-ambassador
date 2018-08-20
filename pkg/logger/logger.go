package logger

import (
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger interface {
	Fatal(...interface{})
	Fatalf(string, ...interface{})
	Error(...interface{})
	Errorf(string, ...interface{})
	Info(...interface{})
	Infof(string, ...interface{})
	Warn(...interface{})
	Warnf(string, ...interface{})
	Debug(...interface{})
	Debugf(string, ...interface{})
}

func NewLogConfig(c *viper.Viper) zap.Config {
	var cfg zap.Config
	lvl := zap.NewAtomicLevel()
	err := lvl.UnmarshalText([]byte(c.GetString("level")))
	if err != nil {
		panic(err)
	}
	c.Set("level", lvl)
	if err := c.Unmarshal(&cfg); err != nil {
		panic(err)
	}
	return cfg
}

func NewEncoderConfig() zapcore.EncoderConfig {
	return zap.NewProductionEncoderConfig()
}
