package server

import (
	"sync"

	"github.com/anchorfree/kafka-ambassador/pkg/kafka"
	"github.com/spf13/viper"
)

// We need this package to prevent cyclic dependencies

type I interface {
	Start()
	Stop()
}

type T struct {
	Producer kafka.T
	Config   *viper.Viper
	Wg       *sync.WaitGroup
}

func (s *T) Stop() {

}
