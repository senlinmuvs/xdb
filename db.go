package main

import (
	"github.com/seefan/gossdb"
	"github.com/seefan/gossdb/conf"
)

func initSSDB() (err error) {
	if pool == nil {
		pool, err = initSSDB0(host, port)
	}
	if targetHost != "" && targetPort > 0 {
		targetPool, err = initSSDB0(targetHost, targetPort)
	}
	return err
}

func initSSDB0(h string, p int) (*gossdb.Connectors, error) {
	pool, err := gossdb.NewPool(&conf.Config{
		Host:             h,
		Port:             p,
		MinPoolSize:      dbMinPoolSize,
		MaxPoolSize:      dbMaxPoolSize,
		MaxWaitSize:      dbMaxWaitSize,
		AcquireIncrement: 5,
	})
	return pool, err
}
