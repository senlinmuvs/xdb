package main

import (
	"github.com/seefan/gossdb"
)

//cp h:user:%d|z:bk:@hrtBid:st:%d:pks z:user:%0:hrtpks:by:lst
func Copy(xdb *XDB) (count int, err error) {
	c, err := pool.NewClient()
	if err != nil {
		return
	}
	defer c.Close()
	//
	var targetClient *gossdb.Client
	if targetPool != nil {
		targetClient, err = targetPool.NewClient()
		if err != nil {
			return
		}
		defer targetClient.Close()
	}

	find0(c, xdb, func(listKey string, datas map[string]interface{}) (err error) {
		co, e := xdb.WriteToTarget(c, targetClient, listKey, datas)
		if e != nil {
			err = e
			return
		}
		count += co
		return
	})
	return
}
