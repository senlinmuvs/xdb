package main

import (
	"fmt"

	"github.com/seefan/gossdb"
)

func Set(xdb *XDB) (count int, err error) {
	c, err := pool.NewClient()
	if err != nil {
		return 0, nil
	}
	c.Close()

	find0(c, xdb, func(listKey string, datas map[string]interface{}) (err error) {
		if err != nil {
			return
		}
		err = set0(c, xdb, listKey, datas)
		count++
		return
	})
	return
}

func set0(c *gossdb.Client, xdb *XDB, listKey string, datas map[string]interface{}) error {
	if xdb.TarKey.Type == Key_Type_Zset {
		arr := Split(xdb.Target, ",")
		if len(arr) > 2 {
			key := xdb.GetCurKey()
			zk := arr[0]
			zk = fillTplParams(key.Key, listKey, zk)
			k := fillTplParams(key.Key, listKey, arr[1])  //TODO fill ref val(hash)
			s_ := fillTplParams(key.Key, listKey, arr[2]) //TODO fill ref val(hash)
			s := ToInt64(s_)
			fmt.Println("zset", zk, k, s)
			if !try {
				c.Zset(zk, k, s)
			}
		} else if len(arr) > 1 {
			zk := listKey
			k := arr[0]
			s := ToInt64(arr[1])
			fmt.Println("zset", zk, k, s)
			if !try {
				c.Zset(zk, k, s)
			}
		}
	} else if xdb.TarKey.Type == Key_Type_Hash {
	} else if xdb.TarKey.Type == Key_Type_KV {
		xdb.TarKey.SetRefVals(c, xdb.SrcKeys[0], listKey)
		s, err := xdb.FillTargetVal()
		if err != nil {
			return err
		}
		arr := Split(s, ",")
		if len(arr) > 1 {
			k := Tr(arr[0])
			v := Tr(arr[1])
			fmt.Println("set", k, v)
			if !try {
				c.Set(k, v)
			}
		}
	}
	return nil
}
