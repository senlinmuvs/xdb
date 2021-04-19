package main

import (
	"fmt"

	"github.com/seefan/gossdb"
)

func Del(xdb *XDB) (count int, err error) {
	c, err := pool.NewClient()
	if err != nil {
		return
	}
	defer c.Close()

	err = find0(c, xdb, func(listKey string, datas map[string]interface{}) (err error) {
		err = del0(c, xdb, listKey, datas)
		if err != nil {
			return
		}
		count++
		return
	})
	return
}

func del0(c *gossdb.Client, xdb *XDB, listKey string, datas map[string]interface{}) (err error) {
	key := xdb.GetCurKey()
	if key.Type == Key_Type_Hash {
		if key.Selectors == nil || len(key.Selectors) == 0 {
			fmt.Println("hclear", listKey)
			if !try {
				c.Hclear(listKey)
			}
		} else {
			//TODO del fields
		}
	} else if key.Type == Key_Type_KV {
		fmt.Println("del", listKey)
		if !try {
			c.Del(listKey)
		}
	} else if key.Type == Key_Type_Zset {
		if key.Conds == nil || len(key.Conds) == 0 {
			fmt.Println("zclear", listKey)
			if !try {
				c.Zclear(listKey)
			}
		} else {
			fmt.Println(Symbol_ZsetKey, ObjToJsonStr(datas))
			zk_ := datas[Symbol_ZsetKey]
			if zk_ != nil {
				zk := fmt.Sprintf("%v", zk_)
				fmt.Println("zdel", listKey, zk)
				if !try {
					c.Zdel(listKey, zk)
				}
			}
		}
	}
	return
}
