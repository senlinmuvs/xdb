package main

import (
	"fmt"
	"strconv"

	"github.com/seefan/gossdb"
)

//find h:user:%d|z:bk:@hrtBid:st:%d:pks %0|key,score
func Find(xdb *XDB) (count int, err error, res [][]string) {
	c, err := pool.NewClient()
	if err != nil {
		return
	}
	defer c.Close()

	err = find0(c, xdb, func(listKey string, datas map[string]interface{}) (err error) {
		key := xdb.GetCurKey()
		if key.Type == Key_Type_KV {
			v, _ := c.Get(listKey)
			ttl, _ := c.Ttl(listKey)
			fmt.Println(listKey, v.String(), ttl)
		} else if key.Type == Key_Type_Hash {
			size, _ := c.Hsize(listKey)
			if len(key.Selectors) > 0 {
				var kvs map[string]gossdb.Value
				keys := key.Fields()
				if len(keys) == 1 && keys[0] == "*" {
					kvs, err = c.HgetAll(listKey)
					keys = MapToKeys(kvs)
				} else {
					kvs, err = c.MultiHget(listKey, keys...)
				}
				if err != nil {
					return
				}
				if unquote {
					UnquoteMap(kvs)
				} else if quote {
					QuoteMap(kvs)
				}
				if count == 0 {
					res = append(res, keys)
					fmt.Println(ArrAsTableStyle(keys))
				}
				arr := MapAsArr(keys, kvs)
				res = append(res, arr)
				fmt.Println(ArrAsTableStyle(arr))
			} else {
				res = append(res, []string{listKey, strconv.Itoa(int(size))})
				fmt.Println(listKey, size)
			}
		} else if key.Type == Key_Type_Zset {
			size, _ := c.Zsize(listKey)
			res = append(res, []string{listKey, strconv.Itoa(int(size))})
			fmt.Println(listKey, size)
		}
		count++
		return
	})
	return
}

func find0(c *gossdb.Client, xdb *XDB, cb FindCB) (err error) {
	keys := xdb.SrcKeys
	keysLen := len(keys)
x:
	for {
		if xdb.CurKeyIndex < 0 || xdb.CurKeyIndex > keysLen {
			break
		}
		if debug {
			fmt.Println("findKeyTpl ->", "keyPre", xdb.GetCurKey().KeyPre, "fromKey", xdb.GetCurKey().FromKey, "keyTpl", xdb.GetCurKey().KeyTpl)
		}
		kiadd1 := false
		err = findKeyTpl(c, xdb, xdb.GetCurKey().KeyPre, xdb.GetCurKey().KeyTpl, xdb.GetCurKey().FromKey,
			func(listKey, fk string) (done bool, err error) {
				if debug {
					fmt.Println("found tpl ->", "listKey", listKey, "fromKey", fk)
				}
				curk := xdb.GetCurKey()
				curk.FromKey = fk
				onSuc := func(i int, datas map[string]interface{}) (err error) {
					if debug {
						fmt.Println("filter ok ->", "listKey", listKey, "datas", ObjToJsonStr(datas))
					}
					curk.FromKey = listKey
					if xdb.CurKeyIndex == keysLen-1 {
						err = cb(listKey, datas)
						if err != nil {
							return
						}
					}
					return
				}
				if curk.Conds != nil && len(curk.Conds) > 0 {
					var ok bool
					for _, co := range curk.Conds {
						ok, err = co.Filter(c, xdb, listKey, onSuc)
						if err != nil {
							return
						}
						if curk.CondAnd {
							if !ok {
								break
							}
						} else {
							if ok {
								break
							}
						}
					}
					if ok {
						err = onSuc(0, nil)
						if err != nil {
							return
						}
					}
				} else {
					err = onSuc(0, nil)
					if err != nil {
						return
					}
				}
				if xdb.CurKeyIndex+1 < keysLen {
					if keys[xdb.CurKeyIndex+1].SetRefVals(c, xdb.GetCurKey(), listKey) {
						kiadd1 = true
						done = true
					}
				}
				return
			})
		if err != nil {
			return
		}
		if xdb.CurKeyIndex == keysLen-1 {
			xdb.GetCurKey().Done = true
			xdb.Stat()
			//回到上一个还没完成的key再接着扫
			i := 0
			for {
				if xdb.CurKeyIndex-i-1 >= 0 {
					if !keys[xdb.CurKeyIndex-i-1].Done {
						xdb.GetCurKey().ClearTempData()
						xdb.CurKeyIndex = xdb.CurKeyIndex - i - 1
						if debug {
							fmt.Printf("back to key %d %s\n", xdb.CurKeyIndex, xdb.GetCurKey().Key)
							fmt.Println("----------------------------------------------------------------------")
						}
						break
					}
				} else {
					break x
				}
				i++
			}
		} else {
			//全部完成
			if !kiadd1 {
				return
			}
		}
		if kiadd1 {
			xdb.CurKeyIndex++
		}
	}
	return
}
