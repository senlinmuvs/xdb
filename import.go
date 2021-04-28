package main

import (
	"fmt"
	"strconv"
	"strings"
)

type (
	ScanTextCB func(line string)
)

func Import(xdb *XDB) (count int, err error) {
	c, err := pool.NewClient()
	if err != nil {
		return
	}
	defer c.Close()

	if IsFile(xdb.Src) {
		path := RightUnicode(xdb.Src, 7)
		var keys []string
		err = ScanFile(path, func(i int, line string) (e error) {
			arr := strings.Split(line, "\t")
			if xdb.TarKey.Type == Key_Type_Hash {
				if i == 0 {
					keys = arr
				} else {
					kvs := map[string]interface{}{}
					for colIndex, k := range keys {
						if len(arr) > colIndex {
							if unquote {
								str, err := strconv.Unquote(arr[colIndex])
								if err != nil {
									str = arr[colIndex]
								}
								kvs[k] = str
								arr[colIndex] = str
							} else if quote {
								str := strconv.Quote(arr[colIndex])
								kvs[k] = str
								arr[colIndex] = str
							} else {
								kvs[k] = arr[colIndex]
							}
						} else {
							kvs[k] = ""
						}
					}
					if len(kvs) > 0 {
						hashKey := xdb.TarKey.FillPlahValByArr(arr)
						fmt.Println("multi_hset", hashKey, MapAsTableStyle2(keys, kvs))
						if !try {
							e = c.MultiHset(hashKey, kvs)
							if e != nil {
								return
							}
						}
						count++
					}
				}
			}
			return
		})
	}
	return
}
