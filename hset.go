package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/seefan/gossdb"
)

type HupTargetVal struct {
	Ty    int
	Key   string
	Field string
	Func  string
	Param string
}
type HupTarget struct {
	Field  string
	Cond   string
	ValStr string
	Val    *HupTargetVal
}

func parseHupTarget(s string) []*HupTarget {
	tars := []*HupTarget{}
	tarsArr := Split(s, "|")
	for _, tar := range tarsArr {
		t := &HupTarget{}
		arr := strings.Split(tar, "=")
		if len(arr) > 1 {
			field_ := strings.Trim(arr[0], " ")
			area, i0, _ := ExtractArea(field_)
			field := LeftUnicode(field_, i0)
			if i0 < 0 {
				field = field_
			}
			t.Cond = area
			t.Field = field
			t.ValStr = strings.Trim(arr[1], " ")
			j0 := strings.Index(t.ValStr, "@")
			t.Val = &HupTargetVal{
				Ty:    parseRefType(t.ValStr),
				Key:   LeftUnicode(t.ValStr, j0),
				Field: RightUnicode(t.ValStr, j0+1),
			}
			if t.Val.Ty == Data_Type_Func {
				t.Val.Func = LeftUnicode(t.ValStr, strings.Index(t.ValStr, Symbol_SmallBra0))
				t.Val.Param, _, _ = Extract(t.ValStr, Symbol_SmallBra0, Symbol_SmallBra1)
			}
		}
		tars = append(tars, t)
	}
	return tars
}

func Hset(xdb *XDB) (count int, err error) {
	var c *gossdb.Client
	c, err = pool.NewClient()
	if err != nil {
		return
	}
	c.Close()
	tars := parseHupTarget(xdb.Target)

	if debug {
		obs, _ := ObjToJsonStyle(tars)
		fmt.Println("tar", obs)
	}

	find0(c, xdb, func(listKey string, datas map[string]interface{}) (err error) {
		ct := 0
		ct, err = hup0(c, listKey, xdb, datas, tars)
		if err != nil {
			return
		}
		count += ct
		return
	})
	return
}

func hup0(c *gossdb.Client, key string, xdb *XDB, datas map[string]interface{}, tars []*HupTarget) (count int, err error) {
	tar := tars[0] //TODO 要改成一次能改多个字段
	if tar.Val.Ty == Data_Type_Func {
		if tar.Val.Func == "UnQuote" {
			oldV, e := c.Hget(key, tar.Field)
			if e != nil {
				return 0, e
			}
			if oldV.String() != "" {
				v, e := strconv.Unquote(oldV.String())
				if e != nil {
					return 0, nil
				}
				if v != oldV.String() {
					fmt.Println("hset", key, tar.Field, v)
					if !try {
						c.Hset(key, tar.Field, v)
					}
					count++
				}
			}
		} else if tar.Val.Func == "DelField" {
			fmt.Println("hdel", key, tar.Field)
			if !try {
				e := c.Hdel(key, tar.Field)
				if e != nil {
					return 0, e
				}
			}
			count++
		}
	} else if tar.Val.Ty == Data_Type_Field {
		vmap, _ := c.MultiHget(key, tar.Field, tar.Val.Field)
		if tar.Cond == "" || InArea(vmap[tar.Field].Int(), tar.Cond) {
			fmt.Println("hset", key, tar.Field, vmap[tar.Val.Field].Int64())
			if !try {
				c.Hset(key, tar.Field, vmap[tar.Val.Field].Int64())
			}
			count++
		}
	}
	return
}
