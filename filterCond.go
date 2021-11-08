package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/seefan/gossdb"
)

type FilterCond struct {
	CondType  int
	Area      string
	FieldType int
	FieldPlah int
	Field     string
	FieldReg  *regexp.Regexp
	ValType   int
	Val       string
	ValKey    string
	ValField  string
}

func parseFilterCond(s string) (conds []*FilterCond, and bool, err error) {
	if s == "" {
		return nil, false, nil
	}
	conds = []*FilterCond{}
	arr := Split(s, Symbol_CondAnd)
	and = false
	if len(arr) > 1 {
		and = true
	} else {
		arr = Split(s, Symbol_CondOr)
	}
	for _, a := range arr {
		c := &FilterCond{}
		c.CondType = parseCondType(a)
		if c.CondType < 0 {
			return nil, and, fmt.Errorf("parse cond type error")
		}
		if c.CondType == Cond_Type_Eq {
			condKV := Split(a, "=")
			if len(condKV) > 0 {
				c.Field = condKV[0]
			}
			if len(condKV) > 1 {
				c.Val = condKV[1]
				c.ValType = parseValType(c.Val)
			}
			condValKV := Split(c.Val, "@")
			if len(condValKV) > 0 {
				c.ValKey = condValKV[0]
			}
			if len(condValKV) > 1 {
				c.ValField = condValKV[1]
			}
		} else if c.CondType == Cond_Type_Area {
			area, i, _ := ExtractArea(a)
			c.Area = area
			c.Field = LeftUnicode(a, i)
		} else if c.CondType == Cond_Type_Reg {
			i := -1
			c.Val, i, _ = Extract(a, "/", "/")
			if i >= 0 {
				c.Field = LeftUnicode(a, i)
				c.FieldType, c.FieldPlah = parseCondField(c.Field)
				r, e := regexp.Compile(c.Val)
				if e != nil {
					return nil, and, e
				}
				c.FieldReg = r
			}
		}
		conds = append(conds, c)
	}
	return
}

//a key中某字段值
//b 条件中某字段值
func (fc *FilterCond) matchCondVal(a string, b string) (ok bool, err error) {
	if fc.CondType == Cond_Type_Eq {
		ok = a == b
	} else if fc.CondType == Cond_Type_Area {
		ok = NumStrInArea(a, fc.Area)
	} else if fc.CondType == Cond_Type_Nil {
		ok = true
	} else {
		err = fmt.Errorf("wtf cond type %s", ObjToJsonStr(fc))
	}
	return
}

//找到满足条件的zset item执行回调
func (fc *FilterCond) FilterZscan(c *gossdb.Client, xdb *XDB, zsetkey string, cb FilterCB) (ok bool, err error) {
	if fc.CondType == Cond_Type_Nil {
		return true, nil
	}
	zscan(c, zsetkey, func(i int, k string, s int64) error {
		scoreStr := strconv.Itoa(int(s))
		ok, err := fc.matchCond(c, xdb, zsetkey, map[string]string{Symbol_ZsetKey: k, Symbol_ZsetScore: scoreStr})
		if ok {
			cb(i, map[string]interface{}{Symbol_ZsetKey: k, Symbol_ZsetScore: s})
		}
		return err
	})
	return
}

//{st(,1)}
//{key=h:user:%0@hrtBid}
func (fc *FilterCond) matchCond(c *gossdb.Client, xdb *XDB, listkey string, fieldValMap map[string]string) (ok bool, err error) {
	key := xdb.GetCurKey()
	var v string
	v, err = fc.GetCondVal(c, key, listkey)
	if err != nil {
		return
	}

	a := fieldValMap[fc.Field]
	if condValUnquote {
		uqv, e2 := strconv.Unquote(a)
		if e2 == nil {
			a = uqv
		}
	}
	return fc.matchCondVal(a, v)
}
func (fc *FilterCond) FilterHash(c *gossdb.Client, xdb *XDB, listkey string) (b bool, e error) {
	if fc.CondType == Cond_Type_Nil {
		return true, nil
	}
	existsField, err := c.Hexists(listkey, fc.Field)
	if err != nil {
		ind := strings.Index(err.Error(), "access ssdb error, code is [error]")
		if ind < 0 {
			return false, err
		}
	}
	if existsField {
		var v gossdb.Value
		v, e = c.Hget(listkey, fc.Field)
		if e != nil {
			return
		}
		b, e = fc.matchCond(c, xdb, listkey, map[string]string{fc.Field: v.String()})
	}
	return
}

func (fc *FilterCond) FilterKV(c *gossdb.Client, xdb *XDB, listkey string) (b bool, e error) {
	if fc.CondType == Cond_Type_Nil {
		return true, nil
	}
	e = fmt.Errorf("not support kv cond yet")
	// var v gossdb.Value
	// v, e = c.Get(listkey)
	// if e != nil {
	// 	return
	// }
	// b, e = fc.matchCond(c, xdb, listkey, map[string]string{fc.Field: v.String()})
	// cb(0, nil)
	return
}

func (fc *FilterCond) Filter(c *gossdb.Client, xdb *XDB, listKey string, cb FilterCB) (ok bool, err error) {
	if slow > 0 {
		time.Sleep(time.Duration(slow) * time.Second)
	}
	key := xdb.GetCurKey()
	if fc.FieldType == Data_Type_Plah {
		if fc.FieldPlah >= 0 && len(key.PlahVals) > fc.FieldPlah && fc.FieldReg != nil {
			plahV := key.PlahVals[fc.FieldPlah]
			ok = fc.FieldReg.MatchString(plahV)
		}
	} else {
		if key.Type == Key_Type_Hash {
			ok, err = fc.FilterHash(c, xdb, listKey)
		} else if key.Type == Key_Type_Zset {
			ok, err = fc.FilterZscan(c, xdb, listKey, cb)
		} else if key.Type == Key_Type_KV {
			ok, err = fc.FilterKV(c, xdb, listKey)
		}
	}
	return
}

func (fc *FilterCond) GetCondVal(c *gossdb.Client, key *Key, listKey string) (v string, e error) {
	if fc.ValType == Data_Type_Val {
		v = fc.Val
	} else if fc.ValType == Data_Type_Plah {
	} else if fc.ValType == Data_Type_Field {
		kk := fillTplParams(key.Key, listKey, fc.ValKey)
		var v_ gossdb.Value
		v_, e = c.Hget(kk, fc.ValField)
		v = v_.String()
	}
	return
}
