package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seefan/gossdb"
)

const (
	Symbol_KeySep      = ":"  //key分割符
	Symbol_CommaSep    = ","  //tar分割符
	Symbol_Plah        = "%"  //占位引导符
	Symbol_Plh1        = "%d" //key占位符
	Symbol_Plh2        = "%s" //key占位符
	Symbol_Field       = "@"  //字段标识
	Symbol_ZsetKey     = "key"
	Symbol_ZsetScore   = "score"
	Symbol_Cond0       = "{" //条件
	Symbol_Cond1       = "}" //条件
	Symbol_Pipe        = "|" //多key连接查询管道符
	Symbol_KeyPre_Hash = "h:"
	Symbol_KeyPre_Zset = "z:"
	Symbol_SmallBra0   = "("
	Symbol_SmallBra1   = ")"
	Symbol_CondAnd     = "&"
	Symbol_CondOr      = ";"

	Key_Type_KV   = 0
	Key_Type_Hash = 1
	Key_Type_Zset = 2

	Cond_Type_Nil  = -1 //无条件
	Cond_Type_Eq   = 0  //相等
	Cond_Type_Area = 1  //区间
	Cond_Type_Reg  = 2  //正则

	Data_Type_Val   = 0 //普通值
	Data_Type_Field = 1 //引用前导key字段
	Data_Type_Plah  = 2 //引用前导key占位符值
	Data_Type_Func  = 3 //函数
)

type (
	ZscanCB  func(i int, k string, s int64) error
	ListCB   func(listKey, fromkey string) (bool, error)
	FilterCB func(i int, datas map[string]interface{}) error
	FindCB   func(listKey string, datas map[string]interface{}) error
)

func parseValType(s string) int {
	if strings.Contains(s, Symbol_Field) && strings.Contains(s, Symbol_KeySep) {
		return Data_Type_Field
	}
	return Data_Type_Val
}
func parseCondField(s string) (int, int) {
	plah := ToIntDef(string(s[1]), -1)
	if string(s[0]) == Symbol_Plah && plah >= 0 {
		return Data_Type_Plah, plah
	}
	return Data_Type_Field, -1
}
func parseCondType(s string) int {
	i := strings.Index(s, "/")
	if i >= 0 {
		j := strings.Index(RightUnicode(s, 2), "/")
		if j >= 0 {
			return Cond_Type_Reg
		}
	}

	i = strings.Index(s, "(")
	if i < 0 {
		i = strings.Index(s, "[")
	}
	j := strings.Index(s, ")")
	if j < 0 {
		j = strings.Index(s, "]")
	}
	if i >= 0 && j >= 0 && i < j {
		return Cond_Type_Area
	} else if strings.Contains(s, "=") {
		return Cond_Type_Eq
	} else {
		//do sth.
	}
	return -1
}

func zscan(c *gossdb.Client, zkey string, cb ZscanCB) error {
	fromKey := ""
	fromScore := int64(0)
	for {
		keys, scores, err := c.Zscan(zkey, fromKey, fromScore, "", int64(batch))
		if err != nil {
			return err
		}
		if len(keys) == 0 || len(scores) == 0 {
			break
		}
		for i, k := range keys {
			score := scores[i]
			err := cb(i, k, score)
			if err != nil {
				return err
			}
			fromKey = k
			fromScore = score
		}
	}
	return nil
}

func ssdbList(c *gossdb.Client, key *Key, fromKey, endKey string, batch int) (keys []string, err error) {
	if slow > 0 {
		time.Sleep(time.Duration(slow) * time.Second)
	}
	if key.Type == Key_Type_Hash {
		keys, err = c.Hlist(fromKey, "", int64(batch))
		// if debug {
		// 	fmt.Printf("[hlist %s %d] res %d\n", fromKey, batch, len(keys))
		// }
	} else if key.Type == Key_Type_Zset {
		keys, err = c.Zlist(fromKey, "", int64(batch))
		if debug {
			le := len(keys)
			first := ""
			if le > 0 {
				first = keys[0]
			}
			fmt.Printf("[zlist %s %d] res %d %s\n", fromKey, batch, le, first)
		}
	} else {
		keys, err = c.Keys(fromKey, "", int64(batch))
		// err = fmt.Errorf("key type [%d] no list", key.Type)
	}
	return
}

func existsKey(c *gossdb.Client, key *Key, k string) bool {
	if key.Type == Key_Type_KV {
		b, e := c.Exists(k)
		if e != nil {
			fmt.Println("existsKey err", e)
			return false
		}
		return b
	} else if key.Type == Key_Type_Hash {
		v, e := c.Hsize(k)
		if e != nil {
			fmt.Println("existsKey err", e)
			return false
		}
		return v > 0
	} else if key.Type == Key_Type_Zset {
		v, e := c.Zsize(k)
		if e != nil {
			fmt.Println("existsKey err", e)
			return false
		}
		return v > 0
	}
	return false
}

//zlist/hlist遍历所有某前缀的key找到匹配模板执行回调
func findKeyTpl(c *gossdb.Client, xdb *XDB, keyPre, keyTpl, fromKey string, cb ListCB) (err error) {
	key := xdb.GetCurKey()
	if keyPre == keyTpl { //固定key,非模板
		// fromKey = LeftUnicode(keyPre, len(keyPre)-1) //-1是为了当keyPre=keyTpl时list能找到这个key
		if existsKey(c, key, keyPre) {
			cb(keyPre, fromKey)
		}
		return
	}
	if fromKey == "" {
		fromKey = keyPre
	}
	//
	bat := 0
	if xdb.IsLashKey() {
		bat = batch
	} else {
		bat = 1
	}
	//
	total := 0
a:
	for {
		var keys []string
		keys, err = ssdbList(c, key, fromKey, "", bat)
		if err != nil {
			return
		}
		if len(keys) == 0 {
			break a
		}
		for _, listKey := range keys {
			if progressCount > 0 && total != 0 && total%progressCount == 0 {
				fmt.Println("INFO total", total, keyPre)
			}
			total++
			//
			i := strings.Index(listKey, keyPre)
			// fmt.Println(">>>>>>>>>>>>", listKey, keyPre, i)
			if i != 0 {
				// if debug {
				// 	fmt.Printf("stop %s %s %s\n", keyTpl, listKey, keyPre)
				// }
				break a
			}
			if matchKey(keyTpl, listKey) {
				// if debug {
				// 	fmt.Printf("matchKey %s %s\n", keyTpl, listKey)
				// }
				done := false
				key.SetPlahVal(listKey)
				done, err = cb(listKey, fromKey)
				if done || err != nil {
					return
				}
			}
			fromKey = listKey
		}
	}
	return
}

//h:user:%d -> h:user:100	匹配
//z:bk:100000003:st:%d:pks -> z:bk:100000003:st:1:pks	匹配
//z:bk:100000003:st:%d:pks -> z:bk:100000004:st:1:pks	不匹配
func matchKey(keytpl, k string) bool {
	if keytpl == k {
		return true
	}
	arr1 := strings.Split(keytpl, ":")
	arr2 := strings.Split(k, ":")
	if len(arr1) != len(arr2) {
		return false
	}
	for i, c := range arr1 {
		if c == Symbol_Plh1 {
			_, err := strconv.ParseInt(arr2[i], 10, 64)
			if err != nil {
				return false
			}
		} else if c == Symbol_Plh2 {
			continue
		} else {
			if c != arr2[i] {
				return false
			}
		}
	}
	return true
}

/*
 * 找字符串按某字符分割后的数组中的第i(从0开始)个占位符在数组中的index
 */
func findPos(s string, x int) int {
	arr := strings.Split(s, Symbol_KeySep)
	n := 0
	for i, e := range arr {
		if e == Symbol_Plh1 || e == Symbol_Plh2 {
			n++
			if x+1 == n {
				return i
			}
		}
	}
	return -1
}

//z:x:%d:%s -> z:x:%0:%1
func replacePlah(k string) (kk string) {
	j := 0
	for i, c := range k {
		if i > 0 && k[i-1] == '%' && (c == 'd' || c == 's') {
			kk += strconv.Itoa(j)
		} else {
			kk += string(c)
		}
	}
	return
}

/*
 * 填充key模板
 * 用k1对应的值替换k2的占位符
 * k1_与k1是相同的模板 k1_ 带占位符 ,k1带值，k2带占位符
 * z:user:%d:wid:%d:st:%d:pks z:user:100000037:wid:100054920:st:1:pks z:user:%0:wid:%1:pks
 */
func fillTplParams(k1_, k1, k2 string) string {
	if k1_ == k1 {
		return k2
	}
	s := k2
	k1arr := strings.Split(k1, ":")
	k2arr := strings.Split(k2, ":")

	x := 0
	var arr []int
	for _, k := range k2arr {
		if len(k) > 1 {
			if k[0] == '%' && k[1] >= 48 && k[1] <= 57 { //k[1] 0～9
				arr = append(arr, findPos(k1_, x))
				x++
			}
		}
	}
	for _, k := range k2arr {
		if len(k) > 1 {
			if k[0] == '%' {
				pos := ToInt(string(k[1]))
				if pos < len(arr) {
					index := arr[pos]
					if index < len(k1arr) {
						v := k1arr[index]
						s = strings.Replace(s, k, v, -1)
					}
				}
			}
		}
	}
	return s
}

func FillPlahVal(keyTpl, listKey string) []string {
	plahArr := []string{}
	arr := Split(listKey, Symbol_KeySep)
	parts := Split(keyTpl, Symbol_KeySep)
	for i, a := range parts {
		if a == Symbol_Plh1 || a == Symbol_Plh2 || string(a[0]) == Symbol_Field {
			plahArr = append(plahArr, arr[i])
		}
	}
	return plahArr
}

func FillPlahByArr(keyParts []string, arr []string) []string {
	for i, a := range keyParts {
		if string(a[0]) == Symbol_Plah {
			n := ToIntDef(RightUnicode(a, 1), -1)
			if n >= 0 {
				if len(arr) > n {
					keyParts[i] = arr[n]
				}
			}
		}
	}
	return keyParts
}

func PartsToKey(arr []string) string {
	s := ""
	for i, a := range arr {
		if i == len(arr)-1 {
			s += a
		} else {
			s += a + Symbol_KeySep
		}
	}
	return s
}

func ArrAsTableStyle(arr []string) string {
	s := ""
	for i, v := range arr {
		if i == len(arr)-1 {
			s += v
		} else {
			s += v + "\t"
		}
	}
	return s
}
func MapAsArr(orderFields []string, m map[string]gossdb.Value) (vs []string) {
	for _, f := range orderFields {
		vs = append(vs, m[f].String())
	}
	return
}

func MapAsTableStyle2(orderFields []string, m map[string]interface{}) string {
	vs := []string{}
	for _, f := range orderFields {
		vs = append(vs, fmt.Sprintf("%v", m[f]))
	}
	s := ""
	for i, v := range vs {
		if i == len(vs)-1 {
			s += orderFields[i] + " " + v
		} else {
			s += orderFields[i] + " " + v + " "
		}
	}
	return s
}

func MapToKeys(m map[string]gossdb.Value) []string {
	ks := []string{}
	for k, _ := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func UnquoteMap(m map[string]gossdb.Value) {
	for k, v := range m {
		s, e := strconv.Unquote(v.String())
		if e != nil {
			continue
		}
		m[k] = gossdb.Value(s)
	}
}

func QuoteMap(m map[string]gossdb.Value) {
	for k, v := range m {
		m[k] = gossdb.Value(strconv.Quote(v.String()))
	}
}

func ConvMapValue(m map[string]gossdb.Value) (mm map[string]interface{}) {
	mm = map[string]interface{}{}
	for k, v := range m {
		mm[k] = v.String() //TODO all type string
	}
	return
}
