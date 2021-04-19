package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/seefan/gossdb"
)

type Ref struct {
	Type  int          //引用类型，引用字段或占位符的值
	Field string       //引用前一个key的哪个字段 - @uid
	Plah  int          //引用前一个key的第几个占位符 - %0
	Val   gossdb.Value //引用值
	//up h:pk:%d(tags=h:tag:@tids(tag))
	SelFields []string //引用后取得的值的哪些字段(tag)
}

//查询：选取的字段(uid,st)
//更新：选取的字段+更新值(uid=1,tag=h:tag:%d@name)
//up h:pk:%d(tags=h:tag:@tids(tag))
type Selector struct {
	Field string //tags
	Ref   *Ref   //h:tag:@tids(tag)
}
type Key struct {
	Type      int
	Key       string
	Parts     []string //:分割后的数组
	Refs      []*Ref
	KeyPre    string
	KeyTpl    string
	FromKey   string
	Conds     []*FilterCond
	CondAnd   bool
	Done      bool     //标记当前key是否已经扫描完
	PlahVals  []string //各%d/@xx上的值
	Selectors []*Selector
}

//copy h:user:%d|z:bk:@hrtBid:st:%d:pks z:user:%0:hrtpks:by:lst
func parseKeys(s string) (keys []*Key, err error) {
	keys = []*Key{}
	arr := Split(s, Symbol_Pipe)
	for _, a := range arr {
		cond_, i0, _ := Extract(a, Symbol_Cond0, Symbol_Cond1)
		var conds []*FilterCond
		a_ := a
		and := true
		if i0 >= 0 {
			conds, and, err = parseFilterCond(cond_)
			if err != nil {
				return
			}
			a_ = LeftUnicode(a, i0)
		} else {
			j := strings.Index(a, Symbol_SmallBra0)
			if j >= 0 {
				a_ = LeftUnicode(a, j)
			}
		}
		arr := Split(a_, Symbol_KeySep)
		key := &Key{
			Type:      parseKeyType(a_),
			Key:       a_,
			KeyTpl:    a_,
			Parts:     arr,
			Refs:      parseKeyRefs(arr),
			KeyPre:    parseKeyPre(a_),
			Conds:     conds,
			CondAnd:   and,
			Selectors: parseSelectors(a),
		}
		keys = append(keys, key)
	}
	return
}

//Key
////////////////////////////////////////////////////////////

//h:user:%d
//z:bk:@hrtBid:st:%d:pks
func (k *Key) SetPlahVal(listKey string) {
	k.PlahVals = FillPlahVal(k.KeyTpl, listKey)
}

func (k *Key) FillPlahValByArr(arr []string) string {
	ps := make([]string, len(k.Parts))
	copy(ps, k.Parts)
	return PartsToKey(FillPlahByArr(ps, arr))
}

//设置引用值
//preKey 前导key
//listKey 前导key,findTpl的listKey
func (k *Key) SetRefVals(c *gossdb.Client, preKey *Key, listKey string) bool {
	for _, ref := range k.Refs {
		ref.setRefVal(c, preKey, listKey)
		if ref.Val == "" {
			if debug {
				fmt.Println("setRefVals found empty field val:", listKey, ref.Field)
			}
			return false
		}
		//z:bk:@hrtBid:st:%d:pks -> z:bk:123:st:%d:pks
		newKey := strings.Replace(k.Key, Symbol_Field+ref.Field, ref.Val.String(), -1)
		k.KeyTpl = newKey
		//z:bk: -> z:bk:123:st:
		k.FromKey = parseKeyPre(newKey)
		k.KeyPre = parseKeyPre(k.FromKey)
	}
	return true
}

func (k *Key) ToStr() string {
	s := "key:" + k.Key + ", "
	for _, r := range k.Refs {
		s += "field:" + r.Field + ", plah:" + strconv.Itoa(r.Plah) + ", val:" + r.Val.String()
	}
	return s
}

//当key回退时清除上一轮扫描产生的临时数据
func (k *Key) ClearTempData() {
	k.FromKey = ""
	k.KeyTpl = k.Key
	k.KeyPre = parseKeyPre(k.Key)
	k.Done = false
}

func (k *Key) Fields() []string {
	fields := []string{}
	for _, sel := range k.Selectors {
		fields = append(fields, sel.Field)
	}
	return fields
}

////////////////////////////////////////////////////////////

//Ref
///////////////////////////////////////////////////////////
func (r *Ref) setRefVal(c *gossdb.Client, preKey *Key, listKey string) (e error) {
	var v gossdb.Value
	if r.Type == Data_Type_Field {
		if preKey.Type == Key_Type_Hash {
			v, e = c.Hget(listKey, r.Field)
			// fmt.Println("hget", listKey, r.Field, "res:", v)
		} else if preKey.Type == Key_Type_KV {
			v, e = c.Get(listKey)
		} else if preKey.Type == Key_Type_Zset {
			sc := int64(0)
			sc, e = c.Zget(listKey, r.Field)
			v = gossdb.Value(strconv.Itoa(int(sc)))
		}
	} else if r.Type == Data_Type_Plah {

	} else if r.Type == Data_Type_Val {

	}
	r.Val = v
	return
}

///////////////////////////////////////////////////////////

func parseRefType(s string) int {
	if strings.Index(s, Symbol_Field) == 0 {
		return Data_Type_Field
	}
	s02 := LeftUnicode(s, 2)
	if string(s02[0]) == Symbol_Plah && IsNumber(string(s02[1])) {
		return Data_Type_Plah
	}
	if len(s) > 2 && s[len(s)-1:] == Symbol_SmallBra1 && strings.Index(s, Symbol_SmallBra0) >= 0 {
		return Data_Type_Func
	}
	return -1
}
func parseKeyType(s string) int {
	if strings.Index(s, Symbol_KeyPre_Hash) == 0 {
		return Key_Type_Hash
	}
	if strings.Index(s, Symbol_KeyPre_Zset) == 0 {
		return Key_Type_Zset
	}
	return Key_Type_KV
}
func parseKeyRefs(arr []string) (refs []*Ref) {
	plahIndex := 0
	for _, a := range arr {
		refType := parseRefType(a)
		if refType >= 0 {
			ref := &Ref{Type: refType}
			if refType == Data_Type_Field {
				ref.Field = RightUnicode(a, len(Symbol_Field))
			} else if refType == Data_Type_Plah {
				ref.Plah = plahIndex
				plahIndex++
			}
			refs = append(refs, ref)
		}
	}
	return
}

func parseKeyPre(s string) (keyPre string) {
	i := strings.Index(s, Symbol_Plh1)
	if i < 0 {
		i = strings.Index(s, Symbol_Plh2)
	}
	j := strings.Index(s, Symbol_Field)
	if i >= 0 && j >= 0 {
		i = Min(i, j)
	} else {
		i = Max(i, j)
	}
	keyPre = LeftUnicode(s, i)
	if keyPre == "" {
		keyPre = s
	}
	return
}

func parseSelectors(s string) []*Selector {
	fields := []*Selector{}
	i := LastIndexUnicode(s, Symbol_SmallBra0)
	if i < 0 {
		return fields
	}
	j := LastIndexUnicode(s, Symbol_SmallBra1)
	if j < 0 {
		return fields
	}
	if i >= j || j != LenUnicode(s)-1 {
		return fields
	}
	str := SubUnicode(s, i+1, j)
	if str == "" {
		return fields
	}
	arr := Split(str, Symbol_CommaSep)
	for _, a := range arr {
		fields = append(fields, &Selector{
			Field: a,
			Ref:   &Ref{}, //TODO 待解析选择器值
		})
	}
	return fields
}
