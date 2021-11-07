package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/seefan/gossdb"
)

const (
	version = "1.4.1"
)

var (
	help           bool
	v              bool
	file           string
	xdbcmd         string
	try            bool
	pwd            string
	host           string
	port           int
	targetHost     string
	targetPort     int
	progressCount  int
	batch          int
	pool           *gossdb.Connectors
	targetPool     *gossdb.Connectors
	debug          bool
	slow           int
	unquote        bool
	condValUnquote bool
	quote          bool
	dbMinPoolSize  int
	dbMaxPoolSize  int
	dbMaxWaitSize  int
	dbAcq          int
	nct            bool
)

func init() {
	flag.BoolVar(&help, "help", false, "")
	flag.BoolVar(&v, "v", false, "show version")
	flag.StringVar(&file, "f", "", "xdb file")
	flag.StringVar(&xdbcmd, "x", "", "xdb cmd")
	flag.BoolVar(&try, "try", false, "try correct, just print log")
	flag.StringVar(&host, "h", "localhost", "ssdb host")
	flag.IntVar(&port, "p", 8888, "ssdb port")
	flag.StringVar(&targetHost, "h2", "localhost", "ssdb target host")
	flag.IntVar(&targetPort, "p2", 0, "ssdb target port")
	flag.IntVar(&progressCount, "pc", 0, "遍历多少个key打印一次进度提示")
	flag.IntVar(&batch, "ba", 100, "batch size")
	flag.BoolVar(&debug, "X", false, "debug")
	flag.BoolVar(&nct, "nct", false, "do not print count info")
	flag.IntVar(&slow, "slow", 0, "slow")
	flag.BoolVar(&unquote, "uq", false, "if unquote for value")
	flag.BoolVar(&quote, "qu", false, "if quote for value")
	flag.BoolVar(&condValUnquote, "cuq", true, "if quote for cond value")
	flag.IntVar(&dbMinPoolSize, "dbmips", 5, "db MinPoolSize")
	flag.IntVar(&dbMaxPoolSize, "dbmxps", 20, "db MaxPoolSize")
	flag.IntVar(&dbMaxWaitSize, "dbmxws", 1000, "db MaxWaitSize")
	flag.Usage = usage
}

func main() {
	flag.Parse()
	exe()
}

func exe() {
	if v {
		fmt.Printf("xdb %s\n", version)
	}

	if help {
		flag.Usage()
	}

	if len(xdbcmd) > 0 {
		initSSDB()
		xdb(xdbcmd)
	} else if len(file) > 0 {
		initSSDB()
		xdb("")
	}
}

//XDB
///////////////////////////////////////////////////////////
type XDB struct {
	Cmd         string
	Src         string
	SrcKeys     []*Key
	Target      string
	TarKey      *Key
	CurKeyIndex int
	Pure        bool
}

func (x *XDB) GetCurKey() *Key {
	return x.SrcKeys[x.CurKeyIndex]
}

//获取前导key
func (x *XDB) GetLeadKey() *Key {
	if x.CurKeyIndex-1 >= len(x.SrcKeys) {
		return nil
	}
	return x.SrcKeys[x.CurKeyIndex-1]
}

func (x *XDB) IsLashKey() bool {
	return x.CurKeyIndex == len(x.SrcKeys)-1
}

func (x *XDB) getKeyPlahVal(i int) string {
	n := 0
	for _, k := range x.SrcKeys {
		for _, pl := range k.PlahVals {
			if n == i {
				return pl
			}
			n++
		}
	}
	return ""
}

func (x *XDB) FillTargetVal() (s string, err error) {
	arr := Split(x.Target, Symbol_CommaSep)
	for _, a := range arr {
		targetParts := Split(a, Symbol_KeySep)
		refFieldIndex := 0
		as := ""
		for _, e := range targetParts {
			if string(e[0]) == Symbol_Plah {
				pi := ToIntDef(RightUnicode(e, 1), -1)
				if pi == -1 {
					err = fmt.Errorf("FillTargetVal ToIntDef error %s", e)
					return
				}
				v := x.getKeyPlahVal(pi)
				as += v + Symbol_KeySep
			} else if string(e[0]) == Symbol_Field {
				r := x.TarKey.Refs[refFieldIndex]
				as += r.Val.String() + Symbol_KeySep
				refFieldIndex++
			} else {
				as += e + Symbol_KeySep
			}
		}
		if as != "" {
			as = as[0 : len(as)-1]
		}
		s += as + Symbol_CommaSep
	}
	if s != "" {
		s = s[0 : len(s)-1]
	}
	return
}

func (x *XDB) WriteToTarget(c *gossdb.Client, xdb *XDB, listKey string, datas map[string]interface{}) (err error) {
	var tar string
	tar, err = xdb.FillTargetVal()
	if x.TarKey.Type == Key_Type_Zset {
		if len(datas) > 1 {
			k := datas[Symbol_ZsetKey]
			s := datas[Symbol_ZsetScore]
			fmt.Println("zset", tar, k, s)
			if !try {
				err = c.Zset(tar, k.(string), s.(int64))
			}
		} else {
			err = fmt.Errorf("WriteToTarget found zset val error params %s", ObjToJsonStr(datas))
		}
	} else if x.TarKey.Type == Key_Type_Hash {
	} else if x.TarKey.Type == Key_Type_KV {
	}
	return
}

func (x *XDB) Stat() {
	if debug {
		fmt.Println("============== xdb stat =============")
		fmt.Println("curKeyIndex", x.CurKeyIndex)
		for i, k := range x.SrcKeys {
			fmt.Println(i, k.Key, "done", k.Done, "fromKey", k.FromKey)
		}
		fmt.Println("=====================================")
	}
}

///////////////////////////////////////////////////////////
func parseXDB(cmd string) (xdbs []*XDB, err error) {
	lines := []string{}
	if cmd == "" {
		lines = ReadLines(file)
		if len(lines) == 0 {
			return xdbs, nil
		}
		if debug {
			fmt.Println("lines", len(lines))
		}
	} else {
		lines = append(lines, cmd)
	}
	for _, line := range lines {
		line = strings.Trim(line, " \n\r")
		if line == "" || strings.Index(line, "#") == 0 {
			continue
		}
		xdb := &XDB{}
		if strings.Index(line, "/") == 0 {
			xdb.Pure = true
			xdb.Cmd = RightUnicode(line, 1)
		} else {
			arr := strings.Split(line, " ")
			if len(arr) > 0 {
				xdb.Cmd = strings.Trim(arr[0], " ")
			} else {
				fmt.Println("WARN no cmd", line)
				continue
			}
			if len(arr) > 1 {
				xdb.Src = strings.Trim(arr[1], " ")
				var keys []*Key
				keys, err = parseKeys(xdb.Src)
				if err != nil {
					return
				}
				xdb.SrcKeys = keys
			}
			if len(arr) > 2 {
				xdb.Target = strings.Trim(arr[2], " ")
				var keys []*Key
				keys, err = parseKeys(xdb.Target)
				if err != nil {
					return
				}
				if len(keys) > 0 {
					xdb.TarKey = keys[0]
				}
			}
		}
		xdbs = append(xdbs, xdb)
	}
	if debug {
		fmt.Println("cmd lines", len(xdbs))
	}
	return xdbs, nil
}

func xdb(cmd string) (count int, res [][]string, e error) {
	if pool == nil {
		e = fmt.Errorf("db connection is null")
		return
	}
	var xdbs []*XDB
	xdbs, e = parseXDB(cmd)
	if e != nil {
		fmt.Println("parseXDB err:", e)
		return
	}
	for _, xdb := range xdbs {
		if debug {
			xdbstr, _ := ObjToJsonStyle(xdb)
			fmt.Println(xdb.Cmd, xdbstr)
		}
		c := 0
		cmd := strings.ToLower(xdb.Cmd)
		if xdb.Pure {
			c, res, e = DoPure(cmd)
		} else {
			if cmd == "cp" {
				c, e = Copy(xdb)
			} else if cmd == "find" {
				c, res, e = Find(xdb)
			} else if cmd == "del" {
				c, e = Del(xdb)
			} else if cmd == "set" {
				c, e = Set(xdb)
			} else if cmd == "hset" {
				c, e = Hset(xdb)
			} else if cmd == "import" {
				c, e = Import(xdb)
			} else {
				fmt.Println("WARN no cmd:", xdb.Cmd, xdb.Src, xdb.Target)
			}
		}
		if e != nil && e.Error() == "stop" {
			e = nil
		}
		if e != nil {
			fmt.Println(xdb.Cmd+" err:", e)
			return
		}
		count += c
	}
	if !nct {
		fmt.Printf("xdb count: %d\n", count)
	}
	return
}

func usage() {
	fmt.Printf("xdb version: %s\nOptions:\n", version)
	flag.PrintDefaults()
}
