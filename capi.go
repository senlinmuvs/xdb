package main

import "C"
import (
	"strings"
)

//go build -buildmode=c-archive -o libxdb.a
//go build -buildmode=c-shared -o lib.dylib

type ApiRes struct {
	Count int        `json:"count"`
	Datas [][]string `json:"datas"`
}

//export XdbInit
func XdbInit(params_ *C.char) *C.char {
	params := C.GoString(params_)
	arr := strings.Split(params, ",")
	// fmt.Println(C.GoString(params_))
	host = arr[0]
	port = ToInt(arr[1])
	pwd = arr[2]
	dbMinPoolSize = ToInt(arr[3])
	dbMaxPoolSize = ToInt(arr[4])
	dbMaxWaitSize = ToInt(arr[5])
	dbAcq = ToInt(arr[6])
	e := initSSDB()
	if e != nil {
		return C.CString(e.Error())
	}
	return C.CString("")
}

//export XdbClose
func XdbClose() {
	Close()
}

//export Xdb
func Xdb(buf *C.char) *C.char {
	param := C.GoString(buf)
	c, res, e := xdb(param)
	if e != nil {
		return C.CString(AppendJson("err", e.Error()))
	}
	apiRes := &ApiRes{c, res}
	r := ObjToJsonStr(apiRes)
	return C.CString(r)
}
