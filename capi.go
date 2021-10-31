package main

import "C"

//go build -buildmode=c-archive -o lib.a
//go build -buildmode=c-shared -o lib.dylib

type ApiRes struct {
	Count int        `json:"count"`
	Datas [][]string `json:"datas"`
}

//export XdbInit
func XdbInit(host_ *C.char, port_ *C.char, pwd_ *C.char,
	minPoolSize_ *C.char, maxPoolSize_ *C.char,
	maxWaitSize_ *C.char, acq_ *C.char) {
	targetHost = C.GoString(host_)
	targetPort = ToInt(C.GoString(port_))
	pwd = C.GoString(pwd_)
	dbMinPoolSize = ToInt(C.GoString(minPoolSize_))
	dbMaxPoolSize = ToInt(C.GoString(maxPoolSize_))
	dbMaxWaitSize = ToInt(C.GoString(maxWaitSize_))
	dbAcq = ToInt(C.GoString(acq_))
	// fmt.Println(host, port, pwd, minPoolSize, maxWaitSize, maxPoolSize, acq)
	initSSDB()
}

//export Xdb
func Xdb(buf *C.char) *C.char {
	param := C.GoString(buf)
	c, res := xdb(param)
	apiRes := &ApiRes{c, res}
	r := ObjToJsonStr(apiRes)
	return C.CString(r)
}
