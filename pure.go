package main

import (
	"fmt"
	"strings"
)

func DoPure(cmd string) (ct int, e error) {
	c, err := pool.NewClient()
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	c.Close()

	cmdty := ""
	arr := strings.Split(cmd, " ")
	if len(arr) < 1 {
		return
	}
	cmdty = arr[0]

	var arr2 []interface{}
	for _, a := range arr {
		if a == "-" {
			a = ""
		}
		arr2 = append(arr2, a)
	}
	resps, err := c.Do(arr2...)
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	l := len(resps)
	if l == 1 {
		fmt.Println(resps[0])
		return
	}

	if cmdty == "hget" ||
		cmdty == "hgetall" ||
		cmdty == "zscan" ||
		cmdty == "zrscan" {
		ct = printKVResp(resps)
	} else {
		ct = print(resps)
	}
	return
}

func printKVResp(resps []string) (ct int) {
	l := len(resps)
	for i := 1; i < l-1; i += 2 {
		k := resps[i]
		v := resps[i+1]
		fmt.Println(k, v)
		ct++
	}
	return
}

func print(resps []string) (ct int) {
	l := len(resps)
	for i := 1; i < l; i += 1 {
		fmt.Println(resps[i])
		ct++
	}
	return
}
