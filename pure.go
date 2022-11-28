package main

import (
	"fmt"
	"strings"
)

func parseParams(cmd string, unquote bool) (arr []string) {
	param := ""
	firstQuote := 0
	quote1Num := 0
	quote2Num := 0
	for i, c := range cmd {
		s := string(c)
		param += s
		if s == "\"" {
			if i > 0 {
				if string(cmd[i-1]) != "\\" {
					quote1Num = quote1Num + 1
				}
			} else {
				quote1Num = quote1Num + 1
			}
			if firstQuote == 0 {
				firstQuote = 1
			}
		}
		if s == "'" {
			if i > 0 {
				if string(cmd[i-1]) != "\\" {
					quote2Num = quote2Num + 1
				}
			} else {
				quote2Num = quote2Num + 1
			}
			if firstQuote == 0 {
				firstQuote = 2
			}
		}
		if quote1Num == 2 {
			if firstQuote == 1 {
				if unquote {
					param = Unquote(strings.Trim(param, " "))
				} else {
					param = strings.Trim(param, " ")
				}
				arr = append(arr, param)
				param = ""
				firstQuote = 0
			}
			quote1Num = 0
		}
		if quote2Num == 2 {
			if firstQuote == 2 {
				if unquote {
					param = Unquote(strings.Trim(param, " "))
				} else {
					param = strings.Trim(param, " ")
				}
				arr = append(arr, param)
				param = ""
				firstQuote = 0
			}
			quote2Num = 0
		}
		if quote1Num == 0 && quote2Num == 0 && s == " " {
			arr = append(arr, strings.Trim(param, " "))
			param = ""
		}
	}
	if (firstQuote == 1 && (quote1Num > 0 && quote1Num < 2)) ||
		(firstQuote == 2 && quote2Num > 0 && quote2Num < 2) {
		fmt.Println("quote error")
		arr = strings.Split(cmd, " ")
		return
	}
	if len(param) > 0 {
		arr = append(arr, strings.Trim(param, " "))
	}
	return
}
func DoPure(cmd string) (ct int, res [][]string, e error) {
	c, err := pool.NewClient()
	if err != nil {
		if !silence {
			fmt.Println(err)
		}
		return 0, res, err
	}
	c.Close()

	n := 1
	if inputLines != nil {
		n = len(inputLines)
	}
	for i := 0; i < n; i++ {
		curCmd := cmd
		if inputLines != nil {
			curCmd = FillKeyTplByInput(inputLines[i], cmd)
		}
		cmdty := ""
		arr := parseParams(curCmd, true)
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
			if !silence {
				fmt.Println(err)
			}
			return 0, res, err
		}
		l := len(resps)
		if l == 1 {
			if !silence {
				fmt.Println(resps[0])
			}
			res = append(res, []string{"-"})
			res = append(res, []string{resps[0]})
			return
		}

		if cmdty == "hgetall" ||
			cmdty == "zscan" ||
			cmdty == "zrscan" ||
			cmdty == "scan" {
			ct, res = printKVResp(resps)
		} else {
			ct, res = print(resps)
		}
	}
	return
}

func printKVResp(resps []string) (ct int, res [][]string) {
	res = append(res, []string{"key", "value"})
	l := len(resps)
	for i := 1; i < l-1; i += 2 {
		k := resps[i]
		v := resps[i+1]
		if !silence {
			fmt.Println(k, v)
		}
		res = append(res, []string{k, v})
		ct++
	}
	return
}

func print(resps []string) (ct int, res [][]string) {
	res = append(res, []string{"-"})
	l := len(resps)
	for i := 1; i < l; i += 1 {
		if !silence {
			fmt.Println(resps[i])
		}
		res = append(res, []string{resps[i]})
		ct++
	}
	return
}
