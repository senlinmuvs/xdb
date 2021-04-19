package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/smtp"
	"os"
	"runtime"
	"strings"
	"time"
)

var CHARSE = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()-+")
var CHARSE_NUM = []rune("0123456789")

func Rand(n int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(n)
}
func ReadFile(file string) string {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return ""
	}
	return string(content)
}
func ReadLines(file string) []string {
	cont := ReadFile(file)
	lines := strings.Split(cont, "\n")
	return lines
}
func WriteFile(file string, cont string) {
	err := ioutil.WriteFile(file, []byte(cont), 0644)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func DelFile(file string) (err error) {
	err = os.Remove(file)
	return
}

func ExistsFile(file string) bool {
	_, err := os.Stat(file)
	return err == nil
}

func ExistsDir(d string) bool {
	if stat, err := os.Stat(d); err == nil && stat.IsDir() {
		return true
	}
	return false
}

func RandStr(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = CHARSE[Rand(len(CHARSE))]
	}
	return string(b)
}

func RandNumberStr(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = CHARSE_NUM[Rand(len(CHARSE_NUM))]
	}
	return string(b)
}

func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func ToJson(d map[string]interface{}) string {
	data, err := json.Marshal(d)
	if err != nil {
		log.Println("json.marshal failed, err:", err)
		return ""
	}
	return string(data)
}

func AppendJson(args ...interface{}) string {
	m := map[string]interface{}{}
	for i := 0; i < len(args); i += 2 {
		m[args[i].(string)] = args[i+1]
	}
	return ToJson(m)
}

func ObjToJsonStr(o interface{}) string {
	data, err := json.Marshal(o)
	if err != nil {
		log.Println("json.marshal failed, err:", err)
		return ""
	}
	return string(data)
}

func ObjToJsonStyle(o interface{}) (string, error) {
	data, err := json.Marshal(o)
	if err != nil {
		return "", err
	}
	var out bytes.Buffer
	err = json.Indent(&out, data, ",", "  ")
	if err != nil {
		return "", err
	}
	return out.String(), nil
}

func Stack() string {
	var buf [2 << 10]byte
	return string(buf[:runtime.Stack(buf[:], true)])
}

func SendToMail(user, nick, password, host string, port int, to, subject, body string) (err error) {
	header := make(map[string]string)
	header["From"] = nick + "<" + user + ">"
	header["To"] = to
	header["Subject"] = subject
	header["Content-Type"] = "text/html; charset=UTF-8"
	message := ""
	for k, v := range header {
		message += fmt.Sprintf("%s: %s\r\n", k, v)
	}
	message += "\r\n" + body
	auth := smtp.PlainAuth("", user, password, host)
	err = SendMailUsingTLS(fmt.Sprintf("%s:%d", host, port), auth, user, []string{to}, []byte(message))
	return err
}

func Dial(addr string) (*smtp.Client, error) {
	conn, err := tls.Dial("tcp", addr, nil)
	if err != nil {
		log.Println("Dialing Error:", err)
		return nil, err
	}
	host, _, _ := net.SplitHostPort(addr)
	return smtp.NewClient(conn, host)
}

func SendMailUsingTLS(addr string, auth smtp.Auth, from string,
	to []string, msg []byte) (err error) {
	//create smtp client
	c, err := Dial(addr)
	if err != nil {
		log.Println("Create smpt client error:", err)
		return err
	}
	defer c.Close()
	if auth != nil {
		if ok, _ := c.Extension("AUTH"); ok {
			if err = c.Auth(auth); err != nil {
				log.Println("Error during AUTH", err)
				return err
			}
		}
	}
	if err = c.Mail(from); err != nil {
		return err
	}
	for _, addr := range to {
		if err = c.Rcpt(addr); err != nil {
			return err
		}
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	_, err = w.Write(msg)
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}
	return c.Quit()
}

func ReadCSV(file string) (arr [][]string, err error) {
	var csvfile *os.File
	csvfile, err = os.Open(file)
	if err != nil {
		return
	}
	// Parse the file
	r := csv.NewReader(csvfile)
	//r := csv.NewReader(bufio.NewReader(csvfile))

	// Iterate through the records
	for {
		// Read each record from csv
		var record []string
		record, err = r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return
		}
		arr = append(arr, record)
	}
	return
}

func ExistsInStrArr(arr []string, item string) bool {
	for _, it := range arr {
		if it == item {
			return true
		}
	}
	return false
}
func ExistsInArr(arr []interface{}, item interface{}) bool {
	for _, it := range arr {
		if it == item {
			return true
		}
	}
	return false
}

func GetErrStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func CurMills() int64 {
	t := time.Now()
	return int64(time.Nanosecond) * t.UnixNano() / int64(time.Millisecond)
}

func Mkdir(d string) (err error) {
	if _, err = os.Stat(d); os.IsNotExist(err) {
		err = os.Mkdir(d, os.ModePerm)
	}
	return
}

func ScanFile(file string, cb func(int, string) error) (err error) {
	var f *os.File
	f, err = os.Open(file)
	if err != nil {
		return
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	i := 0
	for scanner.Scan() {
		err = cb(i, scanner.Text())
		if err != nil {
			return
		}
		i++
	}
	err = scanner.Err()
	return
}
