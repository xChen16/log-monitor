package main

import (
	"bufio"
	"regexp"
	"strconv"

	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type Reader interface {
	Read(rc chan byte)
}
type Writer interface {
	Write(wc chan string)
}
type LogProcess struct {
	rc    chan byte
	wc    chan string
	read  Reader
	write Writer
}
type ReadFromFile struct {
	path string
}
type WriteToInfluxDB struct {
	influxDBDsn string
}

type Message struct {
	TimeLocal                    time.Time
	BytesSent                    int
	Path, Method, Scheme, Status string //各种监控数据
	UpstreamTime, RequestTime    float64
}

// 读取
func (r *ReadFromFile) Read(rc chan []byte) {
	f, err := os.Open(r.path)
	if err != nil {
		panic(fmt.Sprintf("open file error:%s", err.Error()))

	}
	f.Seek(0, 2)
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadBytes('\n')
		if err == io.EOF {
			time.Sleep(500 * time.Millisecond)
			continue

		}
		if err != nil {
			panic(fmt.Sprintf("read error:%s", err.Error()))

		}
		rc <- line[:len(line)-1]
	}

}
func (r *ReadFromFile) Write(rc chan string) {
	fmt.Println(<-wc)
}

// 解析
func (l *LogProcess) Process() {
	//([\d\,]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\&quot;([^&quot;]+)\&quot;\s+(\d{3})\s+(\d+)\s+\&quot;([^&quot;]+)\&quot;\s+\&quot;(.*?)\&quot;\s+\&quot;([\d\.-]+)\&quot;\s+([\d\.-]+)\s+([\d\.-]+)
	r := regexp.MustCompile(`([\d\,]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\&quot;([^&quot;]+)\&quot;\s+(\d{3})\s+(\d+)\s+\&quot;([^&quot;]+)\&quot;\s+\&quot;(.*?)\&quot;\s+\&quot;([\d\.-]+)\&quot;\s+([\d\.-]+)\s+([\d\.-]+)`)
	loc, _ := time.LoadLocation("Asia/Shanghai")
	for v := range l.rc {
		ret := r.FindAllStringSubmatch(string(v))
		if len(ret) != 13 {
			fmt.Printf("fail %s", string(v))
		}
		message := &Message{}
		t, err := time.ParseInLocation("", ret[4], loc)
		if err != nil {

		}
		message.TimeLocal = t
		byteSent, _ := strconv.Atoi(ret[8])
		l.wc <- strings.ToUpper(string(v))
	}

}

func main() {
	r := &ReadFromFile{
		path: "./access.log",
	}
	w := &WriteToInfluxDB{
		influxDBDsn: "username&password..",
	}
	lp := &LogProcess{
		rc:    make(chan []byte),
		wc:    make(chan []byte),
		read:  r,
		write: w,
	}
	go lp.read.Read(lp.rc)
	go lp.Process()
	go lp.write.Write(lp.wc)
}
