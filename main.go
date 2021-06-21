package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

type Reader interface {
	Read(rc chan []byte)
}

type Writer interface {
	Write(wc chan *Message)
}
type ReadFromTail struct {
	inode uint64
	fd    *os.File
	path  string
}

func NewReader(path string) (Reader, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &ReadFromTail{
		inode: stat.Ino,
		fd:    f,
		path:  path,
	}, nil
}

// 讀取模塊
func (r *ReadFromTail) Read(rc chan []byte) {
	defer close(rc)
	var stat syscall.Stat_t

	r.fd.Seek(0, 2) // seek 到末尾
	bf := bufio.NewReader(r.fd)

	for {
		line, err := bf.ReadBytes('\n')
		if err == io.EOF {
			if err := syscall.Stat(r.path, &stat); err != nil {
				// 文件切割, 但是新文件還沒生成
				time.Sleep(1 * time.Second)
			} else {
				nowInode := stat.Ino
				if nowInode == r.inode {
					// 無新的數據產生
					time.Sleep(1 * time.Second)
				} else {
					// 文件切割, 重新開啟檔案
					r.fd.Close()
					fd, err := os.Open(r.path)
					if err != nil {
						panic(fmt.Sprintf("Open file err: %s", err.Error()))
					}
					r.fd = fd
					bf = bufio.NewReader(fd)
					r.inode = nowInode
				}
			}
			continue
		} else if err != nil {
			log.Printf("readFromTail ReadBytes err: %s", err.Error())
			TypeMonitorChan <- TypeReadErr
			continue
		}

		rc <- line[:len(line)-1]
	}
}

type LogProcess struct {
	rc     chan []byte   // read channel
	wc     chan *Message // write channel
	reader Reader
	writer Writer
}

func NewLogProcess(reader Reader, writer Writer) *LogProcess {
	return &LogProcess{
		rc:     make(chan []byte, 200),
		wc:     make(chan *Message, 200),
		reader: reader,
		writer: writer,
	}
}

type Message struct {
	TimeLocal                    time.Time
	BytesSent                    int
	Path, Method, Scheme, Status string
	UpstreamTime, RequestTime    float64
}

// 系統狀態監控
type SystemInfo struct {
	HandleLine   int       `json:"handleLine"`   // 總處理log行數
	Tps          float64   `json:"tps"`          // 系統吞吐量
	ReadChanLen  int       `json:"readChanLen"`  // read channel 長度
	WriteChanLen int       `json:"writeChanLen"` // write channel 長度
	RunTime      string    `json:"runTime"`      // 總運行時間
	ErrInfo      ErrorInfo `json:"errInfo"`
}

type ErrorInfo struct {
	ReadErr    int `json:"readErr"`
	ProcessErr int `json:"processErr"`
	WriteErr   int `json:"writeErr"`
}

const (
	TypeHandleLine = iota
	TypeReadErr
	TypeProcessErr
	TypeWriteErr
)

var (
	path, influxDsn, listenPort, token string
	processNum, writeNum               int
	TypeMonitorChan                    = make(chan int, 200)
)

type Monitor struct {
	listenPort string
	startTime  time.Time
	tpsSli     []int
	systemInfo SystemInfo
}

func (m *Monitor) start(lp *LogProcess) {
	go func() {
		for n := range TypeMonitorChan {
			switch n {
			case TypeHandleLine:
				m.systemInfo.HandleLine += 1
			case TypeReadErr:
				m.systemInfo.ErrInfo.ReadErr += 1
			case TypeProcessErr:
				m.systemInfo.ErrInfo.ProcessErr += 1
			case TypeWriteErr:
				m.systemInfo.ErrInfo.WriteErr += 1
			}
		}
	}()

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for {
			<-ticker.C
			m.tpsSli = append(m.tpsSli, m.systemInfo.HandleLine)
			if len(m.tpsSli) > 2 {
				m.tpsSli = m.tpsSli[1:]
			}
		}
	}()

	http.HandleFunc("/monitor", func(writer http.ResponseWriter, request *http.Request) {
		io.WriteString(writer, m.systemStatus(lp))
	})

	http.ListenAndServe(":"+m.listenPort, nil)
}

func (m *Monitor) systemStatus(lp *LogProcess) string {
	d := time.Now().Sub(m.startTime)
	m.systemInfo.RunTime = d.String()
	m.systemInfo.ReadChanLen = len(lp.rc)
	m.systemInfo.WriteChanLen = len(lp.wc)
	if len(m.tpsSli) >= 2 {
		// return math.Trunc(float64(m.tpsSli[1]-m.tpsSli[0])/5*1e3+0.5) * 1e-3
		m.systemInfo.Tps = float64(m.tpsSli[1]-m.tpsSli[0]) / 5
	}
	res, _ := json.MarshalIndent(m.systemInfo, "", "\t")
	return string(res)
}

type InfluxConf struct {
	Addr, Token, Organization, Bucket, Measurement, Precision string
}
type WriteToInfluxDB struct {
	// batch      uint16
	// retry      uint8 // 寫入失敗時使用, influx2似乎沒有回傳錯誤
	influxConf *InfluxConf
}

// influxDsn: http://ip:port@Organization@bucket@measurement@precision
func NewWriter(influxDsn string, token string) (Writer, error) {
	influxDsnSli := strings.Split(influxDsn, "@")
	if len(influxDsnSli) < 5 {
		return nil, errors.New("param influxDns err")
	}
	return &WriteToInfluxDB{
		// batch: 50,
		// retry: 3,
		influxConf: &InfluxConf{
			Addr:         influxDsnSli[0],
			Organization: influxDsnSli[1],
			Bucket:       influxDsnSli[2],
			Measurement:  influxDsnSli[3],
			Precision:    influxDsnSli[4],
			Token:        token,
		},
	}, nil
}

// 寫入模塊
func (w *WriteToInfluxDB) Write(wc chan *Message) {
	client := influxdb2.NewClient(w.influxConf.Addr, w.influxConf.Token)

	// always close client at the end
	defer client.Close()
	client.Options()
	writeAPI := client.WriteAPI(w.influxConf.Organization, w.influxConf.Bucket)

	// write channel 中讀取監控數據
	for v := range wc {
		// 構造數據並寫入influxdb
		// Tags: Path, Method, Scheme, Status
		tags := map[string]string{
			"Path":   v.Path,
			"Method": v.Method,
			"Scheme": v.Scheme,
			"Status": v.Status,
		}

		// Fields: UpstreamTime, RequestTime, BytesSent
		fields := map[string]interface{}{
			"UpstreamTime": v.UpstreamTime,
			"RequestTime":  v.RequestTime,
			"BytesSent":    v.BytesSent,
		}

		// Write the batch
		p := influxdb2.NewPoint(
			w.influxConf.Measurement,
			tags,
			fields,
			v.TimeLocal)

		// write point asynchronously
		writeAPI.WritePoint(p)

		log.Println("write success!")
	}

}

// 解析模塊
func (l *LogProcess) Process() {
	/**
	172.0.0.12 - - [04/Mar/2018:13:49:52 +0000] http "GET /foo?query=t HTTP/1.0" 200 2133 "-" "KeepAliveClient" "-" 1.005 1.854
	*/

	// 正規提取所需的監控數據(path, status, method 等)
	r := regexp.MustCompile(`([\d\.]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\"([^"]+)\"\s+(\d{3})\s+(\d+)\s+\"([^"]+)\"\s+\"(.*?)\"\s+\"([\d\.-]+)\"\s+([\d\.-]+)\s+([\d\.-]+)`)

	loc, _ := time.LoadLocation("Asia/Taipei")
	// 從 read channel 中讀取每行日誌數據
	for v := range l.rc {
		// 第 0 項是數據本身
		TypeMonitorChan <- TypeHandleLine
		ret := r.FindStringSubmatch(string(v))
		if len(ret) != 14 {
			TypeMonitorChan <- TypeHandleLine
			log.Println("FindStringSubmatch fail:", string(v))
			continue
		}

		// [04/Mar/2018:13:49:52 +0000]
		t, err := time.ParseInLocation("02/Jan/2006:15:04:05 +0000", ret[4], loc)
		if err != nil {
			TypeMonitorChan <- TypeProcessErr
			log.Println("ParseInLocation fail:", err.Error(), ret[4])
			continue
		}
		message := &Message{}
		message.TimeLocal = t

		// 2133
		byteSent, _ := strconv.Atoi(ret[8])
		message.BytesSent = byteSent

		// GET /foo?query=t HTTP/1.0
		reqSli := strings.Split(ret[6], " ")
		if len(reqSli) != 3 {
			TypeMonitorChan <- TypeProcessErr
			log.Println("strings.Split fail", ret[6])
			continue
		}
		// GET
		message.Method = reqSli[0]

		u, err := url.Parse(reqSli[1])
		if err != nil {
			TypeMonitorChan <- TypeProcessErr
			log.Println("url parse fail:", err)
			continue
		}
		message.Path = u.Path

		// http
		message.Scheme = ret[5]
		// 200
		message.Status = ret[7]

		// 1.005
		upstreamTime, _ := strconv.ParseFloat(ret[12], 64)
		// 1.854
		requestTime, _ := strconv.ParseFloat(ret[13], 64)
		message.UpstreamTime = upstreamTime
		message.RequestTime = requestTime

		// 寫入 write channel
		l.wc <- message
	}
}

const defaultToken = "7Vft2nXp1IkgLMu1VaLVEqylPKeJMqO1KLLfwRa1wxOg92DwMqHEjKkTqbqj03k49Inw-cD2rmBQOok-Dij2BQ=="

func init() {
	flag.StringVar(&path, "path", "./log/access.log", "log file path")
	// influxDsn: http://ip:port@Organization@bucket@measurement@precision
	flag.StringVar(&influxDsn, "influxDsn", "http://127.0.0.1:8086@kimiORG@kk@myMeasure@s", "influxDB dsn")
	flag.StringVar(&listenPort, "listenPort", "9193", "monitor port")
	flag.StringVar(&token, "token", defaultToken, "token")
	flag.IntVar(&processNum, "processNum", 1, "process goroutine num")
	flag.IntVar(&writeNum, "writeNum", 1, "write goroutine num")
	flag.Parse()
}

func main() {
	fmt.Println("===== Optimization =====")
	reader, err := NewReader(path)
	if err != nil {
		panic(err)
	}

	writer, err := NewWriter(influxDsn, token)
	if err != nil {
		panic(err)
	}

	lp := NewLogProcess(reader, writer)

	go lp.reader.Read(lp.rc)
	for i := 0; i < processNum; i++ {
		go lp.Process()
	}
	for i := 0; i < writeNum; i++ {
		go lp.writer.Write(lp.wc)
	}

	// 監控模組
	m := &Monitor{
		listenPort: listenPort,
		startTime:  time.Now(),
	}
	m.start(lp)

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1)
	for s := range c {
		switch s {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			log.Println("capture exit signal:", s)
			os.Exit(1)
		case syscall.SIGUSR1: // 用戶自訂訊號
			log.Println(m.systemStatus(lp))
		default:
			log.Println("capture other signal:", s)
		}
	}
}
