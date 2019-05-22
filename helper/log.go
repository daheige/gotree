// Copyright gotree Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package helper

import (
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"time"
)

const (
	_LOG_ERROR = iota //错误日志
	_LOG_WARN  = iota //警告日志
	_LOG_INFO  = iota //普通日志
)

const (
	__LOG_ERROR_DIR_NAME = "error"  //错误日志目录名
	__LOG_WARN_DIR_NAME  = "warn"   //警告日志目录名
	__LOG_INFO_DIR_NAME  = "notice" //普通日志目录名
)

type log struct {
	errorDir string
	warnDir  string
	infoDir  string

	errFile  *os.File
	warnFile *os.File
	infoFile *os.File

	errFileName  string
	warnFileName string
	infoFileName string

	debug    bool
	mutexErr *sync.Mutex
	infoMsg  chan string
	warnMsg  chan string
	closeMsg chan bool

	stream  chan *logItem
	bseqMap bseqInterface
}

type logItem struct {
	category int
	data     []interface{}
}

type bseqInterface interface {
	Get(key string) interface{}
}

//Create 创建本结构
func (self *log) Init(dir string, bdict bseqInterface) {
	self.bseqMap = bdict
	logPath := dir

	if Testing() {
		return
	}
	mode := Config().String("sys::Mode")
	if mode != "prod" {
		self.debug = true
	}
	if !FileExists(logPath) {
		err := os.Mkdir(logPath, os.ModePerm)
		if err != nil {
			panic(err)
		}
	}
	self.errorDir = logPath + "/" + __LOG_ERROR_DIR_NAME
	if !FileExists(self.errorDir) {
		os.Mkdir(self.errorDir, os.ModePerm)
	}

	self.warnDir = logPath + "/" + __LOG_WARN_DIR_NAME
	if !FileExists(self.warnDir) {
		os.Mkdir(self.warnDir, os.ModePerm)
	}

	self.infoDir = logPath + "/" + __LOG_INFO_DIR_NAME
	if !FileExists(self.infoDir) {
		os.Mkdir(self.infoDir, os.ModePerm)
	}

	self.mutexErr = new(sync.Mutex)
	self.warnMsg = make(chan string, Config().DefaultInt("sys::LogWarnQueueLen", 512))
	self.infoMsg = make(chan string, Config().DefaultInt("sys::LogInfoQueueLen", 2048))
	self.closeMsg = make(chan bool, 2)
	go self.warnRun()
	go self.infoRun()
}

//Run
func (self *log) warnRun() {
	for {
		msg := <-self.warnMsg
		if msg == "" {
			self.closeMsg <- true
			break
		}
		self.write(_LOG_WARN, msg)
	}
}

//Run
func (self *log) infoRun() {
	for {
		msg := <-self.infoMsg
		if msg == "" {
			self.closeMsg <- true
			break
		}
		self.write(_LOG_INFO, msg)
	}
}

//Error 写入错误
func (self *log) Error(str ...interface{}) {
	stack := string(debug.Stack())
	str = append(str, "\n"+stack)
	text := []interface{}{}
	if no := self.BseqNo(); no != "" {
		text = append(text, "gseq:"+no)
	}
	text = append(text, str...)
	datetime := self.nowDateTime()
	if self.debug {
		if Testing() {
			fmt.Println(datetime + " " + fmt.Sprint(text))
			return
		}
		fmt.Println(red(datetime + " " + fmt.Sprint(text)))
	}
	defer self.mutexErr.Unlock()
	self.mutexErr.Lock()
	self.write(_LOG_ERROR, datetime+" "+fmt.Sprint(text))
}

//Error 写入警告
func (self *log) Warning(str ...interface{}) {
	text := []interface{}{}
	if no := self.BseqNo(); no != "" {
		text = append(text, "gseq:"+no)
	}
	datetime := self.nowDateTime()
	text = append(text, str...)
	if self.debug {
		if Testing() {
			fmt.Println(datetime + " " + fmt.Sprint(text))
			return
		}
		fmt.Println(red(datetime + " " + fmt.Sprint(text)))
	}

	self.warnMsg <- datetime + " " + fmt.Sprint(text)
}

//Notice 写入普通
func (self *log) Notice(str ...interface{}) {
	text := []interface{}{}
	if no := self.BseqNo(); no != "" {
		text = append(text, "gseq:"+no)
	}
	datetime := self.nowDateTime()
	text = append(text, str...)
	if self.debug {
		if Testing() {
			fmt.Println(datetime + " " + fmt.Sprint(text))
			return
		}
		fmt.Println(green(datetime + " " + fmt.Sprint(text)))

	}
	self.infoMsg <- datetime + " " + fmt.Sprint(text)
}

//Debug 等同WriteInfo,只在dev模式下生效
func (self *log) Debug(str ...interface{}) {
	if !self.debug {
		return
	}
	self.Notice(str...)
}

//Write 写入日志
func (self *log) write(logType int, str string) {
	wt := self.getLogger(logType)
	if wt == nil {
		return
	}

	buf := []byte(str)
	wt.Write(append(buf, '\n'))
}

//Close 关闭日志
func (self *log) Close() {
	self.infoMsg <- ""
	self.warnMsg <- ""

	closeLen := 0
	for index := 0; index < 20; index++ {
		if closeLen == 2 {
			break
		}
		select {
		case _ = <-self.closeMsg:
			closeLen += 1
		default:
			//最多等待2秒
			time.Sleep(time.Millisecond * 100)
			continue
		}
	}

	self.errFile.Close()
	self.infoFile.Close()
	self.warnFile.Close()
}

//获取logger
func (self *log) getLogger(logType int) *os.File {
	var result *os.File

	fileName := self.getFileName(logType)
	switch logType {
	case _LOG_ERROR:
		if self.errFile == nil {
			self.errFile, _ = os.OpenFile(fileName, os.O_APPEND|os.O_RDWR, 0660)
			self.errFileName = fileName
		} else {
			if self.errFileName != fileName {
				self.errFile.Close()
				self.errFile, _ = os.OpenFile(fileName, os.O_APPEND|os.O_RDWR, 0660)
				self.errFileName = fileName
			}
		}
		result = self.errFile
	case _LOG_WARN:
		if self.warnFile == nil {
			self.warnFile, _ = os.OpenFile(fileName, os.O_APPEND|os.O_RDWR, 0660)
			self.warnFileName = fileName
		} else {
			if self.warnFileName != fileName {
				self.warnFile.Close()
				self.warnFile, _ = os.OpenFile(fileName, os.O_APPEND|os.O_RDWR, 0660)
				self.warnFileName = fileName
			}
		}
		result = self.warnFile
	default:
		if self.infoFile == nil {
			self.infoFile, _ = os.OpenFile(fileName, os.O_APPEND|os.O_RDWR, 0660)
			self.infoFileName = fileName
		} else {
			if self.infoFileName != fileName {
				self.infoFile.Close()
				self.infoFile, _ = os.OpenFile(fileName, os.O_APPEND|os.O_RDWR, 0660)
				self.infoFileName = fileName
			}
		}
		result = self.infoFile
	}

	return result
}

//getFileName 获取文件名
func (self *log) getFileName(logType int) string {
	dateDir := self.getDate(logType)
	hour, _, _ := time.Now().Clock()
	fileName := dateDir + "/" + strconv.Itoa(hour) + ".log"
	if FileExists(fileName) {
		return fileName
	}
	file, _ := os.Create(fileName)
	file.Close()
	return fileName
}

//GetDate 获取当前日期
func (self *log) getDate(logType int) string {
	var path string
	switch logType {
	case _LOG_ERROR:
		path = self.errorDir
	case _LOG_WARN:
		path = self.warnDir
	default:
		path = self.infoDir
	}
	year, month, day := time.Now().Date()

	dateDir := path + "/" + fmt.Sprintf("%d-%d-%d", year, month, day)
	if !FileExists(dateDir) {
		os.Mkdir(dateDir, os.ModePerm)
	}
	return dateDir
}

//NowDateTime 当前日期和时间
func (self *log) nowDateTime() string {
	return time.Now().Format("2006-01-02 15:04:05")
}

func (self *log) BseqNo() string {
	if self.bseqMap == nil {
		return ""
	}

	gseq := self.bseqMap.Get("gseq")
	if gseq == nil {
		return ""
	}

	str, ok := gseq.(string)
	if !ok {
		return ""
	}
	return str
}

//QueueLen 待处理日志长度
func (self *log) QueueLen() int {
	return len(self.infoMsg) + len(self.warnMsg)
}

var _log *log

//获取日志单例类
func Log() *log {
	if _log == nil {
		_log = new(log)
	}
	return _log
}

const (
	textBlack = iota + 30
	textRed
	textGreen
	textYellow
	textBlue
	textMagenta
	textCyan
	textWhite
)

func red(str string) string {
	return textColor(textRed, str)
}

func green(str string) string {
	return textColor(textGreen, str)
}

func yellow(str string) string {
	return textColor(textYellow, str)
}

func blue(str string) string {
	return textColor(textBlue, str)
}

func magenta(str string) string {
	return textColor(textMagenta, str)
}

func cyan(str string) string {
	return textColor(textCyan, str)
}

func white(str string) string {
	return textColor(textWhite, str)
}

func black(str string) string {
	return textColor(textBlack, str)
}

func textColor(color int, str string) string {
	switch color {
	case textBlack:
		return fmt.Sprintf("\x1b[0;%dm%s\x1b[0m", textBlack, str)
	case textRed:
		return fmt.Sprintf("\x1b[0;%dm%s\x1b[0m", textRed, str)
	case textGreen:
		return fmt.Sprintf("\x1b[0;%dm%s\x1b[0m", textGreen, str)
	case textYellow:
		return fmt.Sprintf("\x1b[0;%dm%s\x1b[0m", textYellow, str)
	case textBlue:
		return fmt.Sprintf("\x1b[0;%dm%s\x1b[0m", textBlue, str)
	case textMagenta:
		return fmt.Sprintf("\\x1b[0;%dm%s\x1b[0m", textMagenta, str)
	case textCyan:
		return fmt.Sprintf("\x1b[0;%dm%s\x1b[0m", textCyan, str)
	case textWhite:
		return fmt.Sprintf("\x1b[0;%dm%s\x1b[0m", textWhite, str)
	default:
		return str
	}
}

//PathExists 判断文件夹或文件是否存在
func FileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}
