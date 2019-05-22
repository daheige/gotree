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

package dao

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"time"

	"github.com/8treenet/gotree/helper"
	"github.com/8treenet/gotree/lib"
)

var tp *lib.LimiteGo

type Request func(request *helper.HTTPRequest)

type ComApi struct {
	lib.Object
	open    bool
	apiName string
	host    string

	dayMax     int
	hourMax    int
	minMax     int
	dayCount   int
	hourCount  int
	minCount   int
	countMutex sync.Mutex
}

func (self *ComApi) Gotree(child interface{}) *ComApi {
	self.Object.Gotree(self)
	self.AddChild(self, child)
	self.apiName = ""

	self.dayMax = 0
	self.hourMax = 0
	self.minMax = 0
	self.dayCount = 0
	self.hourCount = 0
	self.minCount = 0
	self.apiOn()
	return self
}

//TestOn 单元测试 开启
func (self *ComApi) TestOn() {
	logOn()
	mode := helper.Config().String("sys::Mode")
	if mode == "prod" {
		helper.Exit("ComApi-TestOn Unit test api is not available in production environments")
	}
	self.apiOn()
}

type apiName interface {
	Api() string
}

//apiOn
func (self *ComApi) apiOn() {
	self.open = true
	self.apiName = self.TopChild().(apiName).Api()
	self.host = helper.Config().String("api::" + self.apiName)
}

//HttpGet
func (self *ComApi) HttpGet(apiAddr string, args map[string]interface{}, callback ...Request) (result []byte, e error) {
	req := helper.HttpGet(self.host + apiAddr + "?" + httpBuildQuery(args))
	req = req.SetTimeout(3*time.Second, 10*time.Second)
	fun := func() error {
		var reqerr error
		result, reqerr = req.Bytes()
		return reqerr
	}

	if len(callback) > 0 {
		callback[0](req)
	}

	if self.limited() {
		e = self.count()
		if e != nil {
			return
		}
	}
	mode := helper.Config().String("sys::Mode")
	//2次重试
	for index := 0; index < 2; index++ {
		e = tp.Go(fun)
		if mode == "dev" {
			helper.Log().Notice("ApiGet:", self.host+apiAddr, "ReqData:", args, "ResData:", string(result), "error:", e)
		}
		if e != nil {
			continue
		}

		break
	}

	return
}

//HttpPost
func (self *ComApi) HttpPost(apiAddr string, args map[string]interface{}, callback ...Request) (result []byte, e error) {
	req := helper.HttpPost(self.host + apiAddr)
	req = req.SetTimeout(3*time.Second, 10*time.Second)
	for k, v := range args {
		req.Param(k, fmt.Sprint(v))
	}

	if len(callback) > 0 {
		callback[0](req)
	}

	if self.limited() {
		e = self.count()
		if e != nil {
			return
		}
	}

	fun := func() error {
		var reqerr error
		result, reqerr = req.Bytes()
		return reqerr
	}

	mode := helper.Config().String("sys::Mode")
	//2次重试
	for index := 0; index < 2; index++ {
		e = tp.Go(fun)
		if mode == "dev" {
			helper.Log().Notice("ApiPost:", self.host+apiAddr, "ReqData:", args, "ResData:", string(result), "error:", e)
		}
		if e != nil {
			continue
		}

		break
	}

	return
}

//HttpPostJson
func (self *ComApi) HttpPostJson(apiAddr string, raw interface{}, callback ...Request) (result []byte, e error) {
	req := helper.HttpPost(self.host + apiAddr)
	req = req.SetTimeout(3*time.Second, 10*time.Second)
	req.JSONBody(raw)

	if len(callback) > 0 {
		callback[0](req)
	}

	if self.limited() {
		e = self.count()
		if e != nil {
			return
		}
	}

	fun := func() error {
		var reqerr error
		result, reqerr = req.Bytes()
		return reqerr
	}

	mode := helper.Config().String("sys::Mode")
	//2次重试
	for index := 0; index < 2; index++ {
		e = tp.Go(fun)
		if mode == "dev" {
			helper.Log().Notice("ApiPostJson:", self.host+apiAddr, "ReqData:", raw, "ResData:", string(result), "error:", e)
		}
		if e != nil {
			continue
		}

		break
	}

	return
}

//HostAddr 获取本api dao的host地址
func (self *ComApi) HostAddr() string {
	return self.host
}

//limited api限制
func (self *ComApi) count() error {
	defer self.countMutex.Unlock()
	self.countMutex.Lock()
	if self.minMax > 0 && self.minCount >= self.minMax {
		return errors.New("ComApi-count Frequency per minute exceeded the limit, host:" + self.host)
	}
	if self.hourMax > 0 && self.hourCount >= self.hourMax {
		return errors.New("ComApi-count Hourly usage exceeded the limit, host:" + self.host)
	}
	if self.dayMax > 0 && self.dayCount >= self.dayMax {
		return errors.New("ComApi-count Frequency of daily use exceeds the limit, host:" + self.host)
	}

	if self.minMax > 0 {
		self.minCount += 1
	}
	if self.hourMax > 0 {
		self.hourCount += 1
	}
	if self.dayMax > 0 {
		self.dayCount += 1
	}
	return nil
}

//limited api限制是否开启
func (self *ComApi) limited() bool {
	if self.dayMax > 0 || self.minMax > 0 || self.hourMax > 0 {
		return true
	}
	return false
}

//DayCountLimit 每日限制
func (self *ComApi) DayCountLimit(count int) {
	if self.dayMax > 0 {
		return
	}
	self.dayMax = count
	lib.RunDefaultTimer(24*60*60*1000, func() {
		self.countMutex.Lock()
		self.dayCount = 0
		self.countMutex.Unlock()
	})
}

//HourCountLimit 每小时限制
func (self *ComApi) HourCountLimit(count int) {
	if self.hourMax > 0 {
		return
	}
	self.hourMax = count
	lib.RunDefaultTimer(60*60*1000, func() {
		self.countMutex.Lock()
		self.hourCount = 0
		self.countMutex.Unlock()
	})
}

//MinCountLimit 每分钟限制
func (self *ComApi) MinCountLimit(count int) {
	if self.minMax > 0 {
		return
	}
	self.minMax = count
	lib.RunDefaultTimer(60*1000, func() {
		self.countMutex.Lock()
		self.minCount = 0
		self.countMutex.Unlock()
	})
}

//HttpBuildQuery转换get参数
func httpBuildQuery(args map[string]interface{}) string {
	result := ""
	for k, v := range args {
		result += k + "=" + fmt.Sprint(v) + "&"
	}

	return url.PathEscape(strings.TrimSuffix(result, "&"))
}
