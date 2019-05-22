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
	"fmt"
	"strconv"
	"strings"

	"github.com/8treenet/gotree/dao/redis"
	"github.com/8treenet/gotree/helper"
	"github.com/8treenet/gotree/lib"
)

type ComCache struct {
	lib.Object
	open    bool
	comName string
}

func (self *ComCache) Gotree(child interface{}) *ComCache {
	self.Object.Gotree(self)
	self.AddChild(self, child)
	self.AddSubscribe("DaoTelnet", self.daoTelnet)
	self.AddSubscribe("CacheOn", self.cacheOn)
	self.comName = ""
	return self
}

//TestOn 单元测试 开启
func (self *ComCache) TestOn() {
	logOn()
	mode := helper.Config().String("sys::Mode")
	if mode == "prod" {
		helper.Exit("ComCache-TestOn Unit test cache is not available in production environments")
	}
	self.DaoInit()
	if helper.Config().DefaultString("com_on::"+self.comName, "") == "" {
		helper.Exit("ComCache-TestOn Component not found dao.conf com_on " + self.comName)
	}
	self.redisOn()
}

//daoOn 开启回调
func (self *ComCache) daoTelnet(args ...interface{}) {
	dao := self.TopChild().(comName)
	comName := dao.Com()

	for _, arg := range args {
		dao := arg.(comNode)
		if dao.Name == comName {
			self.redisOn()
			return
		}
	}
}

//cacheOn 开启回调
func (self *ComCache) cacheOn(arg ...interface{}) {
	comName := arg[0].(string)
	if comName == self.comName {
		self.redisOn()
	}
}

//daoOn 开启回调
func (self *ComCache) redisOn() {
	self.open = true
	if !connectDao(self.comName + "cache") {
		return
	}
	redisinfo := helper.Config().String("redis::" + self.comName)
	if redisinfo == "" {
		helper.Log().Error("ComCache-redisinfo Config file dao:" + self.comName + " redis address error or not found")
	}
	list := strings.Split(redisinfo, ";")
	m := map[string]string{}
	for _, item := range list {
		kv := strings.Split(item, "=")
		if len(kv) != 2 {
			helper.Log().Error("ComCache-kv Config file dao:" + self.comName + " redis address error or not found")
			continue
		}
		m[kv[0]] = kv[1]
	}

	client := redis.GetClient(self.comName)
	if client != nil {
		//已注册
		return
	}

	maxIdleConns := helper.Config().String("redis::" + self.comName + "MaxIdleConns")
	maxOpenConns := helper.Config().String("redis::" + self.comName + "MaxOpenConns")
	if maxIdleConns == "" {
		maxIdleConns = helper.Config().DefaultString("sys::RedisMaxIdleConns", "1")
	}
	if maxOpenConns == "" {
		maxOpenConns = helper.Config().DefaultString("sys::RedisMaxOpenConns", "2")
	}
	imaxIdleConns, ei := strconv.Atoi(maxIdleConns)
	imaxOpenConns, eo := strconv.Atoi(maxOpenConns)
	if ei != nil || eo != nil || imaxIdleConns == 0 || imaxOpenConns == 0 || imaxIdleConns > imaxOpenConns {
		helper.Exit("ComCache Connect dao redis:" + self.comName + "failed, error: MaxIdleConns or MaxOpenConns are invalid argumens," + fmt.Sprint(imaxIdleConns, imaxOpenConns))
	}

	db, _ := strconv.Atoi(m["database"])
	helper.Log().Notice("ComCache Connect com " + self.comName + " redis," + " MaxIdleConns:" + maxIdleConns + " MaxOpenConns:" + maxOpenConns + " config:" + fmt.Sprint(m))
	client, e := redis.NewCache(m["server"], m["password"], db, imaxIdleConns, imaxOpenConns)
	if e != nil {
		helper.Exit(e.Error())
	}
	redis.AddDatabase(self.comName, client)
}

//Do
func (self *ComCache) Do(cmd string, args ...interface{}) (reply interface{}, e error) {
	if !self.open || self.comName == "" {
		helper.Exit("ComCache-Do-cache error: Not opened dao:" + self.comName)
	}
	reply, e = redis.Do(self.comName, cmd, args...)
	return
}

func (self *ComCache) Connections(m map[string]int) {
	if !self.open {
		return
	}
	n := redis.GetConnects(self.comName)
	m[self.comName] = n
}

func (self *ComCache) DaoInit() {
	if self.comName == "" {
		self.comName = self.TopChild().(comName).Com()
	}
}
