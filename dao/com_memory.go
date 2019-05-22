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
	"sync"
	"time"

	"github.com/8treenet/gotree/helper"
	"github.com/8treenet/gotree/lib"
)

//ComMemory 内存数据源
type ComMemory struct {
	lib.Object
	comName   string
	open      bool
	dict      *lib.Dict
	dictMutex sync.Mutex
	dictEep   map[interface{}]int64
}

func (self *ComMemory) Gotree(child interface{}) *ComMemory {
	self.Object.Gotree(self)
	self.AddChild(self, child)
	self.comName = ""
	self.AddSubscribe("MemoryOn", self.memoryOn)

	self.dict = new(lib.Dict).Gotree()
	self.dictEep = make(map[interface{}]int64)
	return self
}

//TestOn 单元测试 开启
func (self *ComMemory) TestOn() {
	logOn()
	mode := helper.Config().String("sys::Mode")
	if mode == "prod" {
		helper.Exit("ComMemory-TestOn-mode Unit test model is not available in production environments")
	}
	self.DaoInit()
	if helper.Config().DefaultString("com_on::"+self.comName, "") == "" {
		helper.Exit("ComMemory-TestOn Component not found com.conf com_on " + self.comName)
	}
	self.open = true
}

//daoOn 开启回调
func (self *ComMemory) memoryOn(arg ...interface{}) {
	comName := arg[0].(string)
	if comName == self.comName {
		self.open = true
	}
}

// Set
func (self *ComMemory) Set(key, value interface{}) {
	defer self.dictMutex.Unlock()
	self.dictMutex.Lock()
	self.dict.Set(key, value)
	return
}

// Get 不存在 返回false
func (self *ComMemory) Get(key, value interface{}) bool {
	defer self.dictMutex.Unlock()
	self.dictMutex.Lock()
	e := self.dict.Get(key, value)
	if e != nil {
		return false
	}
	return true
}

// SetTnx 当key不存在设置成功返回true 否则返回false
func (self *ComMemory) Setnx(key, value interface{}) bool {
	defer self.dictMutex.Unlock()
	self.dictMutex.Lock()
	if self.dict.Check(key) {
		return false
	}
	self.dict.Set(key, value)
	return true
}

// MultiSet 多条
func (self *ComMemory) MultiSet(args ...interface{}) {
	defer self.dictMutex.Unlock()
	self.dictMutex.Lock()
	if len(args) <= 0 {
		helper.Exit("MultiSet len(args) <= 0")
	}
	//多参必须是偶数
	if (len(args) & 1) == 1 {
		helper.Exit("MultiSet len(args)&1 == 1")
	}

	for index := 0; index < len(args); index += 2 {
		self.dict.Set(args[index], args[index+1])
	}
	return
}

// MultiSet 多条
func (self *ComMemory) MultiSetnx(args ...interface{}) bool {
	defer self.dictMutex.Unlock()
	self.dictMutex.Lock()
	if len(args) <= 0 {
		helper.Exit("MultiSet len(args) <= 0")
	}
	//多参必须是偶数
	if (len(args) & 1) == 1 {
		helper.Exit("MultiSet len(args)&1 == 1")
	}

	for index := 0; index < len(args); index += 2 {
		if self.dict.Check(args[index]) {
			return false
		}
	}
	for index := 0; index < len(args); index += 2 {
		self.dict.Set(args[index], args[index+1])
	}
	return true
}

// Eexpire 设置 key 的生命周期, sec:秒
func (self *ComMemory) Expire(key interface{}, sec int) {
	defer self.dictMutex.Unlock()
	self.dictMutex.Lock()
	if !self.dict.Check(key) {
		//不存在,直接返回
		return
	}
	self.dictEep[key] = time.Now().Unix() + int64(sec)
	return
}

// Delete 删除 key
func (self *ComMemory) Delete(keys ...interface{}) {
	defer self.dictMutex.Unlock()
	self.dictMutex.Lock()
	for _, key := range keys {
		self.dict.Del(key)
		delete(self.dictEep, key)
	}
}

// DeleteAll 删除全部数据
func (self *ComMemory) DeleteAll(key interface{}) {
	defer self.dictMutex.Unlock()
	self.dictMutex.Lock()
	self.dict.DelAll()
	self.dictEep = make(map[interface{}]int64)
}

// Incr add 加数据, key必须存在否则errror
func (self *ComMemory) Incr(key interface{}, addValue int64) (result int64, e error) {
	defer self.dictMutex.Unlock()
	self.dictMutex.Lock()
	e = self.dict.Get(key, &result)
	if e != nil {
		return
	}
	result += addValue
	self.dict.Set(key, result)
	return
}

// AllKey 获取全部key
func (self *ComMemory) AllKey() (result []interface{}) {
	defer self.dictMutex.Unlock()
	self.dictMutex.Lock()
	result = self.dict.Keys()
	return
}

// MemoryTimeout 超时处理
func (self *ComMemory) MemoryTimeout(now int64) {
	keys := []interface{}{}
	self.dictMutex.Lock()
	for k, v := range self.dictEep {
		if now < v {
			continue
		}
		keys = append(keys, k)
	}
	self.dictMutex.Unlock()

	for index := 0; index < len(keys); index++ {
		self.Delete(keys[index])
	}
	return
}

func (self *ComMemory) DaoInit() {
	if self.comName == "" {
		self.comName = self.TopChild().(comName).Com()
	}
}
