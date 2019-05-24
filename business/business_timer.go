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
package business

import (
	"reflect"
	"runtime"
	"strings"

	"github.com/8treenet/gotree/helper"
	"github.com/8treenet/gotree/lib"
)

type BusinessTimer struct {
	lib.Object
	timerCallBack []trigger
	timerStopTick []lib.StopTick
	_openService  bool
}

func (self *BusinessTimer) Gotree(child interface{}) *BusinessTimer {
	self.Object.Gotree(self)
	self.AddChild(self, child)
	self.timerCallBack = make([]trigger, 0)
	self.AddSubscribe("TimerOn", self.timer)
	self.AddSubscribe("shutdown", self.stopTick)
	self._openService = false
	self.timerStopTick = make([]lib.StopTick, 0)
	return self
}

//RegisterTickTrigger 注册毫秒tick 触发器 delay=延迟毫秒
func (self *BusinessTimer) RegisterTickTrigger(ms int, fun func(), delay ...int) {
	delayms := 0
	if len(delay) > 0 {
		delayms = delay[0]
	}
	self.timerCallBack = append(self.timerCallBack, trigger{t: ms, tickFun: fun, delay: delayms})
	return
}

//RegisterDayTrigger 注册每天小时 触发器 hour:0 - 23, minute :0 - 59
func (self *BusinessTimer) RegisterDayTrigger(hour, minute int, fun func()) {
	if hour < 0 && hour > 23 {
		helper.Log().Error("BusinessTimer-RegisterDayTrigger Invalid argument hour:0 - 23")
	}

	if minute < 0 && minute > 59 {
		helper.Log().Error("BusinessTimer-RegisterDayTrigger Invalid argument minute :0 - 59")
	}
	self.timerCallBack = append(self.timerCallBack, trigger{t: hour, t2: minute, dayFun: fun})
	return
}

func (self *BusinessTimer) stopTick(args ...interface{}) {
	for _, stopFun := range self.timerStopTick {
		stopFun.Stop()
	}
}

//timer 启动观察者
func (self *BusinessTimer) timer(args ...interface{}) {
	selfName := self.ClassName(self.TopChild())
	//获取配置文件开启的定时器服务
	open := false
	for _, namei := range args {
		name := namei.(string)
		if name == selfName {
			open = true
			break
		}
	}

	if !open {
		return
	}
	for _, fun := range self.timerCallBack {
		if fun.tickFun != nil {
			funName := selfName + "."
			funcFor := runtime.FuncForPC(reflect.ValueOf(fun.tickFun).Pointer()).Name()
			if list := strings.Split(funcFor, "."); len(list) > 0 {
				funName += list[len(list)-1]
			}
			if list := strings.Split(funName, "-"); len(list) > 0 {
				funName = list[0]
			}

			tick := lib.RunTick(int64(fun.t), fun.tickFun, funName, fun.delay)
			self.timerStopTick = append(self.timerStopTick, tick)
		}

		if fun.dayFun != nil {
			funName := selfName + "."
			funcFor := runtime.FuncForPC(reflect.ValueOf(fun.dayFun).Pointer()).Name()
			if list := strings.Split(funcFor, "."); len(list) > 0 {
				funName += list[len(list)-1]
			}
			if list := strings.Split(funName, "-"); len(list) > 0 {
				funName = list[0]
			}
			lib.RunDay(fun.t, fun.t2, fun.dayFun, funName)
		}
	}
	return
}

func (self *BusinessTimer) OpenTimer() {
	exist := _tsl.CheckService(self.TopChild())
	//禁止重复实例化
	if exist {
		helper.Exit("BusinessTimer-BusinessTimer Prohibit duplicate instantiation")
	}
	self._openService = true
	return
}

//Async 异步执行
func (self *BusinessTimer) Async(run func(ac AsyncController), completeds ...func()) {
	var completed func()
	if len(completeds) > 0 {
		completed = completeds[0]
	}
	ac := new(async).Gotree(run, completed)
	go ac.execute()
}

type trigger struct {
	t       int
	t2      int
	delay   int //延迟毫秒
	tickFun func()
	dayFun  func()
}
