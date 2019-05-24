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
	"strings"
	"time"

	"github.com/8treenet/gotree/helper"
	"github.com/8treenet/gotree/lib"
	"github.com/8treenet/gotree/lib/rpc"
	"github.com/8treenet/gotree/remote_call"
)

var _ssl *lib.ServiceLocator //系统服务定位器
var _tsl *lib.ServiceLocator //定时业务
var _timer []string

func init() {
	helper.LoadConfig("business")
	logOn()
	appStart()
	_ssl = new(lib.ServiceLocator).Gotree()
	_tsl = new(lib.ServiceLocator).Gotree()

	concurrency := concurrency()
	//注册相关组件
	_ssl.AddComponent(new(remote_call.InnerMaster).Gotree())
	innerRpcServer := new(remote_call.InnerServerController).Gotree()
	client := new(remote_call.RpcClient).Gotree(concurrency, helper.Config().DefaultInt("sys::CallDaoTimeout", 12))
	_ssl.AddComponent(client)
	_ssl.AddComponent(new(remote_call.RpcQps).Gotree())
	_ssl.AddComponent(new(remote_call.RpcBreak).Gotree())
	remote_call.RpcServerRegister(innerRpcServer)
	_timer = []string{}
	helper.SetGoDict(rpc.GoDict())
	lib.SetGoDict(rpc.GoDict())
	remote_call.AsynNumFunc = AsyncNum
}

//RpcServerRegister 注册rpc服务
func RegisterController(controller interface{}) {
	remote_call.RpcServerRegister(controller)
}

//RegisterTimer注册定时器
func RegisterTimer(service interface{}) {
	type openService interface {
		OpenTimer()
	}

	os := service.(openService)
	os.OpenTimer()
	_tsl.AddService(service)
}

func Run(args ...interface{}) {
	rpc.GoDict().Remove()
	printSystem()
	timerOn()
	var bindAddr string
	if len(args) == 0 {
		bindAddr = helper.Config().String("dispersed::BindAddr")
	}

	//通知所有定时服务
	_tsl.NotitySubscribe("TimerOn", timerServices()...)
	helper.Log().Notice("business run ....")
	var client *remote_call.RpcClient
	var breaker *remote_call.RpcBreak
	_ssl.GetComponent(&client)
	_ssl.GetComponent(&breaker)
	_ssl.NotitySubscribe("startup")
	breaker.RunTick()
	client.Start()
	rpcSer := remote_call.RpcServerRun(bindAddr)
	_ssl.NotitySubscribe("shutdown")
	breaker.StopTick()
	//优雅关闭
	for index := 0; index < 30; index++ {
		//当前rpc数量, 天定时器, 异步数量
		num := rpc.CurrentCallNum()
		timenum := lib.CurrentTimeNum()
		anum := AsyncNum()
		helper.Log().Notice("business run close: Timing service surplus:", timenum, "Request service surplus:", num, "Async service surplus:", anum)
		if num <= 0 && anum <= 0 && timenum <= 0 {
			break
		}
		time.Sleep(time.Second * 1)
	}
	rpcSer.Close()
	client.Close()
	helper.Log().Notice("business close: success")
	helper.Log().Close()
}

func timerServices() (list []interface{}) {
	for _, serName := range _timer {
		list = append(list, serName)
	}
	return
}

func logOn() {
	dir := helper.Config().DefaultString("sys::LogDir", "log")
	//生产环境定位 ~/log/xx目录
	helper.Log().Init(dir, rpc.GoDict())
}

//读取配置文件启动定时 service服务
func timerOn() {
	serList := strings.Split(helper.Config().String("TimerOn"), ",")
	for _, ser := range serList {
		_timer = append(_timer, ser)
	}
}

func concurrency() (concurrency int) {
	//读取最大调用dao并发数量
	concurrency = helper.Config().DefaultInt("sys::CallDaoConcurrency", 8192)
	return
}
