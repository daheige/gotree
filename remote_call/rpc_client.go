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

package remote_call

import (
	"errors"
	"fmt"
	"net/rpc"
	"reflect"
	"strings"
	"time"

	"github.com/8treenet/gotree/helper"
	"github.com/8treenet/gotree/lib"
	gotree_rpc "github.com/8treenet/gotree/lib/rpc"
)

type RpcClient struct {
	lib.Object
	limiteGo    *lib.LimiteGo
	retry       int
	sleepCount  int
	connReuse   bool //连接池,连接复用 默认开启
	innerMaster *InnerMaster
	rpcQps      *RpcQps
	rpcBreak    *RpcBreak
}

type cmdCall interface {
	RemoteAddr(interface{}) (string, error) //远程地址 0.0.0.1:9123
	ServiceMethod() string                  //远程方法
	Cache() interface{}                     //如果开启缓存返回标识否则nil
}

type businessCmd interface {
	BusinessAddr() string //bussiness地址
}

//并发数量 和rpc失败重试次数
func (self *RpcClient) Gotree(concurrency int, timeout int) *RpcClient {
	self.Object.Gotree(self)
	self.limiteGo = new(lib.LimiteGo).Gotree(concurrency)
	maxConnCount += concurrency / 64

	self.retry = timeout //网络超时重试重试次数
	self.connReuse = true

	self.sleepCount = timeout * 1000 //转毫秒
	if self.sleepCount > 2000 {
		ms10count := (self.sleepCount - 2000) / 10
		self.sleepCount = ms10count + 2000
	}
	return self
}

func (self *RpcClient) Start() {
	self.GetComponent(&self.innerMaster)
	self.GetComponent(&self.rpcBreak)
	self.GetComponent(&self.rpcQps)
}

//Call cmd obj参数, reply &daostruct 或 &businessstruct
func (self *RpcClient) Call(obj interface{}, reply interface{}) (err error) {
	var (
		identity    string
		identityUse bool = false
		beginMs     int64
		timeoutCall bool
	)

	cmd := obj.(cmdCall)
	if !self.innerMaster.ping {
		beginMs = time.Now().UnixNano() / 1e6
	}

	defer func() {
		if !self.innerMaster.ping && !identityUse && (err == nil || err != unknownNetwork || err != helper.ErrBreaker) {
			//无错误或非网络错误 并且 非cmd缓存 加入统计
			self.qps(cmd.ServiceMethod(), time.Now().UnixNano()/1e6-beginMs)
		}
		if err != nil {
			return
		}
		//如果未开启缓存 并且使用缓存获取的数据不处理
		if identity == "" && !identityUse {
			return
		}
		cacheValue := reflect.Indirect(reflect.ValueOf(reply)).Interface()
		gotree_rpc.GoDict().Set(identity, cacheValue)
	}()
	if self.rpcBreak.Breaking(cmd) {
		return helper.ErrBreaker
	}
	cacheIdentity := cmd.Cache()
	if cacheIdentity != nil {
		identity = self.ClassName(cmd) + "_" + fmt.Sprint(cacheIdentity)
		if gotree_rpc.GoDict().Eval(identity, reply) == nil {
			identityUse = true
			return
		}
	}

	fun := func() error {
		var addr string
		var resultErr error
		if self.innerMaster.ping {
			//api层调用Business
			bc, ok := obj.(businessCmd)
			if ok {
				addr = bc.BusinessAddr()
			} else {
				addr = self.innerMaster.randomRpcAddr()
			}
		} else {
			//Business层调用dao
			addr, resultErr = cmd.RemoteAddr(self.innerMaster)
			if resultErr != nil {
				return resultErr
			}
		}
		if addr == "" {
			return ErrNetwork
		}

		jrc, e := self.createJsonCall(addr)
		if e != nil {
			return ErrNetwork
		}

		callDone := jrc.client.Go(cmd.ServiceMethod(), cmd, reply, make(chan *rpc.Call, 1)).Done
		e = errors.New("RpcClient-Call Request timed out")
		timeoutCall = true
		for index := 0; index < self.sleepCount; index++ {
			select {
			case call := <-callDone:
				timeoutCall = false
				e = call.Error
				break
			default:
				if index < 2000 {
					time.Sleep(1 * time.Millisecond)
				} else {
					time.Sleep(10 * time.Millisecond)
				}
			}
			if !timeoutCall {
				break
			}
		}
		self.releaseJsonCall(jrc, e) //释放
		resultErr = e

		//如果是网络错误,sleep后重试
		if resultErr == nil {
			return resultErr
		}
		emsg := e.Error()
		if emsg == ErrShutdown.Error() || emsg == Unexpected.Error() || emsg == ErrConnect.Error() || strings.Contains(emsg, "closed network connection") || strings.Contains(emsg, "read: connection reset by peer") || strings.Contains(emsg, "write: broken pipe") || strings.Contains(emsg, "ServerShutDown") {
			return ErrNetwork
		}
		return resultErr
	}

	for index := 1; index <= self.retry; index++ {
		err = self.limiteGo.Go(fun)
		//如果无错误 或 不是网络造成的错误 return
		if err == nil || err != ErrNetwork {
			go self.rpcBreak.Call(cmd, timeoutCall)
			return err
		}
		//如果是网络错误 并且最后一次重试
		if err == ErrNetwork && index == self.retry {
			go self.rpcBreak.Call(cmd, true)
			break
		}
		time.Sleep(1 * time.Second)
	}
	err = unknownNetwork
	return
}

func (self *RpcClient) createJsonCall(addr string) (client *rpcConn, err error) {
	if !self.connReuse {
		client, e := jsonRpc(addr)
		if e != nil {
			return nil, e
		}
		r := new(rpcConn)
		r.client = client
		return r, nil
	}
	//复用连接
	return connPool.takeConn(addr)
}

func (self *RpcClient) releaseJsonCall(client *rpcConn, err error) {
	if !self.connReuse {
		client.client.Close()
		return
	}
	//复用连接
	netok := true
	if err == nil {
		client.release(netok)
		return
	}
	emsg := err.Error()
	if emsg == ErrShutdown.Error() || emsg == ErrConnect.Error() || emsg == ErrNetwork.Error() {
		netok = false
	}
	client.release(netok)
	return
}

func (self *RpcClient) Close() {
	connPool.Close()
	return
}

func (self *RpcClient) qps(serviceMethod string, ms int64) {
	if self.rpcQps == nil {
		panic("RpcQps not obtained")
	}
	go func() {
		self.rpcQps.Qps(serviceMethod, ms)
	}()
}

var ErrShutdown = errors.New("connection is shut down")
var ErrConnect = errors.New("dial is fail")
var ErrNetwork = errors.New("connection is shut down")
var Unexpected = errors.New("unexpected EOF")
var unknownNetwork = errors.New("Unknown network error")
