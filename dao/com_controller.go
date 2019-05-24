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
	"reflect"

	"github.com/8treenet/gotree/helper"

	"github.com/8treenet/gotree/dao/model"
	"github.com/8treenet/gotree/remote_call"
)

//ComController
type ComController struct {
	remote_call.RpcController
	selfName string
}

//Gotree
func (self *ComController) Gotree(child interface{}) *ComController {
	self.RpcController.Gotree(self)
	self.AddChild(self, child)

	type fun interface {
		RpcName() string
	}
	self.selfName = self.TopChild().(fun).RpcName()
	return self
}

//Model 服务定位器获取model
func (self *ComController) Model(child interface{}) {
	modelDao := reflect.ValueOf(child).Elem().Interface().(comName).Com()
	if self.selfName != modelDao {
		helper.Exit("ComController-Model-modelDao Not in the same com, prohibit invoking")
	}

	err := _msl.Service(child)
	if err != nil {
		helper.Exit("ComController-Model-Service Prohibit invoking " + err.Error())
	}
	return
}

//Cache 服务定位器获取Cache
func (self *ComController) Cache(child interface{}) {
	cacheDao := reflect.ValueOf(child).Elem().Interface().(comName).Com()
	if self.selfName != cacheDao {
		helper.Exit("ComController-Cache-cacheDao Not in the same com, prohibit invoking")
	}

	err := _csl.Service(child)
	if err != nil {
		helper.Exit("ComController-Cache-Service Prohibit invoking " + err.Error())
	}
	return
}

//Api 服务定位器获取Api
func (self *ComController) Api(child interface{}) {
	err := _api.Service(child)
	if err != nil {
		helper.Exit("ComController-Api-Service Prohibit invoking " + err.Error())
	}
	return
}

//Memory 服务定位器获取Memory
func (self *ComController) Memory(child interface{}) {
	apiDao := reflect.ValueOf(child).Elem().Interface().(comName).Com()
	if self.selfName != apiDao {
		helper.Exit("ComController-Memory-apiDao Not in the same com, prohibit invoking")
	}

	err := _esl.Service(child)
	if err != nil {
		helper.Exit("ComController-Memory-Service Prohibit invoking" + err.Error())
	}
	return
}

//Other 服务定位器获取其他数据源
func (self *ComController) Other(child interface{}) {
	apiDao := reflect.ValueOf(child).Elem().Interface().(comName).Com()
	if self.selfName != apiDao {
		helper.Exit("ComController--Other Not in the same com, prohibit invoking")
	}

	err := _other.Service(child)
	if err != nil {
		helper.Exit("ComController-Memory-Service Prohibit invoking" + err.Error())
	}
	return
}

//Transaction 事务
func (self *ComController) Transaction(fun func() error) error {
	return model.Transaction(self.selfName, fun)
}

//Queue 队列处理
func (self *ComController) Queue(name string, fun func() error) {
	q, ok := queueMap[self.selfName+"_"+name]
	if !ok {
		helper.Exit("ComController-Queue Unregistered queue:" + self.selfName + "." + name)
	}
	q.cast(fun)
}

//TotalPage 总页数
func (self *ComController) TotalPage(size, pageSize int) int {
	if size%pageSize == 0 {
		return size / pageSize
	}
	return size/pageSize + 1
}
