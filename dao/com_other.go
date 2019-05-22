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
	"github.com/8treenet/gotree/helper"
	"github.com/8treenet/gotree/lib"
	"github.com/8treenet/gotree/lib/rpc"
)

// Other data sources
type ComOther struct {
	lib.Object
	comName string
}

func (self *ComOther) Gotree(child interface{}) *ComOther {
	self.Object.Gotree(self)
	self.AddChild(self, child)
	self.comName = ""
	self.AddSubscribe("OtherOn", self.otherOn)
	return self
}

func (self *ComOther) DaoInit() {
	if self.comName == "" {
		self.comName = self.TopChild().(comName).Com()
	}
}

//TestOn 单元测试 开启
func (self *ComOther) TestOn() {
	logOn()
	mode := helper.Config().String("sys::Mode")
	if mode == "prod" {
		helper.Exit("ComOther-TestOn Unit test model is not available in production environments")
	}
	rpc.GoDict().Set("gseq", "ModelUnit")
	self.DaoInit()
	if helper.Config().DefaultString("com_on::"+self.comName, "") == "" {
		helper.Exit("ComOther-TestOn Component not found com.conf com_on " + self.comName)
	}
	self.TopChild().(prepare).Prepare()
}

type prepare interface {
	Prepare()
}

//otherOn
func (self *ComOther) otherOn(arg ...interface{}) {
	comName := arg[0].(string)
	if comName == self.comName {
		self.TopChild().(prepare).Prepare()
	}
}
