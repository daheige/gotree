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

	"github.com/8treenet/gotree/dao/gtorm"
	"github.com/8treenet/gotree/helper"
	"github.com/8treenet/gotree/lib"
	"github.com/8treenet/gotree/lib/chart"

	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/8treenet/gotree/lib/rpc"

	_ "github.com/go-sql-driver/mysql"
)

type ComModel struct {
	lib.Object
	open    bool
	comName string
	node    *chart.Node
}

var (
	node               = new(chart.Node).Node(50)
	modelProfiler      = false
	modelProfilerCount = map[string]int{}
	modelProfilerSync  sync.Mutex
)

func init() {
	gtorm.RegisterDriver("mysql", gtorm.DRMySQL)
	gtorm.RegisterDriver("mssql", gtorm.DRMySQL)
}

func (self *ComModel) Gotree(child interface{}) *ComModel {
	self.Object.Gotree(self)
	self.AddChild(self, child)
	self.open = false
	self.comName = ""

	self.AddSubscribe("DaoTelnet", self.daoTelnet)
	self.AddSubscribe("ModelOn", self.modelOn)
	return self
}

type Conn interface {
	Raw(query string, args ...interface{}) gtorm.RawSeter
}

//Orm 获取orm
func (self *ComModel) Conn() Conn {
	if !self.open {
		helper.Exit("ComModel-Conn open model error: Not opened com:" + self.comName)
	}
	if self.comName == "" {
		helper.Exit("ComModel-Conn This is an unregistered com")
		return nil
	}
	o := gtorm.New(self.comName)
	if modelProfiler {
		o.RawCallBack(func(sql string, args []interface{}) {
			self.profiler(sql, args...)
		})
	}
	return o
}

//TestOn 单元测试 开启
func (self *ComModel) TestOn() {
	logOn()
	mode := helper.Config().String("sys::Mode")
	if mode == "prod" {
		helper.Exit("ComModel-TestOn Unit test model is not available in production environments")
	}
	rpc.GoDict().Set("gseq", "ModelUnit")
	self.DaoInit()
	if helper.Config().DefaultString("com_on::"+self.comName, "") == "" {
		helper.Exit("ComModel-TestOn Component not found com.conf com_on " + self.comName)
	}
	self.ormOn()
}

//daoOn 开启回调
func (self *ComModel) daoTelnet(args ...interface{}) {
	for _, arg := range args {
		dao := arg.(comNode)
		if dao.Name == self.comName {
			self.ormOn()
			return
		}
	}
}

//modelOn 开启回调
func (self *ComModel) modelOn(arg ...interface{}) {
	comName := arg[0].(string)
	if comName == self.comName {
		self.ormOn()
	}
}

//ormOn 开启orm
func (self *ComModel) ormOn() {
	self.open = true

	if !connectDao(self.comName + "model") {
		return
	}
	//处理连接
	driver := "mysql"
	dbconfig := helper.Config().String("mysql::" + self.comName)
	dbMaxIdleConns := helper.Config().String("mysql::" + self.comName + "MaxIdleConns")
	dbMaxOpenConns := helper.Config().String("mysql::" + self.comName + "MaxOpenConns")
	if dbconfig == "" {
		driver = "mssql"
		dbconfig = helper.Config().String("mssql::" + self.comName)
		dbMaxIdleConns = helper.Config().String("mssql::" + self.comName + "MaxIdleConns")
		dbMaxOpenConns = helper.Config().String("mssql::" + self.comName + "MaxOpenConns")
	}

	if dbconfig == "" {
		helper.Exit("ComModel " + self.comName + ":No database configuration information exists")
	}
	_, err := gtorm.GetDB(self.comName)
	if err == nil {
		//已注册
		return
	}
	if dbMaxIdleConns == "" {
		dbMaxIdleConns = helper.Config().DefaultString("sys::SqlMaxIdleConns", "1")
	}
	if dbMaxOpenConns == "" {
		dbMaxOpenConns = helper.Config().DefaultString("sys::SqlMaxOpenConns", "2")
	}
	maxIdleConns, ei := strconv.Atoi(dbMaxIdleConns)
	maxOpenConns, eo := strconv.Atoi(dbMaxOpenConns)
	if ei != nil || eo != nil || maxIdleConns == 0 || maxOpenConns == 0 || maxIdleConns > maxOpenConns {
		helper.Exit("ComModel Failure to connect " + self.comName + " db, MaxIdleConns or MaxOpenConns are invalid args")
	}
	helper.Log().Notice("ComModel Connect com " + self.comName + " database, MaxIdleConns:" + dbMaxIdleConns + ", MaxOpenConns:" + dbMaxOpenConns + ", config:" + dbconfig)
	err = gtorm.RegisterDataBase(self.comName, driver, dbconfig, maxIdleConns, maxOpenConns)
	if err != nil {
		helper.Exit("ComModel-RegisterDataBase Connect " + self.comName + " error:," + err.Error())
	}
}

//FormatArray 格式化in参数
func (self *ComModel) FormatArray(args ...interface{}) (list []interface{}) {
	for _, item := range args {
		slice := reflect.ValueOf(item)
		if slice.Kind() != reflect.Slice {
			list = append(list, item)
			continue
		}

		//slice组合
		items, ok := takeSliceArg(item)
		if !ok {
			continue
		}
		for _, sliceItem := range items {
			list = append(list, sliceItem)
		}

	}
	return
}

//FormatPlaceholder 格式化in参数占位符
func (self *ComModel) FormatPlaceholder(arg interface{}) string {
	slice := reflect.ValueOf(arg)
	if slice.Kind() != reflect.Slice {
		return ""
	}

	result := []string{}
	c := slice.Len()
	for i := 0; i < c; i++ {
		result = append(result, "?")
	}
	return strings.Join(result, ",")
}

func (self *ComModel) Hash(data interface{}, mod int) int {
	return node.HashNodeSum(data, mod)
}

//profiler 分析
func (self *ComModel) profiler(ssql string, args ...interface{}) {
	dict := rpc.GoDict()
	if dict == nil || dict.Get("gseq") == nil {
		return
	}
	if strings.Contains(ssql, "EXPLAIN") {
		return
	}
	sourceSql := fmt.Sprintf(strings.Replace(ssql, "?", "%v", -1), args...)
	sql := strings.ToLower(sourceSql)
	if strings.Contains(sql, "delete") || strings.Contains(sql, "update") || strings.Contains(sql, "insert") || strings.Contains(sql, "count") || strings.Contains(sql, "sum") || strings.Contains(sql, "max") {
		helper.Log().Notice("sql profiler ", sourceSql)
		return
	}
	var explain []struct {
		Table string
		Type  string
	}
	explainLog := ""
	o := gtorm.New(self.comName)
	_, e := o.Raw("EXPLAIN "+ssql, args...).QueryRows(&explain)
	tables := []string{}
	warn := false
	if e == nil {
		for index := 0; index < len(explain); index++ {
			if index > 0 {
				explainLog += " "
			}
			etype := strings.ToLower(explain[index].Type)
			if etype == "all" || etype == "index" {
				warn = true
			}
			explainLog += " tableName " + explain[index].Table + " Sql Explain Type " + explain[index].Type
			tables = append(tables, explain[index].Table)
		}
	}
	if explainLog != "" {
		explainLog = "explain :(" + explainLog + ")"
	}
	if warn {
		helper.Log().Warning("sql profiler ", explainLog, "source :("+sourceSql+")")
	} else {
		helper.Log().Notice("sql profiler ", explainLog, "source :("+sourceSql+")")
	}

	gseq := dict.Get("gseq")
	str, ok := gseq.(string)
	if !ok {
		return
	}

	for _, item := range tables {
		table := item
		if profilerSync(str + "_" + table) {
			continue
		}
		go func() {
			time.Sleep(3 * time.Second)
			var tableCount int
			var ok bool
			modelProfilerSync.Lock()
			tableCount, ok = modelProfilerCount[str+"_"+table]
			delete(modelProfilerCount, str+"_"+table)
			modelProfilerSync.Unlock()
			if tableCount > 1 && ok {
				helper.Log().Warning(str + "reads the table'" + table + "' " + fmt.Sprint(tableCount) + " times in a bsep")
			}
		}()
	}
}

func profilerSync(key string) (exist bool) {
	defer modelProfilerSync.Unlock()
	modelProfilerSync.Lock()
	exist = false
	v, ok := modelProfilerCount[key]
	if ok {
		exist = true
		modelProfilerCount[key] = v + 1
		return
	}
	modelProfilerCount[key] = 1
	return
}

func (self *ComModel) Connections(m map[string]int) {
	if !self.open {
		return
	}
	db, err := gtorm.GetDB(self.comName)
	if err != nil {
		return
	}
	m[self.comName] = db.Stats().OpenConnections
}

func (self *ComModel) DaoInit() {
	if self.comName == "" {
		self.comName = self.TopChild().(comName).Com()
	}
}

func takeSliceArg(arg interface{}) (out []interface{}, ok bool) {

	slice := reflect.ValueOf(arg)
	if slice.Kind() != reflect.Slice {
		return nil, false
	}

	c := slice.Len()
	out = make([]interface{}, c)
	for i := 0; i < c; i++ {
		out[i] = slice.Index(i).Interface()
	}
	return out, true
}
