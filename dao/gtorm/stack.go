package gtorm

import (
	"errors"
	"fmt"
	"sync"

	"github.com/8treenet/gotree/helper"
)

var stackOrm map[string]Ormer
var mutex sync.Mutex

func init() {
	stackOrm = make(map[string]Ormer)
}

//New 获取orm
func New(db string) (resultOrm Ormer) {
	//查看栈中是否有 同goid 同db的orm
	resultOrm = getStackOrm(db)
	if resultOrm == nil {
		resultOrm = newOrm()
		e := resultOrm.Using(db)
		if e != nil {
			panic(e)
		}
	}
	return
}

//Transaction 事务执行回调函数
func Transaction(db string, fun func() error) (e error) {
	defer removeStackOrm(db) //移除栈中的orm
	//获取栈中的orm
	orm, err := createStackOrm(db)
	if err != nil {
		e = err
		return
	}

	defer func() {
		if perr := recover(); perr != nil {
			e = errors.New(fmt.Sprint(perr))
			return
		}
		if e != nil {
			orm.Rollback()
			return
		}

		e = orm.Commit()
	}()

	orm.Begin()
	e = fun()
	return
}

//removeStackOrm 移出栈中的orm对象
func removeStackOrm(db string) {
	defer mutex.Unlock()
	mutex.Lock()
	id := goId()
	delete(stackOrm, id+db)
}

//createStackOrm 创建栈中的缓存
func createStackOrm(db string) (Ormer, error) {
	defer mutex.Unlock()
	mutex.Lock()

	orm := newOrm()
	err := orm.Using(db)
	if err != nil {
		return nil, err
	}

	id := goId()
	stackOrm[id+db] = orm
	return orm, err
}

//getStackOrm 获取栈中的orm对象
func getStackOrm(db string) Ormer {
	defer mutex.Unlock()
	mutex.Lock()

	id := goId()
	o, check := stackOrm[id+db]
	if check {
		return o
	}
	return nil
}

func goId() string {
	// var buf [64]byte
	// n := runtime.Stack(buf[:], false)
	// idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	//return idField
	return fmt.Sprint(helper.RuntimePointer())
}
