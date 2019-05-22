package gtorm

import (
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Driver define database driver
type Driver interface {
	Name() string
	Type() DriverType
}

// Fielder define field info
type Fielder interface {
	String() string
	FieldType() int
	SetRaw(interface{}) error
	RawValue() interface{}
}

// Ormer define the orm interface
type Ormer interface {
	// switch to another registered database driver by given name.
	Using(name string) error
	Begin() error
	Commit() error
	Rollback() error
	Raw(query string, args ...interface{}) RawSeter
	RawCallBack(func(string, []interface{}))
	Driver() Driver
}

type QuerySeter interface {
}

// RawPreparer raw query statement
type RawPreparer interface {
	Exec(...interface{}) (sql.Result, error)
	Close() error
}

type RawSeter interface {
	Exec() (sql.Result, error)
	QueryRow(containers ...interface{}) error
	QueryRows(containers ...interface{}) (int64, error)
	Prepare() (RawPreparer, error)
}

// stmtQuerier statement querier
type stmtQuerier interface {
	Close() error
	Exec(args ...interface{}) (sql.Result, error)
	Query(args ...interface{}) (*sql.Rows, error)
	QueryRow(args ...interface{}) *sql.Row
}

// db querier
type dbQuerier interface {
	Prepare(query string) (*sql.Stmt, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// transaction beginner
type txer interface {
	Begin() (*sql.Tx, error)
}

// transaction ending
type txEnder interface {
	Commit() error
	Rollback() error
}

// base database struct
type dbBaser interface {
	OperatorSQL(string) string
	TableQuote() string
	ReplaceMarks(*string)
	HasReturningID(*modelInfo, *string) bool
	TimeFromDB(*time.Time, *time.Location)
	TimeToDB(*time.Time, *time.Location)
	ShowTablesQuery() string
	ShowColumnsQuery(string) string
	setval(dbQuerier, *modelInfo, []string) error
}

const (
	DebugQueries = iota
)

var (
	DefaultRowsLimit = 1000
	DefaultRelsDepth = 2
	DefaultTimeLoc   = time.Local
	ErrTxHasBegan    = errors.New("<Ormer.Begin> transaction already begin")
	ErrTxDone        = errors.New("<Ormer.Commit/Rollback> transaction not begin")
	ErrMultiRows     = errors.New("<QuerySeter> return multi rows")
	ErrNoRows        = errors.New("<QuerySeter> no row found")
	ErrStmtClosed    = errors.New("<QuerySeter> stmt already closed")
	ErrArgs          = errors.New("<Ormer> args error may be empty")
	ErrNotImplement  = errors.New("have not implement")
)

// Params stores the Params
type Params map[string]interface{}

// ParamsList stores paramslist
type ParamsList []interface{}

type orm struct {
	alias       *alias
	db          dbQuerier
	isTx        bool
	rawCallBack func(string, []interface{})
}

var _ Ormer = new(orm)

// RawCallBack
func (o *orm) RawCallBack(rawCallBack func(string, []interface{})) {
	o.rawCallBack = rawCallBack
}

// get model info and model reflect value
func (o *orm) getMiInd(md interface{}, needPtr bool) (mi *modelInfo, ind reflect.Value) {
	val := reflect.ValueOf(md)
	ind = reflect.Indirect(val)
	typ := ind.Type()
	if needPtr && val.Kind() != reflect.Ptr {
		panic(fmt.Errorf("<Ormer> cannot use non-ptr model struct `%s`", getFullName(typ)))
	}
	name := getFullName(typ)
	if mi, ok := modelCache.getByFullName(name); ok {
		return mi, ind
	}
	panic(fmt.Errorf("<Ormer> table: `%s` not found, make sure it was registered with `RegisterModel()`", name))
}

// get field info from model info by given field name
func (o *orm) getFieldInfo(mi *modelInfo, name string) *fieldInfo {
	fi, ok := mi.fields.GetByAny(name)
	if !ok {
		panic(fmt.Errorf("<Ormer> cannot find field `%s` for model `%s`", name, mi.fullName))
	}
	return fi
}

// set auto pk field
func (o *orm) setPk(mi *modelInfo, ind reflect.Value, id int64) {
	if mi.fields.pk.auto {
		if mi.fields.pk.fieldType&IsPositiveIntegerField > 0 {
			ind.FieldByIndex(mi.fields.pk.fieldIndex).SetUint(uint64(id))
		} else {
			ind.FieldByIndex(mi.fields.pk.fieldIndex).SetInt(id)
		}
	}
}

// get QuerySeter for related models to md model
func (o *orm) queryRelated(md interface{}, name string) (*modelInfo, *fieldInfo, reflect.Value, QuerySeter) {
	mi, ind := o.getMiInd(md, true)
	fi := o.getFieldInfo(mi, name)

	_, _, exist := getExistPk(mi, ind)
	if !exist {
		panic(ErrMissPK)
	}

	var qs *querySet

	switch fi.fieldType {
	case RelOneToOne, RelForeignKey, RelManyToMany:
		if !fi.inModel {
			break
		}
		qs = o.getRelQs(md, mi, fi)
	case RelReverseOne, RelReverseMany:
		if !fi.inModel {
			break
		}
		qs = o.getReverseQs(md, mi, fi)
	}

	if qs == nil {
		panic(fmt.Errorf("<Ormer> name `%s` for model `%s` is not an available rel/reverse field", md, name))
	}

	return mi, fi, ind, qs
}

// get reverse relation QuerySeter
func (o *orm) getReverseQs(md interface{}, mi *modelInfo, fi *fieldInfo) *querySet {
	switch fi.fieldType {
	case RelReverseOne, RelReverseMany:
	default:
		panic(fmt.Errorf("<Ormer> name `%s` for model `%s` is not an available reverse field", fi.name, mi.fullName))
	}

	var q *querySet

	if fi.fieldType == RelReverseMany && fi.reverseFieldInfo.mi.isThrough {
		q = newQuerySet(o, fi.relModelInfo).(*querySet)
		//q.cond = NewCondition().And(fi.reverseFieldInfoM2M.column+ExprSep+fi.reverseFieldInfo.column, md)
	} else {
		q = newQuerySet(o, fi.reverseFieldInfo.mi).(*querySet)
		//q.cond = NewCondition().And(fi.reverseFieldInfo.column, md)
	}

	return q
}

// get relation QuerySeter
func (o *orm) getRelQs(md interface{}, mi *modelInfo, fi *fieldInfo) *querySet {
	switch fi.fieldType {
	case RelOneToOne, RelForeignKey, RelManyToMany:
	default:
		panic(fmt.Errorf("<Ormer> name `%s` for model `%s` is not an available rel field", fi.name, mi.fullName))
	}

	q := newQuerySet(o, fi.relModelInfo).(*querySet)
	//q.cond = NewCondition()

	if fi.fieldType == RelManyToMany {
		//q.cond = q.cond.And(fi.reverseFieldInfoM2M.column+ExprSep+fi.reverseFieldInfo.column, md)
	} else {
		//q.cond = q.cond.And(fi.reverseFieldInfo.column, md)
	}

	return q
}

// switch to another registered database driver by given name.
func (o *orm) Using(name string) error {
	o.rawCallBack = nil
	if o.isTx {
		panic(fmt.Errorf("<Ormer.Using> transaction has been start, cannot change db"))
	}
	if al, ok := dataBaseCache.get(name); ok {
		o.alias = al
		o.db = al.DB
	} else {
		return fmt.Errorf("<Ormer.Using> unknown db alias name `%s`", name)
	}
	return nil
}

// begin transaction
func (o *orm) Begin() error {
	if o.isTx {
		return ErrTxHasBegan
	}
	var tx *sql.Tx
	tx, err := o.db.(txer).Begin()
	if err != nil {
		return err
	}
	o.isTx = true
	o.db = tx
	return nil
}

// commit transaction
func (o *orm) Commit() error {
	if !o.isTx {
		return ErrTxDone
	}
	err := o.db.(txEnder).Commit()
	if err == nil {
		o.isTx = false
		o.Using(o.alias.Name)
	} else if err == sql.ErrTxDone {
		return ErrTxDone
	}
	return err
}

// rollback transaction
func (o *orm) Rollback() error {
	if !o.isTx {
		return ErrTxDone
	}
	err := o.db.(txEnder).Rollback()
	if err == nil {
		o.isTx = false
		o.Using(o.alias.Name)
	} else if err == sql.ErrTxDone {
		return ErrTxDone
	}
	return err
}

// return a raw query seter for raw sql string.
func (o *orm) Raw(query string, args ...interface{}) RawSeter {
	if o.rawCallBack != nil {
		o.rawCallBack(query, args)
	}
	return newRawSet(o, query, args)
}

// return current using database Driver
func (o *orm) Driver() Driver {
	return driver(o.alias.Name)
}

// NewOrm create new orm
func newOrm() Ormer {
	BootStrap() // execute only once
	return new(orm)
}

// StrTo is the target string
type StrTo string

// Set string
func (f *StrTo) Set(v string) {
	if v != "" {
		*f = StrTo(v)
	} else {
		f.Clear()
	}
}

// Clear string
func (f *StrTo) Clear() {
	*f = StrTo(0x1E)
}

// Exist check string exist
func (f StrTo) Exist() bool {
	return string(f) != string(0x1E)
}

// Bool string to bool
func (f StrTo) Bool() (bool, error) {
	return strconv.ParseBool(f.String())
}

// Float32 string to float32
func (f StrTo) Float32() (float32, error) {
	v, err := strconv.ParseFloat(f.String(), 32)
	return float32(v), err
}

// Float64 string to float64
func (f StrTo) Float64() (float64, error) {
	return strconv.ParseFloat(f.String(), 64)
}

// Int string to int
func (f StrTo) Int() (int, error) {
	v, err := strconv.ParseInt(f.String(), 10, 32)
	return int(v), err
}

// Int8 string to int8
func (f StrTo) Int8() (int8, error) {
	v, err := strconv.ParseInt(f.String(), 10, 8)
	return int8(v), err
}

// Int16 string to int16
func (f StrTo) Int16() (int16, error) {
	v, err := strconv.ParseInt(f.String(), 10, 16)
	return int16(v), err
}

// Int32 string to int32
func (f StrTo) Int32() (int32, error) {
	v, err := strconv.ParseInt(f.String(), 10, 32)
	return int32(v), err
}

// Int64 string to int64
func (f StrTo) Int64() (int64, error) {
	v, err := strconv.ParseInt(f.String(), 10, 64)
	if err != nil {
		i := new(big.Int)
		ni, ok := i.SetString(f.String(), 10) // octal
		if !ok {
			return v, err
		}
		return ni.Int64(), nil
	}
	return v, err
}

// Uint string to uint
func (f StrTo) Uint() (uint, error) {
	v, err := strconv.ParseUint(f.String(), 10, 32)
	return uint(v), err
}

// Uint8 string to uint8
func (f StrTo) Uint8() (uint8, error) {
	v, err := strconv.ParseUint(f.String(), 10, 8)
	return uint8(v), err
}

// Uint16 string to uint16
func (f StrTo) Uint16() (uint16, error) {
	v, err := strconv.ParseUint(f.String(), 10, 16)
	return uint16(v), err
}

// Uint32 string to uint31
func (f StrTo) Uint32() (uint32, error) {
	v, err := strconv.ParseUint(f.String(), 10, 32)
	return uint32(v), err
}

// Uint64 string to uint64
func (f StrTo) Uint64() (uint64, error) {
	v, err := strconv.ParseUint(f.String(), 10, 64)
	if err != nil {
		i := new(big.Int)
		ni, ok := i.SetString(f.String(), 10)
		if !ok {
			return v, err
		}
		return ni.Uint64(), nil
	}
	return v, err
}

// String string to string
func (f StrTo) String() string {
	if f.Exist() {
		return string(f)
	}
	return ""
}

// ToStr interface to string
func ToStr(value interface{}, args ...int) (s string) {
	switch v := value.(type) {
	case bool:
		s = strconv.FormatBool(v)
	case float32:
		s = strconv.FormatFloat(float64(v), 'f', argInt(args).Get(0, -1), argInt(args).Get(1, 32))
	case float64:
		s = strconv.FormatFloat(v, 'f', argInt(args).Get(0, -1), argInt(args).Get(1, 64))
	case int:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int8:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int16:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int32:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int64:
		s = strconv.FormatInt(v, argInt(args).Get(0, 10))
	case uint:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint8:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint16:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint32:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint64:
		s = strconv.FormatUint(v, argInt(args).Get(0, 10))
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		s = fmt.Sprintf("%v", v)
	}
	return s
}

// ToInt64 interface to int64
func ToInt64(value interface{}) (d int64) {
	val := reflect.ValueOf(value)
	switch value.(type) {
	case int, int8, int16, int32, int64:
		d = val.Int()
	case uint, uint8, uint16, uint32, uint64:
		d = int64(val.Uint())
	default:
		panic(fmt.Errorf("ToInt64 need numeric not `%T`", value))
	}
	return
}

// snake string, XxYy to xx_yy , XxYY to xx_yy
func snakeString(s string) string {
	data := make([]byte, 0, len(s)*2)
	j := false
	num := len(s)
	for i := 0; i < num; i++ {
		d := s[i]
		if i > 0 && d >= 'A' && d <= 'Z' && j {
			data = append(data, '_')
		}
		if d != '_' {
			j = true
		}
		data = append(data, d)
	}
	return strings.ToLower(string(data[:]))
}

// camel string, xx_yy to XxYy
func camelString(s string) string {
	data := make([]byte, 0, len(s))
	flag, num := true, len(s)-1
	for i := 0; i <= num; i++ {
		d := s[i]
		if d == '_' {
			flag = true
			continue
		} else if flag {
			if d >= 'a' && d <= 'z' {
				d = d - 32
			}
			flag = false
		}
		data = append(data, d)
	}
	return string(data[:])
}

type argString []string

// get string by index from string slice
func (a argString) Get(i int, args ...string) (r string) {
	if i >= 0 && i < len(a) {
		r = a[i]
	} else if len(args) > 0 {
		r = args[0]
	}
	return
}

type argInt []int

// get int by index from int slice
func (a argInt) Get(i int, args ...int) (r int) {
	if i >= 0 && i < len(a) {
		r = a[i]
	}
	if len(args) > 0 {
		r = args[0]
	}
	return
}

// parse time to string with location
func timeParse(dateString, format string) (time.Time, error) {
	tp, err := time.ParseInLocation(format, dateString, DefaultTimeLoc)
	return tp, err
}

// get pointer indirect type
func indirectType(v reflect.Type) reflect.Type {
	switch v.Kind() {
	case reflect.Ptr:
		return indirectType(v.Elem())
	default:
		return v
	}
}

// raw sql string prepared statement
type rawPrepare struct {
	rs     *rawSet
	stmt   stmtQuerier
	closed bool
}

func (o *rawPrepare) Exec(args ...interface{}) (sql.Result, error) {
	if o.closed {
		return nil, ErrStmtClosed
	}
	return o.stmt.Exec(args...)
}

func (o *rawPrepare) Close() error {
	o.closed = true
	return o.stmt.Close()
}

func newRawPreparer(rs *rawSet) (RawPreparer, error) {
	o := new(rawPrepare)
	o.rs = rs

	query := rs.query
	rs.orm.alias.DbBaser.ReplaceMarks(&query)

	st, err := rs.orm.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	o.stmt = st
	return o, nil
}

// raw query seter
type rawSet struct {
	query string
	args  []interface{}
	orm   *orm
}

var _ RawSeter = new(rawSet)

// set args for every query
func (o rawSet) SetArgs(args ...interface{}) RawSeter {
	o.args = args
	return &o
}

// execute raw sql and return sql.Result
func (o *rawSet) Exec() (sql.Result, error) {
	query := o.query
	o.orm.alias.DbBaser.ReplaceMarks(&query)

	args := getFlatParams(nil, o.args, o.orm.alias.TZ)
	return o.orm.db.Exec(query, args...)
}

// set field value to row container
func (o *rawSet) setFieldValue(ind reflect.Value, value interface{}) {
	switch ind.Kind() {
	case reflect.Bool:
		if value == nil {
			ind.SetBool(false)
		} else if v, ok := value.(bool); ok {
			ind.SetBool(v)
		} else {
			v, _ := StrTo(ToStr(value)).Bool()
			ind.SetBool(v)
		}

	case reflect.String:
		if value == nil {
			ind.SetString("")
		} else {
			ind.SetString(ToStr(value))
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if value == nil {
			ind.SetInt(0)
		} else {
			val := reflect.ValueOf(value)
			switch val.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				ind.SetInt(val.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				ind.SetInt(int64(val.Uint()))
			default:
				v, _ := StrTo(ToStr(value)).Int64()
				ind.SetInt(v)
			}
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if value == nil {
			ind.SetUint(0)
		} else {
			val := reflect.ValueOf(value)
			switch val.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				ind.SetUint(uint64(val.Int()))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				ind.SetUint(val.Uint())
			default:
				v, _ := StrTo(ToStr(value)).Uint64()
				ind.SetUint(v)
			}
		}
	case reflect.Float64, reflect.Float32:
		if value == nil {
			ind.SetFloat(0)
		} else {
			val := reflect.ValueOf(value)
			switch val.Kind() {
			case reflect.Float64:
				ind.SetFloat(val.Float())
			default:
				v, _ := StrTo(ToStr(value)).Float64()
				ind.SetFloat(v)
			}
		}

	case reflect.Struct:
		if value == nil {
			ind.Set(reflect.Zero(ind.Type()))

		} else if _, ok := ind.Interface().(time.Time); ok {
			var str string
			switch d := value.(type) {
			case time.Time:
				o.orm.alias.DbBaser.TimeFromDB(&d, o.orm.alias.TZ)
				ind.Set(reflect.ValueOf(d))
			case []byte:
				str = string(d)
			case string:
				str = d
			}
			if str != "" {
				if len(str) >= 19 {
					str = str[:19]
					t, err := time.ParseInLocation(formatDateTime, str, o.orm.alias.TZ)
					if err == nil {
						t = t.In(DefaultTimeLoc)
						ind.Set(reflect.ValueOf(t))
					}
				} else if len(str) >= 10 {
					str = str[:10]
					t, err := time.ParseInLocation(formatDate, str, DefaultTimeLoc)
					if err == nil {
						ind.Set(reflect.ValueOf(t))
					}
				}
			}
		}
	}
}

// set field value in loop for slice container
func (o *rawSet) loopSetRefs(refs []interface{}, sInds []reflect.Value, nIndsPtr *[]reflect.Value, eTyps []reflect.Type, init bool) {
	nInds := *nIndsPtr

	cur := 0
	for i := 0; i < len(sInds); i++ {
		sInd := sInds[i]
		eTyp := eTyps[i]

		typ := eTyp
		isPtr := false
		if typ.Kind() == reflect.Ptr {
			isPtr = true
			typ = typ.Elem()
		}
		if typ.Kind() == reflect.Ptr {
			isPtr = true
			typ = typ.Elem()
		}

		var nInd reflect.Value
		if init {
			nInd = reflect.New(sInd.Type()).Elem()
		} else {
			nInd = nInds[i]
		}

		val := reflect.New(typ)
		ind := val.Elem()

		tpName := ind.Type().String()

		if ind.Kind() == reflect.Struct {
			if tpName == "time.Time" {
				value := reflect.ValueOf(refs[cur]).Elem().Interface()
				if isPtr && value == nil {
					val = reflect.New(val.Type()).Elem()
				} else {
					o.setFieldValue(ind, value)
				}
				cur++
			}

		} else {
			value := reflect.ValueOf(refs[cur]).Elem().Interface()
			if isPtr && value == nil {
				val = reflect.New(val.Type()).Elem()
			} else {
				o.setFieldValue(ind, value)
			}
			cur++
		}

		if nInd.Kind() == reflect.Slice {
			if isPtr {
				nInd = reflect.Append(nInd, val)
			} else {
				nInd = reflect.Append(nInd, ind)
			}
		} else {
			if isPtr {
				nInd.Set(val)
			} else {
				nInd.Set(ind)
			}
		}

		nInds[i] = nInd
	}
}

// query data and map to container
func (o *rawSet) QueryRow(containers ...interface{}) error {
	var (
		refs  = make([]interface{}, 0, len(containers))
		sInds []reflect.Value
		eTyps []reflect.Type
		sMi   *modelInfo
	)
	structMode := false
	for _, container := range containers {
		val := reflect.ValueOf(container)
		ind := reflect.Indirect(val)

		if val.Kind() != reflect.Ptr {
			panic(fmt.Errorf("<RawSeter.QueryRow> all args must be use ptr"))
		}

		etyp := ind.Type()
		typ := etyp
		if typ.Kind() == reflect.Ptr {
			typ = typ.Elem()
		}

		sInds = append(sInds, ind)
		eTyps = append(eTyps, etyp)

		if typ.Kind() == reflect.Struct && typ.String() != "time.Time" {
			if len(containers) > 1 {
				panic(fmt.Errorf("<RawSeter.QueryRow> now support one struct only. see #384"))
			}

			structMode = true
			fn := getFullName(typ)
			if mi, ok := modelCache.getByFullName(fn); ok {
				sMi = mi
			}
		} else {
			var ref interface{}
			refs = append(refs, &ref)
		}
	}

	query := o.query
	o.orm.alias.DbBaser.ReplaceMarks(&query)

	args := getFlatParams(nil, o.args, o.orm.alias.TZ)
	rows, err := o.orm.db.Query(query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return ErrNoRows
		}
		return err
	}

	defer rows.Close()

	if rows.Next() {
		if structMode {
			columns, err := rows.Columns()
			if err != nil {
				return err
			}

			columnsMp := make(map[string]interface{}, len(columns))

			refs = make([]interface{}, 0, len(columns))
			for _, col := range columns {
				ncol := strings.ToLower(col)
				ncol = strings.Replace(ncol, "_", "", -1)
				var ref interface{}
				columnsMp[ncol] = &ref
				refs = append(refs, &ref)
			}

			if err := rows.Scan(refs...); err != nil {
				return err
			}

			ind := sInds[0]

			if ind.Kind() == reflect.Ptr {
				if ind.IsNil() || !ind.IsValid() {
					ind.Set(reflect.New(eTyps[0].Elem()))
				}
				ind = ind.Elem()
			}

			if sMi != nil {
				for _, col := range columns {
					if fi := sMi.fields.GetByColumn(col); fi != nil {
						value := reflect.ValueOf(columnsMp[col]).Elem().Interface()
						field := ind.FieldByIndex(fi.fieldIndex)
						if fi.fieldType&IsRelField > 0 {
							mf := reflect.New(fi.relModelInfo.addrField.Elem().Type())
							field.Set(mf)
							field = mf.Elem().FieldByIndex(fi.relModelInfo.fields.pk.fieldIndex)
						}
						o.setFieldValue(field, value)
					}
				}
			} else {
				for i := 0; i < ind.NumField(); i++ {
					f := ind.Field(i)
					fe := ind.Type().Field(i)
					_, tags := parseStructTag(fe.Tag.Get(defaultStructTagName))
					var col string
					if col = tags["column"]; col == "" {
						//col = snakeString(fe.Name)
						col = fe.Name
					}

					col = strings.ToLower(col)
					if v, ok := columnsMp[col]; ok {
						value := reflect.ValueOf(v).Elem().Interface()
						o.setFieldValue(f, value)
					}
				}
			}

		} else {
			if err := rows.Scan(refs...); err != nil {
				return err
			}

			nInds := make([]reflect.Value, len(sInds))
			o.loopSetRefs(refs, sInds, &nInds, eTyps, true)
			for i, sInd := range sInds {
				nInd := nInds[i]
				sInd.Set(nInd)
			}
		}

	} else {
		return ErrNoRows
	}

	return nil
}

// query data rows and map to container
func (o *rawSet) QueryRows(containers ...interface{}) (int64, error) {
	var (
		refs  = make([]interface{}, 0, len(containers))
		sInds []reflect.Value
		eTyps []reflect.Type
		sMi   *modelInfo
	)
	structMode := false
	for _, container := range containers {
		val := reflect.ValueOf(container)
		sInd := reflect.Indirect(val)
		if val.Kind() != reflect.Ptr || sInd.Kind() != reflect.Slice {
			panic(fmt.Errorf("<RawSeter.QueryRows> all args must be use ptr slice"))
		}

		etyp := sInd.Type().Elem()
		typ := etyp
		if typ.Kind() == reflect.Ptr {
			typ = typ.Elem()
		}

		sInds = append(sInds, sInd)
		eTyps = append(eTyps, etyp)

		if typ.Kind() == reflect.Struct && typ.String() != "time.Time" {
			if len(containers) > 1 {
				panic(fmt.Errorf("<RawSeter.QueryRow> now support one struct only. see #384"))
			}

			structMode = true
			fn := getFullName(typ)
			if mi, ok := modelCache.getByFullName(fn); ok {
				sMi = mi
			}
		} else {
			var ref interface{}
			refs = append(refs, &ref)
		}
	}

	query := o.query
	o.orm.alias.DbBaser.ReplaceMarks(&query)

	args := getFlatParams(nil, o.args, o.orm.alias.TZ)
	rows, err := o.orm.db.Query(query, args...)
	if err != nil {
		return 0, err
	}

	defer rows.Close()

	var cnt int64
	nInds := make([]reflect.Value, len(sInds))
	sInd := sInds[0]

	for rows.Next() {

		if structMode {
			columns, err := rows.Columns()
			if err != nil {
				return 0, err
			}

			columnsMp := make(map[string]interface{}, len(columns))

			refs = make([]interface{}, 0, len(columns))
			for _, col := range columns {
				ncol := strings.ToLower(col)
				ncol = strings.Replace(ncol, "_", "", -1)
				var ref interface{}
				columnsMp[ncol] = &ref
				refs = append(refs, &ref)
			}

			if err := rows.Scan(refs...); err != nil {
				return 0, err
			}

			if cnt == 0 && !sInd.IsNil() {
				sInd.Set(reflect.New(sInd.Type()).Elem())
			}

			var ind reflect.Value
			if eTyps[0].Kind() == reflect.Ptr {
				ind = reflect.New(eTyps[0].Elem())
			} else {
				ind = reflect.New(eTyps[0])
			}

			if ind.Kind() == reflect.Ptr {
				ind = ind.Elem()
			}

			if sMi != nil {
				for _, col := range columns {
					if fi := sMi.fields.GetByColumn(col); fi != nil {
						value := reflect.ValueOf(columnsMp[col]).Elem().Interface()
						field := ind.FieldByIndex(fi.fieldIndex)
						if fi.fieldType&IsRelField > 0 {
							mf := reflect.New(fi.relModelInfo.addrField.Elem().Type())
							field.Set(mf)
							field = mf.Elem().FieldByIndex(fi.relModelInfo.fields.pk.fieldIndex)
						}
						o.setFieldValue(field, value)
					}
				}
			} else {
				// define recursive function
				var recursiveSetField func(rv reflect.Value)
				recursiveSetField = func(rv reflect.Value) {
					for i := 0; i < rv.NumField(); i++ {
						f := rv.Field(i)
						fe := rv.Type().Field(i)

						// check if the field is a Struct
						// recursive the Struct type
						if fe.Type.Kind() == reflect.Struct {
							recursiveSetField(f)
						}

						_, tags := parseStructTag(fe.Tag.Get(defaultStructTagName))
						var col string
						if col = tags["column"]; col == "" {
							//col = snakeString(fe.Name)
							col = fe.Name
						}
						col = strings.ToLower(col)
						if v, ok := columnsMp[col]; ok {
							value := reflect.ValueOf(v).Elem().Interface()
							o.setFieldValue(f, value)
						}
					}
				}

				// init call the recursive function
				recursiveSetField(ind)
			}

			if eTyps[0].Kind() == reflect.Ptr {
				ind = ind.Addr()
			}

			sInd = reflect.Append(sInd, ind)

		} else {
			if err := rows.Scan(refs...); err != nil {
				return 0, err
			}

			o.loopSetRefs(refs, sInds, &nInds, eTyps, cnt == 0)
		}

		cnt++
	}

	if cnt > 0 {

		if structMode {
			sInds[0].Set(sInd)
		} else {
			for i, sInd := range sInds {
				nInd := nInds[i]
				sInd.Set(nInd)
			}
		}
	}

	return cnt, nil
}

func (o *rawSet) readValues(container interface{}, needCols []string) (int64, error) {
	var (
		maps  []Params
		lists []ParamsList
		list  ParamsList
	)

	typ := 0
	switch container.(type) {
	case *[]Params:
		typ = 1
	case *[]ParamsList:
		typ = 2
	case *ParamsList:
		typ = 3
	default:
		panic(fmt.Errorf("<RawSeter> unsupport read values type `%T`", container))
	}

	query := o.query
	o.orm.alias.DbBaser.ReplaceMarks(&query)

	args := getFlatParams(nil, o.args, o.orm.alias.TZ)

	var rs *sql.Rows
	rs, err := o.orm.db.Query(query, args...)
	if err != nil {
		return 0, err
	}

	defer rs.Close()

	var (
		refs   []interface{}
		cnt    int64
		cols   []string
		indexs []int
	)

	for rs.Next() {
		if cnt == 0 {
			columns, err := rs.Columns()
			if err != nil {
				return 0, err
			}
			if len(needCols) > 0 {
				indexs = make([]int, 0, len(needCols))
			} else {
				indexs = make([]int, 0, len(columns))
			}

			cols = columns
			refs = make([]interface{}, len(cols))
			for i := range refs {
				var ref sql.NullString
				refs[i] = &ref

				if len(needCols) > 0 {
					for _, c := range needCols {
						if c == cols[i] {
							indexs = append(indexs, i)
						}
					}
				} else {
					indexs = append(indexs, i)
				}
			}
		}

		if err := rs.Scan(refs...); err != nil {
			return 0, err
		}

		switch typ {
		case 1:
			params := make(Params, len(cols))
			for _, i := range indexs {
				ref := refs[i]
				value := reflect.Indirect(reflect.ValueOf(ref)).Interface().(sql.NullString)
				if value.Valid {
					params[cols[i]] = value.String
				} else {
					params[cols[i]] = nil
				}
			}
			maps = append(maps, params)
		case 2:
			params := make(ParamsList, 0, len(cols))
			for _, i := range indexs {
				ref := refs[i]
				value := reflect.Indirect(reflect.ValueOf(ref)).Interface().(sql.NullString)
				if value.Valid {
					params = append(params, value.String)
				} else {
					params = append(params, nil)
				}
			}
			lists = append(lists, params)
		case 3:
			for _, i := range indexs {
				ref := refs[i]
				value := reflect.Indirect(reflect.ValueOf(ref)).Interface().(sql.NullString)
				if value.Valid {
					list = append(list, value.String)
				} else {
					list = append(list, nil)
				}
			}
		}

		cnt++
	}

	switch v := container.(type) {
	case *[]Params:
		*v = maps
	case *[]ParamsList:
		*v = lists
	case *ParamsList:
		*v = list
	}

	return cnt, nil
}

func (o *rawSet) queryRowsTo(container interface{}, keyCol, valueCol string) (int64, error) {
	var (
		maps Params
		ind  *reflect.Value
	)

	var typ int
	switch container.(type) {
	case *Params:
		typ = 1
	default:
		typ = 2
		vl := reflect.ValueOf(container)
		id := reflect.Indirect(vl)
		if vl.Kind() != reflect.Ptr || id.Kind() != reflect.Struct {
			panic(fmt.Errorf("<RawSeter> RowsTo unsupport type `%T` need ptr struct", container))
		}

		ind = &id
	}

	query := o.query
	o.orm.alias.DbBaser.ReplaceMarks(&query)

	args := getFlatParams(nil, o.args, o.orm.alias.TZ)

	rs, err := o.orm.db.Query(query, args...)
	if err != nil {
		return 0, err
	}

	defer rs.Close()

	var (
		refs []interface{}
		cnt  int64
		cols []string
	)

	var (
		keyIndex   = -1
		valueIndex = -1
	)

	for rs.Next() {
		if cnt == 0 {
			columns, err := rs.Columns()
			if err != nil {
				return 0, err
			}
			cols = columns
			refs = make([]interface{}, len(cols))
			for i := range refs {
				if keyCol == cols[i] {
					keyIndex = i
				}
				if typ == 1 || keyIndex == i {
					var ref sql.NullString
					refs[i] = &ref
				} else {
					var ref interface{}
					refs[i] = &ref
				}
				if valueCol == cols[i] {
					valueIndex = i
				}
			}
			if keyIndex == -1 || valueIndex == -1 {
				panic(fmt.Errorf("<RawSeter> RowsTo unknown key, value column name `%s: %s`", keyCol, valueCol))
			}
		}

		if err := rs.Scan(refs...); err != nil {
			return 0, err
		}

		if cnt == 0 {
			switch typ {
			case 1:
				maps = make(Params)
			}
		}

		key := reflect.Indirect(reflect.ValueOf(refs[keyIndex])).Interface().(sql.NullString).String

		switch typ {
		case 1:
			value := reflect.Indirect(reflect.ValueOf(refs[valueIndex])).Interface().(sql.NullString)
			if value.Valid {
				maps[key] = value.String
			} else {
				maps[key] = nil
			}

		default:
			if id := ind.FieldByName(camelString(key)); id.IsValid() {
				o.setFieldValue(id, reflect.ValueOf(refs[valueIndex]).Elem().Interface())
			}
		}

		cnt++
	}

	if typ == 1 {
		v, _ := container.(*Params)
		*v = maps
	}

	return cnt, nil
}

// query data to []map[string]interface
func (o *rawSet) Values(container *[]Params, cols ...string) (int64, error) {
	return o.readValues(container, cols)
}

// query data to [][]interface
func (o *rawSet) ValuesList(container *[]ParamsList, cols ...string) (int64, error) {
	return o.readValues(container, cols)
}

// query data to []interface
func (o *rawSet) ValuesFlat(container *ParamsList, cols ...string) (int64, error) {
	return o.readValues(container, cols)
}

// query all rows into map[string]interface with specify key and value column name.
// keyCol = "name", valueCol = "value"
// table data
// name  | value
// total | 100
// found | 200
// to map[string]interface{}{
// 	"total": 100,
// 	"found": 200,
// }
func (o *rawSet) RowsToMap(result *Params, keyCol, valueCol string) (int64, error) {
	return o.queryRowsTo(result, keyCol, valueCol)
}

// query all rows into struct with specify key and value column name.
// keyCol = "name", valueCol = "value"
// table data
// name  | value
// total | 100
// found | 200
// to struct {
// 	Total int
// 	Found int
// }
func (o *rawSet) RowsToStruct(ptrStruct interface{}, keyCol, valueCol string) (int64, error) {
	return o.queryRowsTo(ptrStruct, keyCol, valueCol)
}

// return prepared raw statement for used in times.
func (o *rawSet) Prepare() (RawPreparer, error) {
	return newRawPreparer(o)
}

func newRawSet(orm *orm, query string, args []interface{}) RawSeter {
	o := new(rawSet)
	o.query = query
	o.args = args
	o.orm = orm
	return o
}

type colValue struct {
	value int64
	opt   operator
}

type operator int

// define Col operations
const (
	ColAdd operator = iota
	ColMinus
	ColMultiply
	ColExcept
)

// ColValue do the field raw changes. e.g Nums = Nums + 10. usage:
// 	Params{
// 		"Nums": ColValue(Col_Add, 10),
// 	}
func ColValue(opt operator, value interface{}) interface{} {
	switch opt {
	case ColAdd, ColMinus, ColMultiply, ColExcept:
	default:
		panic(fmt.Errorf("orm.ColValue wrong operator"))
	}
	v, err := StrTo(ToStr(value)).Int64()
	if err != nil {
		panic(fmt.Errorf("orm.ColValue doesn't support non string/numeric type, %s", err))
	}
	var val colValue
	val.value = v
	val.opt = opt
	return val
}

// real query struct
type querySet struct {
	mi       *modelInfo
	related  []string
	relDepth int
	distinct bool
	orm      *orm
}

var _ QuerySeter = new(querySet)

// set relation model to query together.
// it will query relation models and assign to parent model.
func (o querySet) RelatedSel(params ...interface{}) QuerySeter {
	if len(params) == 0 {
		o.relDepth = DefaultRelsDepth
	} else {
		for _, p := range params {
			switch val := p.(type) {
			case string:
				o.related = append(o.related, val)
			case int:
				o.relDepth = val
			default:
				panic(fmt.Errorf("<QuerySeter.RelatedSel> wrong param kind: %v", val))
			}
		}
	}
	return &o
}

// create new QuerySeter.
func newQuerySet(orm *orm, mi *modelInfo) QuerySeter {
	o := new(querySet)
	o.mi = mi
	o.orm = orm
	return o
}
