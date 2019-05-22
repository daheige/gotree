package gtorm

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	odCascade             = "cascade"
	odSetNULL             = "set_null"
	odSetDefault          = "set_default"
	odDoNothing           = "do_nothing"
	defaultStructTagName  = "orm"
	defaultStructTagDelim = ";"
)

var (
	modelCache = &_modelCache{
		cache:           make(map[string]*modelInfo),
		cacheByFullName: make(map[string]*modelInfo),
	}
)

// model info collection
type _modelCache struct {
	sync.RWMutex    // only used outsite for bootStrap
	orders          []string
	cache           map[string]*modelInfo
	cacheByFullName map[string]*modelInfo
	done            bool
}

// get all model info
func (mc *_modelCache) all() map[string]*modelInfo {
	m := make(map[string]*modelInfo, len(mc.cache))
	for k, v := range mc.cache {
		m[k] = v
	}
	return m
}

// get orderd model info
func (mc *_modelCache) allOrdered() []*modelInfo {
	m := make([]*modelInfo, 0, len(mc.orders))
	for _, table := range mc.orders {
		m = append(m, mc.cache[table])
	}
	return m
}

// get model info by table name
func (mc *_modelCache) get(table string) (mi *modelInfo, ok bool) {
	mi, ok = mc.cache[table]
	return
}

// get model info by full name
func (mc *_modelCache) getByFullName(name string) (mi *modelInfo, ok bool) {
	mi, ok = mc.cacheByFullName[name]
	return
}

// set model info to collection
func (mc *_modelCache) set(table string, mi *modelInfo) *modelInfo {
	mii := mc.cache[table]
	mc.cache[table] = mi
	mc.cacheByFullName[mi.fullName] = mi
	if mii == nil {
		mc.orders = append(mc.orders, table)
	}
	return mii
}

// clean all model info.
func (mc *_modelCache) clean() {
	mc.orders = make([]string, 0)
	mc.cache = make(map[string]*modelInfo)
	mc.cacheByFullName = make(map[string]*modelInfo)
	mc.done = false
}

// ResetModelCache Clean model cache. Then you can re-RegisterModel.
// Common use this api for test case.
func ResetModelCache() {
	modelCache.clean()
}

var supportTag = map[string]int{
	"-":            1,
	"null":         1,
	"index":        1,
	"unique":       1,
	"pk":           1,
	"auto":         1,
	"auto_now":     1,
	"auto_now_add": 1,
	"size":         2,
	"column":       2,
	"default":      2,
	"rel":          2,
	"reverse":      2,
	"rel_table":    2,
	"rel_through":  2,
	"digits":       2,
	"decimals":     2,
	"on_delete":    2,
	"type":         2,
}

// get reflect.Type name with package path.
func getFullName(typ reflect.Type) string {
	return typ.PkgPath() + "." + typ.Name()
}

// getTableName get struct table name.
// If the struct implement the TableName, then get the result as tablename
// else use the struct name which will apply snakeString.
func getTableName(val reflect.Value) string {
	if fun := val.MethodByName("TableName"); fun.IsValid() {
		vals := fun.Call([]reflect.Value{})
		// has return and the first val is string
		if len(vals) > 0 && vals[0].Kind() == reflect.String {
			return vals[0].String()
		}
	}
	return snakeString(reflect.Indirect(val).Type().Name())
}

// get table engine, mysiam or innodb.
func getTableEngine(val reflect.Value) string {
	fun := val.MethodByName("TableEngine")
	if fun.IsValid() {
		vals := fun.Call([]reflect.Value{})
		if len(vals) > 0 && vals[0].Kind() == reflect.String {
			return vals[0].String()
		}
	}
	return ""
}

// get table index from method.
func getTableIndex(val reflect.Value) [][]string {
	fun := val.MethodByName("TableIndex")
	if fun.IsValid() {
		vals := fun.Call([]reflect.Value{})
		if len(vals) > 0 && vals[0].CanInterface() {
			if d, ok := vals[0].Interface().([][]string); ok {
				return d
			}
		}
	}
	return nil
}

// get table unique from method
func getTableUnique(val reflect.Value) [][]string {
	fun := val.MethodByName("TableUnique")
	if fun.IsValid() {
		vals := fun.Call([]reflect.Value{})
		if len(vals) > 0 && vals[0].CanInterface() {
			if d, ok := vals[0].Interface().([][]string); ok {
				return d
			}
		}
	}
	return nil
}

// get snaked column name
func getColumnName(ft int, addrField reflect.Value, sf reflect.StructField, col string) string {
	column := col
	if col == "" {
		//column = snakeString(sf.Name)
		column = sf.Name
	}
	switch ft {
	case RelForeignKey, RelOneToOne:
		if len(col) == 0 {
			column = column + "_id"
		}
	case RelManyToMany, RelReverseMany, RelReverseOne:
		column = sf.Name
	}
	return column
}

// return field type as type constant from reflect.Value
func getFieldType(val reflect.Value) (ft int, err error) {
	switch val.Type() {
	case reflect.TypeOf(new(int8)):
		ft = TypeBitField
	case reflect.TypeOf(new(int16)):
		ft = TypeSmallIntegerField
	case reflect.TypeOf(new(int32)),
		reflect.TypeOf(new(int)):
		ft = TypeIntegerField
	case reflect.TypeOf(new(int64)):
		ft = TypeBigIntegerField
	case reflect.TypeOf(new(uint8)):
		ft = TypePositiveBitField
	case reflect.TypeOf(new(uint16)):
		ft = TypePositiveSmallIntegerField
	case reflect.TypeOf(new(uint32)),
		reflect.TypeOf(new(uint)):
		ft = TypePositiveIntegerField
	case reflect.TypeOf(new(uint64)):
		ft = TypePositiveBigIntegerField
	case reflect.TypeOf(new(float32)),
		reflect.TypeOf(new(float64)):
		ft = TypeFloatField
	case reflect.TypeOf(new(bool)):
		ft = TypeBooleanField
	case reflect.TypeOf(new(string)):
		ft = TypeCharField
	case reflect.TypeOf(new(time.Time)):
		ft = TypeDateTimeField
	default:
		elm := reflect.Indirect(val)
		switch elm.Kind() {
		case reflect.Int8:
			ft = TypeBitField
		case reflect.Int16:
			ft = TypeSmallIntegerField
		case reflect.Int32, reflect.Int:
			ft = TypeIntegerField
		case reflect.Int64:
			ft = TypeBigIntegerField
		case reflect.Uint8:
			ft = TypePositiveBitField
		case reflect.Uint16:
			ft = TypePositiveSmallIntegerField
		case reflect.Uint32, reflect.Uint:
			ft = TypePositiveIntegerField
		case reflect.Uint64:
			ft = TypePositiveBigIntegerField
		case reflect.Float32, reflect.Float64:
			ft = TypeFloatField
		case reflect.Bool:
			ft = TypeBooleanField
		case reflect.String:
			ft = TypeCharField
		default:
			if elm.Interface() == nil {
				panic(fmt.Errorf("%s is nil pointer, may be miss setting tag", val))
			}
			switch elm.Interface().(type) {
			case sql.NullInt64:
				ft = TypeBigIntegerField
			case sql.NullFloat64:
				ft = TypeFloatField
			case sql.NullBool:
				ft = TypeBooleanField
			case sql.NullString:
				ft = TypeCharField
			case time.Time:
				ft = TypeDateTimeField
			}
		}
	}
	if ft&IsFieldType == 0 {
		err = fmt.Errorf("unsupport field type %s, may be miss setting tag", val)
	}
	return
}

// parse struct tag string
func parseStructTag(data string) (attrs map[string]bool, tags map[string]string) {
	attrs = make(map[string]bool)
	tags = make(map[string]string)
	for _, v := range strings.Split(data, defaultStructTagDelim) {
		if v == "" {
			continue
		}
		v = strings.TrimSpace(v)
		if t := strings.ToLower(v); supportTag[t] == 1 {
			attrs[t] = true
		} else if i := strings.Index(v, "("); i > 0 && strings.Index(v, ")") == len(v)-1 {
			name := t[:i]
			if supportTag[name] == 2 {
				v = v[i+1 : len(v)-1]
				tags[name] = v
			}
		} else {

		}
	}
	return
}

// single model info
type modelInfo struct {
	pkg       string
	name      string
	fullName  string
	table     string
	model     interface{}
	fields    *fields
	manual    bool
	addrField reflect.Value //store the original struct value
	uniques   []string
	isThrough bool
}

// new model info
func newModelInfo(val reflect.Value) (mi *modelInfo) {
	mi = &modelInfo{}
	mi.fields = newFields()
	ind := reflect.Indirect(val)
	mi.addrField = val
	mi.name = ind.Type().Name()
	mi.fullName = getFullName(ind.Type())
	addModelFields(mi, ind, "", []int{})
	return
}

// index: FieldByIndex returns the nested field corresponding to index
func addModelFields(mi *modelInfo, ind reflect.Value, mName string, index []int) {
	var (
		err error
		fi  *fieldInfo
		sf  reflect.StructField
	)

	for i := 0; i < ind.NumField(); i++ {
		field := ind.Field(i)
		sf = ind.Type().Field(i)
		// if the field is unexported skip
		if sf.PkgPath != "" {
			continue
		}
		// add anonymous struct fields
		if sf.Anonymous {
			addModelFields(mi, field, mName+"."+sf.Name, append(index, i))
			continue
		}

		fi, err = newFieldInfo(mi, field, sf, mName)
		if err == errSkipField {
			err = nil
			continue
		} else if err != nil {
			break
		}
		//record current field index
		fi.fieldIndex = append(index, i)
		fi.mi = mi
		fi.inModel = true
		if !mi.fields.Add(fi) {
			err = fmt.Errorf("duplicate column name: %s", fi.column)
			break
		}
		if fi.pk {
			if mi.fields.pk != nil {
				err = fmt.Errorf("one model must have one pk field only")
				break
			} else {
				mi.fields.pk = fi
			}
		}
	}

	if err != nil {
		fmt.Println(fmt.Errorf("field: %s.%s, %s", ind.Type(), sf.Name, err))
		os.Exit(2)
	}
}

// combine related model info to new model info.
// prepare for relation models query.
func newM2MModelInfo(m1, m2 *modelInfo) (mi *modelInfo) {
	mi = new(modelInfo)
	mi.fields = newFields()
	mi.table = m1.table + "_" + m2.table + "s"
	mi.name = camelString(mi.table)
	mi.fullName = m1.pkg + "." + mi.name

	fa := new(fieldInfo) // pk
	f1 := new(fieldInfo) // m1 table RelForeignKey
	f2 := new(fieldInfo) // m2 table RelForeignKey
	fa.fieldType = TypeBigIntegerField
	fa.auto = true
	fa.pk = true
	fa.dbcol = true
	fa.name = "Id"
	fa.column = "id"
	fa.fullName = mi.fullName + "." + fa.name

	f1.dbcol = true
	f2.dbcol = true
	f1.fieldType = RelForeignKey
	f2.fieldType = RelForeignKey
	f1.name = camelString(m1.table)
	f2.name = camelString(m2.table)
	f1.fullName = mi.fullName + "." + f1.name
	f2.fullName = mi.fullName + "." + f2.name
	f1.column = m1.table + "_id"
	f2.column = m2.table + "_id"
	f1.rel = true
	f2.rel = true
	f1.relTable = m1.table
	f2.relTable = m2.table
	f1.relModelInfo = m1
	f2.relModelInfo = m2
	f1.mi = mi
	f2.mi = mi

	mi.fields.Add(fa)
	mi.fields.Add(f1)
	mi.fields.Add(f2)
	mi.fields.pk = fa

	mi.uniques = []string{f1.column, f2.column}
	return
}

var errSkipField = errors.New("skip field")

// field info collection
type fields struct {
	pk            *fieldInfo
	columns       map[string]*fieldInfo
	fields        map[string]*fieldInfo
	fieldsLow     map[string]*fieldInfo
	fieldsByType  map[int][]*fieldInfo
	fieldsRel     []*fieldInfo
	fieldsReverse []*fieldInfo
	fieldsDB      []*fieldInfo
	rels          []*fieldInfo
	orders        []string
	dbcols        []string
}

// add field info
func (f *fields) Add(fi *fieldInfo) (added bool) {
	if f.fields[fi.name] == nil && f.columns[fi.column] == nil {
		f.columns[fi.column] = fi
		f.fields[fi.name] = fi
		f.fieldsLow[strings.ToLower(fi.name)] = fi
	} else {
		return
	}
	if _, ok := f.fieldsByType[fi.fieldType]; !ok {
		f.fieldsByType[fi.fieldType] = make([]*fieldInfo, 0)
	}
	f.fieldsByType[fi.fieldType] = append(f.fieldsByType[fi.fieldType], fi)
	f.orders = append(f.orders, fi.column)
	if fi.dbcol {
		f.dbcols = append(f.dbcols, fi.column)
		f.fieldsDB = append(f.fieldsDB, fi)
	}
	if fi.rel {
		f.fieldsRel = append(f.fieldsRel, fi)
	}
	if fi.reverse {
		f.fieldsReverse = append(f.fieldsReverse, fi)
	}
	return true
}

// get field info by name
func (f *fields) GetByName(name string) *fieldInfo {
	return f.fields[name]
}

// get field info by column name
func (f *fields) GetByColumn(column string) *fieldInfo {
	return f.columns[column]
}

// get field info by string, name is prior
func (f *fields) GetByAny(name string) (*fieldInfo, bool) {
	if fi, ok := f.fields[name]; ok {
		return fi, ok
	}
	if fi, ok := f.fieldsLow[strings.ToLower(name)]; ok {
		return fi, ok
	}
	if fi, ok := f.columns[name]; ok {
		return fi, ok
	}
	return nil, false
}

// create new field info collection
func newFields() *fields {
	f := new(fields)
	f.fields = make(map[string]*fieldInfo)
	f.fieldsLow = make(map[string]*fieldInfo)
	f.columns = make(map[string]*fieldInfo)
	f.fieldsByType = make(map[int][]*fieldInfo)
	return f
}

// single field info
type fieldInfo struct {
	mi                  *modelInfo
	fieldIndex          []int
	fieldType           int
	dbcol               bool // table column fk and onetoone
	inModel             bool
	name                string
	fullName            string
	column              string
	addrValue           reflect.Value
	sf                  reflect.StructField
	auto                bool
	pk                  bool
	null                bool
	index               bool
	unique              bool
	colDefault          bool  // whether has default tag
	initial             StrTo // store the default value
	size                int
	toText              bool
	autoNow             bool
	autoNowAdd          bool
	rel                 bool // if type equal to RelForeignKey, RelOneToOne, RelManyToMany then true
	reverse             bool
	reverseField        string
	reverseFieldInfo    *fieldInfo
	reverseFieldInfoTwo *fieldInfo
	reverseFieldInfoM2M *fieldInfo
	relTable            string
	relThrough          string
	relThroughModelInfo *modelInfo
	relModelInfo        *modelInfo
	digits              int
	decimals            int
	isFielder           bool // implement Fielder interface
	onDelete            string
}

// new field info
func newFieldInfo(mi *modelInfo, field reflect.Value, sf reflect.StructField, mName string) (fi *fieldInfo, err error) {
	var (
		tag       string
		tagValue  string
		initial   StrTo // store the default value
		fieldType int
		attrs     map[string]bool
		tags      map[string]string
		addrField reflect.Value
	)

	fi = new(fieldInfo)

	// if field which CanAddr is the follow type
	//  A value is addressable if it is an element of a slice,
	//  an element of an addressable array, a field of an
	//  addressable struct, or the result of dereferencing a pointer.
	addrField = field
	if field.CanAddr() && field.Kind() != reflect.Ptr {
		addrField = field.Addr()
		if _, ok := addrField.Interface().(Fielder); !ok {
			if field.Kind() == reflect.Slice {
				addrField = field
			}
		}
	}

	attrs, tags = parseStructTag(sf.Tag.Get(defaultStructTagName))

	if _, ok := attrs["-"]; ok {
		return nil, errSkipField
	}

	digits := tags["digits"]
	decimals := tags["decimals"]
	size := tags["size"]
	onDelete := tags["on_delete"]

	initial.Clear()
	if v, ok := tags["default"]; ok {
		initial.Set(v)
	}

checkType:
	switch f := addrField.Interface().(type) {
	case Fielder:
		fi.isFielder = true
		if field.Kind() == reflect.Ptr {
			err = fmt.Errorf("the model Fielder can not be use ptr")
			goto end
		}
		fieldType = f.FieldType()
		if fieldType&IsRelField > 0 {
			err = fmt.Errorf("unsupport type custom field, please refer to https://github.com/astaxie/beego/blob/master/orm/models_fields.go#L24-L42")
			goto end
		}
	default:
		tag = "rel"
		tagValue = tags[tag]
		if tagValue != "" {
			switch tagValue {
			case "fk":
				fieldType = RelForeignKey
				break checkType
			case "one":
				fieldType = RelOneToOne
				break checkType
			case "m2m":
				fieldType = RelManyToMany
				if tv := tags["rel_table"]; tv != "" {
					fi.relTable = tv
				} else if tv := tags["rel_through"]; tv != "" {
					fi.relThrough = tv
				}
				break checkType
			default:
				err = fmt.Errorf("rel only allow these value: fk, one, m2m")
				goto wrongTag
			}
		}
		tag = "reverse"
		tagValue = tags[tag]
		if tagValue != "" {
			switch tagValue {
			case "one":
				fieldType = RelReverseOne
				break checkType
			case "many":
				fieldType = RelReverseMany
				if tv := tags["rel_table"]; tv != "" {
					fi.relTable = tv
				} else if tv := tags["rel_through"]; tv != "" {
					fi.relThrough = tv
				}
				break checkType
			default:
				err = fmt.Errorf("reverse only allow these value: one, many")
				goto wrongTag
			}
		}

		fieldType, err = getFieldType(addrField)
		if err != nil {
			goto end
		}
		if fieldType == TypeCharField {
			switch tags["type"] {
			case "text":
				fieldType = TypeTextField
			case "json":
				fieldType = TypeJSONField
			case "jsonb":
				fieldType = TypeJsonbField
			}
		}
		if fieldType == TypeFloatField && (digits != "" || decimals != "") {
			fieldType = TypeDecimalField
		}
		if fieldType == TypeDateTimeField && tags["type"] == "date" {
			fieldType = TypeDateField
		}
		if fieldType == TypeTimeField && tags["type"] == "time" {
			fieldType = TypeTimeField
		}
	}

	// check the rel and reverse type
	// rel should Ptr
	// reverse should slice []*struct
	switch fieldType {
	case RelForeignKey, RelOneToOne, RelReverseOne:
		if field.Kind() != reflect.Ptr {
			err = fmt.Errorf("rel/reverse:one field must be *%s", field.Type().Name())
			goto end
		}
	case RelManyToMany, RelReverseMany:
		if field.Kind() != reflect.Slice {
			err = fmt.Errorf("rel/reverse:many field must be slice")
			goto end
		} else {
			if field.Type().Elem().Kind() != reflect.Ptr {
				err = fmt.Errorf("rel/reverse:many slice must be []*%s", field.Type().Elem().Name())
				goto end
			}
		}
	}

	if fieldType&IsFieldType == 0 {
		err = fmt.Errorf("wrong field type")
		goto end
	}

	fi.fieldType = fieldType
	fi.name = sf.Name
	fi.column = getColumnName(fieldType, addrField, sf, tags["column"])
	fi.addrValue = addrField
	fi.sf = sf
	fi.fullName = mi.fullName + mName + "." + sf.Name

	fi.null = attrs["null"]
	fi.index = attrs["index"]
	fi.auto = attrs["auto"]
	fi.pk = attrs["pk"]
	fi.unique = attrs["unique"]

	// Mark object property if there is attribute "default" in the orm configuration
	if _, ok := tags["default"]; ok {
		fi.colDefault = true
	}

	switch fieldType {
	case RelManyToMany, RelReverseMany, RelReverseOne:
		fi.null = false
		fi.index = false
		fi.auto = false
		fi.pk = false
		fi.unique = false
	default:
		fi.dbcol = true
	}

	switch fieldType {
	case RelForeignKey, RelOneToOne, RelManyToMany:
		fi.rel = true
		if fieldType == RelOneToOne {
			fi.unique = true
		}
	case RelReverseMany, RelReverseOne:
		fi.reverse = true
	}

	if fi.rel && fi.dbcol {
		switch onDelete {
		case odCascade, odDoNothing:
		case odSetDefault:
			if !initial.Exist() {
				err = errors.New("on_delete: set_default need set field a default value")
				goto end
			}
		case odSetNULL:
			if !fi.null {
				err = errors.New("on_delete: set_null need set field null")
				goto end
			}
		default:
			if onDelete == "" {
				onDelete = odCascade
			} else {
				err = fmt.Errorf("on_delete value expected choice in `cascade,set_null,set_default,do_nothing`, unknown `%s`", onDelete)
				goto end
			}
		}

		fi.onDelete = onDelete
	}

	switch fieldType {
	case TypeBooleanField:
	case TypeCharField, TypeJSONField, TypeJsonbField:
		if size != "" {
			v, e := StrTo(size).Int32()
			if e != nil {
				err = fmt.Errorf("wrong size value `%s`", size)
			} else {
				fi.size = int(v)
			}
		} else {
			fi.size = 255
			fi.toText = true
		}
	case TypeTextField:
		fi.index = false
		fi.unique = false
	case TypeTimeField, TypeDateField, TypeDateTimeField:
		if attrs["auto_now"] {
			fi.autoNow = true
		} else if attrs["auto_now_add"] {
			fi.autoNowAdd = true
		}
	case TypeFloatField:
	case TypeDecimalField:
		d1 := digits
		d2 := decimals
		v1, er1 := StrTo(d1).Int8()
		v2, er2 := StrTo(d2).Int8()
		if er1 != nil || er2 != nil {
			err = fmt.Errorf("wrong digits/decimals value %s/%s", d2, d1)
			goto end
		}
		fi.digits = int(v1)
		fi.decimals = int(v2)
	default:
		switch {
		case fieldType&IsIntegerField > 0:
		case fieldType&IsRelField > 0:
		}
	}

	if fieldType&IsIntegerField == 0 {
		if fi.auto {
			err = fmt.Errorf("non-integer type cannot set auto")
			goto end
		}
	}

	if fi.auto || fi.pk {
		if fi.auto {
			switch addrField.Elem().Kind() {
			case reflect.Int, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint32, reflect.Uint64:
			default:
				err = fmt.Errorf("auto primary key only support int, int32, int64, uint, uint32, uint64 but found `%s`", addrField.Elem().Kind())
				goto end
			}
			fi.pk = true
		}
		fi.null = false
		fi.index = false
		fi.unique = false
	}

	if fi.unique {
		fi.index = false
	}

	// can not set default for these type
	if fi.auto || fi.pk || fi.unique || fieldType == TypeTimeField || fieldType == TypeDateField || fieldType == TypeDateTimeField {
		initial.Clear()
	}

	if initial.Exist() {
		v := initial
		switch fieldType {
		case TypeBooleanField:
			_, err = v.Bool()
		case TypeFloatField, TypeDecimalField:
			_, err = v.Float64()
		case TypeBitField:
			_, err = v.Int8()
		case TypeSmallIntegerField:
			_, err = v.Int16()
		case TypeIntegerField:
			_, err = v.Int32()
		case TypeBigIntegerField:
			_, err = v.Int64()
		case TypePositiveBitField:
			_, err = v.Uint8()
		case TypePositiveSmallIntegerField:
			_, err = v.Uint16()
		case TypePositiveIntegerField:
			_, err = v.Uint32()
		case TypePositiveBigIntegerField:
			_, err = v.Uint64()
		}
		if err != nil {
			tag, tagValue = "default", tags["default"]
			goto wrongTag
		}
	}

	fi.initial = initial
end:
	if err != nil {
		return nil, err
	}
	return
wrongTag:
	return nil, fmt.Errorf("wrong tag format: `%s:\"%s\"`, %s", tag, tagValue, err)
}

// Define the Type enum
const (
	TypeBooleanField = 1 << iota
	TypeCharField
	TypeTextField
	TypeTimeField
	TypeDateField
	TypeDateTimeField
	TypeBitField
	TypeSmallIntegerField
	TypeIntegerField
	TypeBigIntegerField
	TypePositiveBitField
	TypePositiveSmallIntegerField
	TypePositiveIntegerField
	TypePositiveBigIntegerField
	TypeFloatField
	TypeDecimalField
	TypeJSONField
	TypeJsonbField
	RelForeignKey
	RelOneToOne
	RelManyToMany
	RelReverseOne
	RelReverseMany
)

// Define some logic enum
const (
	IsIntegerField         = ^-TypePositiveBigIntegerField >> 5 << 6
	IsPositiveIntegerField = ^-TypePositiveBigIntegerField >> 9 << 10
	IsRelField             = ^-RelReverseMany >> 17 << 18
	IsFieldType            = ^-RelReverseMany<<1 + 1
)

// BooleanField A true/false field.
type BooleanField bool

// Value return the BooleanField
func (e BooleanField) Value() bool {
	return bool(e)
}

// Set will set the BooleanField
func (e *BooleanField) Set(d bool) {
	*e = BooleanField(d)
}

// String format the Bool to string
func (e *BooleanField) String() string {
	return strconv.FormatBool(e.Value())
}

// FieldType return BooleanField the type
func (e *BooleanField) FieldType() int {
	return TypeBooleanField
}

// SetRaw set the interface to bool
func (e *BooleanField) SetRaw(value interface{}) error {
	switch d := value.(type) {
	case bool:
		e.Set(d)
	case string:
		v, err := StrTo(d).Bool()
		if err != nil {
			e.Set(v)
		}
		return err
	default:
		return fmt.Errorf("<BooleanField.SetRaw> unknown value `%s`", value)
	}
	return nil
}

// RawValue return the current value
func (e *BooleanField) RawValue() interface{} {
	return e.Value()
}

// verify the BooleanField implement the Fielder interface
var _ Fielder = new(BooleanField)

// CharField A string field
// required values tag: size
// The size is enforced at the database level and in models’s validation.
// eg: `orm:"size(120)"`
type CharField string

// Value return the CharField's Value
func (e CharField) Value() string {
	return string(e)
}

// Set CharField value
func (e *CharField) Set(d string) {
	*e = CharField(d)
}

// String return the CharField
func (e *CharField) String() string {
	return e.Value()
}

// FieldType return the enum type
func (e *CharField) FieldType() int {
	return TypeCharField
}

// SetRaw set the interface to string
func (e *CharField) SetRaw(value interface{}) error {
	switch d := value.(type) {
	case string:
		e.Set(d)
	default:
		return fmt.Errorf("<CharField.SetRaw> unknown value `%s`", value)
	}
	return nil
}

// RawValue return the CharField value
func (e *CharField) RawValue() interface{} {
	return e.Value()
}

// verify CharField implement Fielder
var _ Fielder = new(CharField)

// TimeField A time, represented in go by a time.Time instance.
// only time values like 10:00:00
// Has a few extra, optional attr tag:
//
// auto_now:
// Automatically set the field to now every time the object is saved. Useful for “last-modified” timestamps.
// Note that the current date is always used; it’s not just a default value that you can override.
//
// auto_now_add:
// Automatically set the field to now when the object is first created. Useful for creation of timestamps.
// Note that the current date is always used; it’s not just a default value that you can override.
//
// eg: `orm:"auto_now"` or `orm:"auto_now_add"`
type TimeField time.Time

// Value return the time.Time
func (e TimeField) Value() time.Time {
	return time.Time(e)
}

// Set set the TimeField's value
func (e *TimeField) Set(d time.Time) {
	*e = TimeField(d)
}

// String convert time to string
func (e *TimeField) String() string {
	return e.Value().String()
}

// FieldType return enum type Date
func (e *TimeField) FieldType() int {
	return TypeDateField
}

// SetRaw convert the interface to time.Time. Allow string and time.Time
func (e *TimeField) SetRaw(value interface{}) error {
	switch d := value.(type) {
	case time.Time:
		e.Set(d)
	case string:
		v, err := timeParse(d, formatTime)
		if err != nil {
			e.Set(v)
		}
		return err
	default:
		return fmt.Errorf("<TimeField.SetRaw> unknown value `%s`", value)
	}
	return nil
}

// RawValue return time value
func (e *TimeField) RawValue() interface{} {
	return e.Value()
}

var _ Fielder = new(TimeField)

// DateField A date, represented in go by a time.Time instance.
// only date values like 2006-01-02
// Has a few extra, optional attr tag:
//
// auto_now:
// Automatically set the field to now every time the object is saved. Useful for “last-modified” timestamps.
// Note that the current date is always used; it’s not just a default value that you can override.
//
// auto_now_add:
// Automatically set the field to now when the object is first created. Useful for creation of timestamps.
// Note that the current date is always used; it’s not just a default value that you can override.
//
// eg: `orm:"auto_now"` or `orm:"auto_now_add"`
type DateField time.Time

// Value return the time.Time
func (e DateField) Value() time.Time {
	return time.Time(e)
}

// Set set the DateField's value
func (e *DateField) Set(d time.Time) {
	*e = DateField(d)
}

// String convert datatime to string
func (e *DateField) String() string {
	return e.Value().String()
}

// FieldType return enum type Date
func (e *DateField) FieldType() int {
	return TypeDateField
}

// SetRaw convert the interface to time.Time. Allow string and time.Time
func (e *DateField) SetRaw(value interface{}) error {
	switch d := value.(type) {
	case time.Time:
		e.Set(d)
	case string:
		v, err := timeParse(d, formatDate)
		if err != nil {
			e.Set(v)
		}
		return err
	default:
		return fmt.Errorf("<DateField.SetRaw> unknown value `%s`", value)
	}
	return nil
}

// RawValue return Date value
func (e *DateField) RawValue() interface{} {
	return e.Value()
}

// verify DateField implement fielder interface
var _ Fielder = new(DateField)

// DateTimeField A date, represented in go by a time.Time instance.
// datetime values like 2006-01-02 15:04:05
// Takes the same extra arguments as DateField.
type DateTimeField time.Time

// Value return the datatime value
func (e DateTimeField) Value() time.Time {
	return time.Time(e)
}

// Set set the time.Time to datatime
func (e *DateTimeField) Set(d time.Time) {
	*e = DateTimeField(d)
}

// String return the time's String
func (e *DateTimeField) String() string {
	return e.Value().String()
}

// FieldType return the enum TypeDateTimeField
func (e *DateTimeField) FieldType() int {
	return TypeDateTimeField
}

// SetRaw convert the string or time.Time to DateTimeField
func (e *DateTimeField) SetRaw(value interface{}) error {
	switch d := value.(type) {
	case time.Time:
		e.Set(d)
	case string:
		v, err := timeParse(d, formatDateTime)
		if err != nil {
			e.Set(v)
		}
		return err
	default:
		return fmt.Errorf("<DateTimeField.SetRaw> unknown value `%s`", value)
	}
	return nil
}

// RawValue return the datatime value
func (e *DateTimeField) RawValue() interface{} {
	return e.Value()
}

// verify datatime implement fielder
var _ Fielder = new(DateTimeField)

// FloatField A floating-point number represented in go by a float32 value.
type FloatField float64

// Value return the FloatField value
func (e FloatField) Value() float64 {
	return float64(e)
}

// Set the Float64
func (e *FloatField) Set(d float64) {
	*e = FloatField(d)
}

// String return the string
func (e *FloatField) String() string {
	return ToStr(e.Value(), -1, 32)
}

// FieldType return the enum type
func (e *FloatField) FieldType() int {
	return TypeFloatField
}

// SetRaw converter interface Float64 float32 or string to FloatField
func (e *FloatField) SetRaw(value interface{}) error {
	switch d := value.(type) {
	case float32:
		e.Set(float64(d))
	case float64:
		e.Set(d)
	case string:
		v, err := StrTo(d).Float64()
		if err != nil {
			e.Set(v)
		}
	default:
		return fmt.Errorf("<FloatField.SetRaw> unknown value `%s`", value)
	}
	return nil
}

// RawValue return the FloatField value
func (e *FloatField) RawValue() interface{} {
	return e.Value()
}

// verify FloatField implement Fielder
var _ Fielder = new(FloatField)

// SmallIntegerField -32768 to 32767
type SmallIntegerField int16

// Value return int16 value
func (e SmallIntegerField) Value() int16 {
	return int16(e)
}

// Set the SmallIntegerField value
func (e *SmallIntegerField) Set(d int16) {
	*e = SmallIntegerField(d)
}

// String convert smallint to string
func (e *SmallIntegerField) String() string {
	return ToStr(e.Value())
}

// FieldType return enum type SmallIntegerField
func (e *SmallIntegerField) FieldType() int {
	return TypeSmallIntegerField
}

// SetRaw convert interface int16/string to int16
func (e *SmallIntegerField) SetRaw(value interface{}) error {
	switch d := value.(type) {
	case int16:
		e.Set(d)
	case string:
		v, err := StrTo(d).Int16()
		if err != nil {
			e.Set(v)
		}
	default:
		return fmt.Errorf("<SmallIntegerField.SetRaw> unknown value `%s`", value)
	}
	return nil
}

// RawValue return smallint value
func (e *SmallIntegerField) RawValue() interface{} {
	return e.Value()
}

// verify SmallIntegerField implement Fielder
var _ Fielder = new(SmallIntegerField)

// IntegerField -2147483648 to 2147483647
type IntegerField int32

// Value return the int32
func (e IntegerField) Value() int32 {
	return int32(e)
}

// Set IntegerField value
func (e *IntegerField) Set(d int32) {
	*e = IntegerField(d)
}

// String convert Int32 to string
func (e *IntegerField) String() string {
	return ToStr(e.Value())
}

// FieldType return the enum type
func (e *IntegerField) FieldType() int {
	return TypeIntegerField
}

// SetRaw convert interface int32/string to int32
func (e *IntegerField) SetRaw(value interface{}) error {
	switch d := value.(type) {
	case int32:
		e.Set(d)
	case string:
		v, err := StrTo(d).Int32()
		if err != nil {
			e.Set(v)
		}
	default:
		return fmt.Errorf("<IntegerField.SetRaw> unknown value `%s`", value)
	}
	return nil
}

// RawValue return IntegerField value
func (e *IntegerField) RawValue() interface{} {
	return e.Value()
}

// verify IntegerField implement Fielder
var _ Fielder = new(IntegerField)

// BigIntegerField -9223372036854775808 to 9223372036854775807.
type BigIntegerField int64

// Value return int64
func (e BigIntegerField) Value() int64 {
	return int64(e)
}

// Set the BigIntegerField value
func (e *BigIntegerField) Set(d int64) {
	*e = BigIntegerField(d)
}

// String convert BigIntegerField to string
func (e *BigIntegerField) String() string {
	return ToStr(e.Value())
}

// FieldType return enum type
func (e *BigIntegerField) FieldType() int {
	return TypeBigIntegerField
}

// SetRaw convert interface int64/string to int64
func (e *BigIntegerField) SetRaw(value interface{}) error {
	switch d := value.(type) {
	case int64:
		e.Set(d)
	case string:
		v, err := StrTo(d).Int64()
		if err != nil {
			e.Set(v)
		}
	default:
		return fmt.Errorf("<BigIntegerField.SetRaw> unknown value `%s`", value)
	}
	return nil
}

// RawValue return BigIntegerField value
func (e *BigIntegerField) RawValue() interface{} {
	return e.Value()
}

// verify BigIntegerField implement Fielder
var _ Fielder = new(BigIntegerField)

// PositiveSmallIntegerField 0 to 65535
type PositiveSmallIntegerField uint16

// Value return uint16
func (e PositiveSmallIntegerField) Value() uint16 {
	return uint16(e)
}

// Set PositiveSmallIntegerField value
func (e *PositiveSmallIntegerField) Set(d uint16) {
	*e = PositiveSmallIntegerField(d)
}

// String convert uint16 to string
func (e *PositiveSmallIntegerField) String() string {
	return ToStr(e.Value())
}

// FieldType return enum type
func (e *PositiveSmallIntegerField) FieldType() int {
	return TypePositiveSmallIntegerField
}

// SetRaw convert Interface uint16/string to uint16
func (e *PositiveSmallIntegerField) SetRaw(value interface{}) error {
	switch d := value.(type) {
	case uint16:
		e.Set(d)
	case string:
		v, err := StrTo(d).Uint16()
		if err != nil {
			e.Set(v)
		}
	default:
		return fmt.Errorf("<PositiveSmallIntegerField.SetRaw> unknown value `%s`", value)
	}
	return nil
}

// RawValue returns PositiveSmallIntegerField value
func (e *PositiveSmallIntegerField) RawValue() interface{} {
	return e.Value()
}

// verify PositiveSmallIntegerField implement Fielder
var _ Fielder = new(PositiveSmallIntegerField)

// PositiveIntegerField 0 to 4294967295
type PositiveIntegerField uint32

// Value return PositiveIntegerField value. Uint32
func (e PositiveIntegerField) Value() uint32 {
	return uint32(e)
}

// Set the PositiveIntegerField value
func (e *PositiveIntegerField) Set(d uint32) {
	*e = PositiveIntegerField(d)
}

// String convert PositiveIntegerField to string
func (e *PositiveIntegerField) String() string {
	return ToStr(e.Value())
}

// FieldType return enum type
func (e *PositiveIntegerField) FieldType() int {
	return TypePositiveIntegerField
}

// SetRaw convert interface uint32/string to Uint32
func (e *PositiveIntegerField) SetRaw(value interface{}) error {
	switch d := value.(type) {
	case uint32:
		e.Set(d)
	case string:
		v, err := StrTo(d).Uint32()
		if err != nil {
			e.Set(v)
		}
	default:
		return fmt.Errorf("<PositiveIntegerField.SetRaw> unknown value `%s`", value)
	}
	return nil
}

// RawValue return the PositiveIntegerField Value
func (e *PositiveIntegerField) RawValue() interface{} {
	return e.Value()
}

// verify PositiveIntegerField implement Fielder
var _ Fielder = new(PositiveIntegerField)

// PositiveBigIntegerField 0 to 18446744073709551615
type PositiveBigIntegerField uint64

// Value return uint64
func (e PositiveBigIntegerField) Value() uint64 {
	return uint64(e)
}

// Set PositiveBigIntegerField value
func (e *PositiveBigIntegerField) Set(d uint64) {
	*e = PositiveBigIntegerField(d)
}

// String convert PositiveBigIntegerField to string
func (e *PositiveBigIntegerField) String() string {
	return ToStr(e.Value())
}

// FieldType return enum type
func (e *PositiveBigIntegerField) FieldType() int {
	return TypePositiveIntegerField
}

// SetRaw convert interface uint64/string to Uint64
func (e *PositiveBigIntegerField) SetRaw(value interface{}) error {
	switch d := value.(type) {
	case uint64:
		e.Set(d)
	case string:
		v, err := StrTo(d).Uint64()
		if err != nil {
			e.Set(v)
		}
	default:
		return fmt.Errorf("<PositiveBigIntegerField.SetRaw> unknown value `%s`", value)
	}
	return nil
}

// RawValue return PositiveBigIntegerField value
func (e *PositiveBigIntegerField) RawValue() interface{} {
	return e.Value()
}

// verify PositiveBigIntegerField implement Fielder
var _ Fielder = new(PositiveBigIntegerField)

// TextField A large text field.
type TextField string

// Value return TextField value
func (e TextField) Value() string {
	return string(e)
}

// Set the TextField value
func (e *TextField) Set(d string) {
	*e = TextField(d)
}

// String convert TextField to string
func (e *TextField) String() string {
	return e.Value()
}

// FieldType return enum type
func (e *TextField) FieldType() int {
	return TypeTextField
}

// SetRaw convert interface string to string
func (e *TextField) SetRaw(value interface{}) error {
	switch d := value.(type) {
	case string:
		e.Set(d)
	default:
		return fmt.Errorf("<TextField.SetRaw> unknown value `%s`", value)
	}
	return nil
}

// RawValue return TextField value
func (e *TextField) RawValue() interface{} {
	return e.Value()
}

// verify TextField implement Fielder
var _ Fielder = new(TextField)

// JSONField postgres json field.
type JSONField string

// Value return JSONField value
func (j JSONField) Value() string {
	return string(j)
}

// Set the JSONField value
func (j *JSONField) Set(d string) {
	*j = JSONField(d)
}

// String convert JSONField to string
func (j *JSONField) String() string {
	return j.Value()
}

// FieldType return enum type
func (j *JSONField) FieldType() int {
	return TypeJSONField
}

// SetRaw convert interface string to string
func (j *JSONField) SetRaw(value interface{}) error {
	switch d := value.(type) {
	case string:
		j.Set(d)
	default:
		return fmt.Errorf("<JSONField.SetRaw> unknown value `%s`", value)
	}
	return nil
}

// RawValue return JSONField value
func (j *JSONField) RawValue() interface{} {
	return j.Value()
}

// verify JSONField implement Fielder
var _ Fielder = new(JSONField)

// JsonbField postgres json field.
type JsonbField string

// Value return JsonbField value
func (j JsonbField) Value() string {
	return string(j)
}

// Set the JsonbField value
func (j *JsonbField) Set(d string) {
	*j = JsonbField(d)
}

// String convert JsonbField to string
func (j *JsonbField) String() string {
	return j.Value()
}

// FieldType return enum type
func (j *JsonbField) FieldType() int {
	return TypeJsonbField
}

// SetRaw convert interface string to string
func (j *JsonbField) SetRaw(value interface{}) error {
	switch d := value.(type) {
	case string:
		j.Set(d)
	default:
		return fmt.Errorf("<JsonbField.SetRaw> unknown value `%s`", value)
	}
	return nil
}

// RawValue return JsonbField value
func (j *JsonbField) RawValue() interface{} {
	return j.Value()
}

// verify JsonbField implement Fielder
var _ Fielder = new(JsonbField)

// register models.
// PrefixOrSuffix means table name prefix or suffix.
// isPrefix whether the prefix is prefix or suffix
func registerModel(PrefixOrSuffix string, model interface{}, isPrefix bool) {
	val := reflect.ValueOf(model)
	typ := reflect.Indirect(val).Type()

	if val.Kind() != reflect.Ptr {
		panic(fmt.Errorf("<orm.RegisterModel> cannot use non-ptr model struct `%s`", getFullName(typ)))
	}
	// For this case:
	// u := &User{}
	// registerModel(&u)
	if typ.Kind() == reflect.Ptr {
		panic(fmt.Errorf("<orm.RegisterModel> only allow ptr model struct, it looks you use two reference to the struct `%s`", typ))
	}

	table := getTableName(val)

	if PrefixOrSuffix != "" {
		if isPrefix {
			table = PrefixOrSuffix + table
		} else {
			table = table + PrefixOrSuffix
		}
	}
	// models's fullname is pkgpath + struct name
	name := getFullName(typ)
	if _, ok := modelCache.getByFullName(name); ok {
		fmt.Printf("<orm.RegisterModel> model `%s` repeat register, must be unique\n", name)
		os.Exit(2)
	}

	if _, ok := modelCache.get(table); ok {
		fmt.Printf("<orm.RegisterModel> table name `%s` repeat register, must be unique\n", table)
		os.Exit(2)
	}

	mi := newModelInfo(val)
	if mi.fields.pk == nil {
	outFor:
		for _, fi := range mi.fields.fieldsDB {
			if strings.ToLower(fi.name) == "id" {
				switch fi.addrValue.Elem().Kind() {
				case reflect.Int, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint32, reflect.Uint64:
					fi.auto = true
					fi.pk = true
					mi.fields.pk = fi
					break outFor
				}
			}
		}

		if mi.fields.pk == nil {
			fmt.Printf("<orm.RegisterModel> `%s` needs a primary key field, default is to use 'id' if not set\n", name)
			os.Exit(2)
		}

	}

	mi.table = table
	mi.pkg = typ.PkgPath()
	mi.model = model
	mi.manual = true

	modelCache.set(table, mi)
}

// boostrap models
func bootStrap() {
	if modelCache.done {
		return
	}
	var (
		err    error
		models map[string]*modelInfo
	)
	// if dataBaseCache.getDefault() == nil {
	// 	err = fmt.Errorf("must have one register DataBase alias named `default`")
	// 	goto end
	// }

	// set rel and reverse model
	// RelManyToMany set the relTable
	models = modelCache.all()
	for _, mi := range models {
		for _, fi := range mi.fields.columns {
			if fi.rel || fi.reverse {
				elm := fi.addrValue.Type().Elem()
				if fi.fieldType == RelReverseMany || fi.fieldType == RelManyToMany {
					elm = elm.Elem()
				}
				// check the rel or reverse model already register
				name := getFullName(elm)
				mii, ok := modelCache.getByFullName(name)
				if !ok || mii.pkg != elm.PkgPath() {
					err = fmt.Errorf("can not find rel in field `%s`, `%s` may be miss register", fi.fullName, elm.String())
					goto end
				}
				fi.relModelInfo = mii

				switch fi.fieldType {
				case RelManyToMany:
					if fi.relThrough != "" {
						if i := strings.LastIndex(fi.relThrough, "."); i != -1 && len(fi.relThrough) > (i+1) {
							pn := fi.relThrough[:i]
							rmi, ok := modelCache.getByFullName(fi.relThrough)
							if !ok || pn != rmi.pkg {
								err = fmt.Errorf("field `%s` wrong rel_through value `%s` cannot find table", fi.fullName, fi.relThrough)
								goto end
							}
							fi.relThroughModelInfo = rmi
							fi.relTable = rmi.table
						} else {
							err = fmt.Errorf("field `%s` wrong rel_through value `%s`", fi.fullName, fi.relThrough)
							goto end
						}
					} else {
						i := newM2MModelInfo(mi, mii)
						if fi.relTable != "" {
							i.table = fi.relTable
						}
						if v := modelCache.set(i.table, i); v != nil {
							err = fmt.Errorf("the rel table name `%s` already registered, cannot be use, please change one", fi.relTable)
							goto end
						}
						fi.relTable = i.table
						fi.relThroughModelInfo = i
					}

					fi.relThroughModelInfo.isThrough = true
				}
			}
		}
	}

	// check the rel filed while the relModelInfo also has filed point to current model
	// if not exist, add a new field to the relModelInfo
	models = modelCache.all()
	for _, mi := range models {
		for _, fi := range mi.fields.fieldsRel {
			switch fi.fieldType {
			case RelForeignKey, RelOneToOne, RelManyToMany:
				inModel := false
				for _, ffi := range fi.relModelInfo.fields.fieldsReverse {
					if ffi.relModelInfo == mi {
						inModel = true
						break
					}
				}
				if !inModel {
					rmi := fi.relModelInfo
					ffi := new(fieldInfo)
					ffi.name = mi.name
					ffi.column = ffi.name
					ffi.fullName = rmi.fullName + "." + ffi.name
					ffi.reverse = true
					ffi.relModelInfo = mi
					ffi.mi = rmi
					if fi.fieldType == RelOneToOne {
						ffi.fieldType = RelReverseOne
					} else {
						ffi.fieldType = RelReverseMany
					}
					if !rmi.fields.Add(ffi) {
						added := false
						for cnt := 0; cnt < 5; cnt++ {
							ffi.name = fmt.Sprintf("%s%d", mi.name, cnt)
							ffi.column = ffi.name
							ffi.fullName = rmi.fullName + "." + ffi.name
							if added = rmi.fields.Add(ffi); added {
								break
							}
						}
						if !added {
							panic(fmt.Errorf("cannot generate auto reverse field info `%s` to `%s`", fi.fullName, ffi.fullName))
						}
					}
				}
			}
		}
	}

	models = modelCache.all()
	for _, mi := range models {
		for _, fi := range mi.fields.fieldsRel {
			switch fi.fieldType {
			case RelManyToMany:
				for _, ffi := range fi.relThroughModelInfo.fields.fieldsRel {
					switch ffi.fieldType {
					case RelOneToOne, RelForeignKey:
						if ffi.relModelInfo == fi.relModelInfo {
							fi.reverseFieldInfoTwo = ffi
						}
						if ffi.relModelInfo == mi {
							fi.reverseField = ffi.name
							fi.reverseFieldInfo = ffi
						}
					}
				}
				if fi.reverseFieldInfoTwo == nil {
					err = fmt.Errorf("can not find m2m field for m2m model `%s`, ensure your m2m model defined correct",
						fi.relThroughModelInfo.fullName)
					goto end
				}
			}
		}
	}

	models = modelCache.all()
	for _, mi := range models {
		for _, fi := range mi.fields.fieldsReverse {
			switch fi.fieldType {
			case RelReverseOne:
				found := false
			mForA:
				for _, ffi := range fi.relModelInfo.fields.fieldsByType[RelOneToOne] {
					if ffi.relModelInfo == mi {
						found = true
						fi.reverseField = ffi.name
						fi.reverseFieldInfo = ffi

						ffi.reverseField = fi.name
						ffi.reverseFieldInfo = fi
						break mForA
					}
				}
				if !found {
					err = fmt.Errorf("reverse field `%s` not found in model `%s`", fi.fullName, fi.relModelInfo.fullName)
					goto end
				}
			case RelReverseMany:
				found := false
			mForB:
				for _, ffi := range fi.relModelInfo.fields.fieldsByType[RelForeignKey] {
					if ffi.relModelInfo == mi {
						found = true
						fi.reverseField = ffi.name
						fi.reverseFieldInfo = ffi

						ffi.reverseField = fi.name
						ffi.reverseFieldInfo = fi

						break mForB
					}
				}
				if !found {
				mForC:
					for _, ffi := range fi.relModelInfo.fields.fieldsByType[RelManyToMany] {
						conditions := fi.relThrough != "" && fi.relThrough == ffi.relThrough ||
							fi.relTable != "" && fi.relTable == ffi.relTable ||
							fi.relThrough == "" && fi.relTable == ""
						if ffi.relModelInfo == mi && conditions {
							found = true

							fi.reverseField = ffi.reverseFieldInfoTwo.name
							fi.reverseFieldInfo = ffi.reverseFieldInfoTwo
							fi.relThroughModelInfo = ffi.relThroughModelInfo
							fi.reverseFieldInfoTwo = ffi.reverseFieldInfo
							fi.reverseFieldInfoM2M = ffi
							ffi.reverseFieldInfoM2M = fi

							break mForC
						}
					}
				}
				if !found {
					err = fmt.Errorf("reverse field for `%s` not found in model `%s`", fi.fullName, fi.relModelInfo.fullName)
					goto end
				}
			}
		}
	}

end:
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
}

// RegisterModel register models
func RegisterModel(models ...interface{}) {
	if modelCache.done {
		panic(fmt.Errorf("RegisterModel must be run before BootStrap"))
	}
	RegisterModelWithPrefix("", models...)
}

// RegisterModelWithPrefix register models with a prefix
func RegisterModelWithPrefix(prefix string, models ...interface{}) {
	if modelCache.done {
		panic(fmt.Errorf("RegisterModelWithPrefix must be run before BootStrap"))
	}

	for _, model := range models {
		registerModel(prefix, model, true)
	}
}

// RegisterModelWithSuffix register models with a suffix
func RegisterModelWithSuffix(suffix string, models ...interface{}) {
	if modelCache.done {
		panic(fmt.Errorf("RegisterModelWithSuffix must be run before BootStrap"))
	}

	for _, model := range models {
		registerModel(suffix, model, false)
	}
}

// BootStrap bootrap models.
// make all model parsed and can not add more models
func BootStrap() {
	if modelCache.done {
		return
	}
	modelCache.Lock()
	defer modelCache.Unlock()
	bootStrap()
	modelCache.done = true
}
