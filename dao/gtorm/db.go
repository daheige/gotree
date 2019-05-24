package gtorm

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	formatTime     = "15:04:05"
	formatDate     = "2006-01-02"
	formatDateTime = "2006-01-02 15:04:05"
)

var (
	// ErrMissPK missing pk error
	ErrMissPK = errors.New("missed pk value")
)

// an instance of dbBaser interface/
type dbBase struct {
	ins dbBaser
}

// check dbBase implements dbBaser interface.
var _ dbBaser = new(dbBase)

// get struct columns values as interface slice.
func (d *dbBase) collectValues(mi *modelInfo, ind reflect.Value, cols []string, skipAuto bool, insert bool, names *[]string, tz *time.Location) (values []interface{}, autoFields []string, err error) {
	if names == nil {
		ns := make([]string, 0, len(cols))
		names = &ns
	}
	values = make([]interface{}, 0, len(cols))

	for _, column := range cols {
		var fi *fieldInfo
		if fi, _ = mi.fields.GetByAny(column); fi != nil {
			column = fi.column
		} else {
			panic(fmt.Errorf("wrong db field/column name `%s` for model `%s`", column, mi.fullName))
		}
		if !fi.dbcol || fi.auto && skipAuto {
			continue
		}
		value, err := d.collectFieldValue(mi, fi, ind, insert, tz)
		if err != nil {
			return nil, nil, err
		}

		// ignore empty value auto field
		if insert && fi.auto {
			if fi.fieldType&IsPositiveIntegerField > 0 {
				if vu, ok := value.(uint64); !ok || vu == 0 {
					continue
				}
			} else {
				if vu, ok := value.(int64); !ok || vu == 0 {
					continue
				}
			}
			autoFields = append(autoFields, fi.column)
		}

		*names, values = append(*names, column), append(values, value)
	}

	return
}

// get one field value in struct column as interface.
func (d *dbBase) collectFieldValue(mi *modelInfo, fi *fieldInfo, ind reflect.Value, insert bool, tz *time.Location) (interface{}, error) {
	var value interface{}
	if fi.pk {
		_, value, _ = getExistPk(mi, ind)
	} else {
		field := ind.FieldByIndex(fi.fieldIndex)
		if fi.isFielder {
			f := field.Addr().Interface().(Fielder)
			value = f.RawValue()
		} else {
			switch fi.fieldType {
			case TypeBooleanField:
				if nb, ok := field.Interface().(sql.NullBool); ok {
					value = nil
					if nb.Valid {
						value = nb.Bool
					}
				} else if field.Kind() == reflect.Ptr {
					if field.IsNil() {
						value = nil
					} else {
						value = field.Elem().Bool()
					}
				} else {
					value = field.Bool()
				}
			case TypeCharField, TypeTextField, TypeJSONField, TypeJsonbField:
				if ns, ok := field.Interface().(sql.NullString); ok {
					value = nil
					if ns.Valid {
						value = ns.String
					}
				} else if field.Kind() == reflect.Ptr {
					if field.IsNil() {
						value = nil
					} else {
						value = field.Elem().String()
					}
				} else {
					value = field.String()
				}
			case TypeFloatField, TypeDecimalField:
				if nf, ok := field.Interface().(sql.NullFloat64); ok {
					value = nil
					if nf.Valid {
						value = nf.Float64
					}
				} else if field.Kind() == reflect.Ptr {
					if field.IsNil() {
						value = nil
					} else {
						value = field.Elem().Float()
					}
				} else {
					vu := field.Interface()
					if _, ok := vu.(float32); ok {
						value, _ = StrTo(ToStr(vu)).Float64()
					} else {
						value = field.Float()
					}
				}
			case TypeTimeField, TypeDateField, TypeDateTimeField:
				value = field.Interface()
				if t, ok := value.(time.Time); ok {
					d.ins.TimeToDB(&t, tz)
					if t.IsZero() {
						value = nil
					} else {
						value = t
					}
				}
			default:
				switch {
				case fi.fieldType&IsPositiveIntegerField > 0:
					if field.Kind() == reflect.Ptr {
						if field.IsNil() {
							value = nil
						} else {
							value = field.Elem().Uint()
						}
					} else {
						value = field.Uint()
					}
				case fi.fieldType&IsIntegerField > 0:
					if ni, ok := field.Interface().(sql.NullInt64); ok {
						value = nil
						if ni.Valid {
							value = ni.Int64
						}
					} else if field.Kind() == reflect.Ptr {
						if field.IsNil() {
							value = nil
						} else {
							value = field.Elem().Int()
						}
					} else {
						value = field.Int()
					}
				case fi.fieldType&IsRelField > 0:
					if field.IsNil() {
						value = nil
					} else {
						if _, vu, ok := getExistPk(fi.relModelInfo, reflect.Indirect(field)); ok {
							value = vu
						} else {
							value = nil
						}
					}
					if !fi.null && value == nil {
						return nil, fmt.Errorf("field `%s` cannot be NULL", fi.fullName)
					}
				}
			}
		}
		switch fi.fieldType {
		case TypeTimeField, TypeDateField, TypeDateTimeField:
			if fi.autoNow || fi.autoNowAdd && insert {
				if insert {
					if t, ok := value.(time.Time); ok && !t.IsZero() {
						break
					}
				}
				tnow := time.Now()
				d.ins.TimeToDB(&tnow, tz)
				value = tnow
				if fi.isFielder {
					f := field.Addr().Interface().(Fielder)
					f.SetRaw(tnow.In(DefaultTimeLoc))
				} else if field.Kind() == reflect.Ptr {
					v := tnow.In(DefaultTimeLoc)
					field.Set(reflect.ValueOf(&v))
				} else {
					field.Set(reflect.ValueOf(tnow.In(DefaultTimeLoc)))
				}
			}
		case TypeJSONField, TypeJsonbField:
			if s, ok := value.(string); (ok && len(s) == 0) || value == nil {
				if fi.colDefault && fi.initial.Exist() {
					value = fi.initial.String()
				} else {
					value = nil
				}
			}
		}
	}
	return value, nil
}

// create insert sql preparation statement object.
func (d *dbBase) PrepareInsert(q dbQuerier, mi *modelInfo) (stmtQuerier, string, error) {
	Q := d.ins.TableQuote()

	dbcols := make([]string, 0, len(mi.fields.dbcols))
	marks := make([]string, 0, len(mi.fields.dbcols))
	for _, fi := range mi.fields.fieldsDB {
		if !fi.auto {
			dbcols = append(dbcols, fi.column)
			marks = append(marks, "?")
		}
	}
	qmarks := strings.Join(marks, ", ")
	sep := fmt.Sprintf("%s, %s", Q, Q)
	columns := strings.Join(dbcols, sep)

	query := fmt.Sprintf("INSERT INTO %s%s%s (%s%s%s) VALUES (%s)", Q, mi.table, Q, Q, columns, Q, qmarks)

	d.ins.ReplaceMarks(&query)

	d.ins.HasReturningID(mi, &query)

	stmt, err := q.Prepare(query)
	return stmt, query, err
}

// insert struct with prepared statement and given struct reflect value.
func (d *dbBase) InsertStmt(stmt stmtQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location) (int64, error) {
	values, _, err := d.collectValues(mi, ind, mi.fields.dbcols, true, true, nil, tz)
	if err != nil {
		return 0, err
	}

	if d.ins.HasReturningID(mi, nil) {
		row := stmt.QueryRow(values...)
		var id int64
		err := row.Scan(&id)
		return id, err
	}
	res, err := stmt.Exec(values...)
	if err == nil {
		return res.LastInsertId()
	}
	return 0, err
}

// query sql ,read records and persist in dbBaser.
func (d *dbBase) Read(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location, cols []string, isForUpdate bool) error {
	var whereCols []string
	var args []interface{}

	// if specify cols length > 0, then use it for where condition.
	if len(cols) > 0 {
		var err error
		whereCols = make([]string, 0, len(cols))
		args, _, err = d.collectValues(mi, ind, cols, false, false, &whereCols, tz)
		if err != nil {
			return err
		}
	} else {
		// default use pk value as where condtion.
		pkColumn, pkValue, ok := getExistPk(mi, ind)
		if !ok {
			return ErrMissPK
		}
		whereCols = []string{pkColumn}
		args = append(args, pkValue)
	}

	Q := d.ins.TableQuote()

	sep := fmt.Sprintf("%s, %s", Q, Q)
	sels := strings.Join(mi.fields.dbcols, sep)
	colsNum := len(mi.fields.dbcols)

	sep = fmt.Sprintf("%s = ? AND %s", Q, Q)
	wheres := strings.Join(whereCols, sep)

	forUpdate := ""
	if isForUpdate {
		forUpdate = "FOR UPDATE"
	}

	query := fmt.Sprintf("SELECT %s%s%s FROM %s%s%s WHERE %s%s%s = ? %s", Q, sels, Q, Q, mi.table, Q, Q, wheres, Q, forUpdate)

	refs := make([]interface{}, colsNum)
	for i := range refs {
		var ref interface{}
		refs[i] = &ref
	}

	d.ins.ReplaceMarks(&query)

	row := q.QueryRow(query, args...)
	if err := row.Scan(refs...); err != nil {
		if err == sql.ErrNoRows {
			return ErrNoRows
		}
		return err
	}
	elm := reflect.New(mi.addrField.Elem().Type())
	mind := reflect.Indirect(elm)
	d.setColsValues(mi, &mind, mi.fields.dbcols, refs, tz)
	ind.Set(mind)
	return nil
}

// execute insert sql dbQuerier with given struct reflect.Value.
func (d *dbBase) Insert(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location) (int64, error) {
	names := make([]string, 0, len(mi.fields.dbcols))
	values, autoFields, err := d.collectValues(mi, ind, mi.fields.dbcols, false, true, &names, tz)
	if err != nil {
		return 0, err
	}

	id, err := d.InsertValue(q, mi, false, names, values)
	if err != nil {
		return 0, err
	}

	if len(autoFields) > 0 {
		err = d.ins.setval(q, mi, autoFields)
	}
	return id, err
}

// multi-insert sql with given slice struct reflect.Value.
func (d *dbBase) InsertMulti(q dbQuerier, mi *modelInfo, sind reflect.Value, bulk int, tz *time.Location) (int64, error) {
	var (
		cnt    int64
		nums   int
		values []interface{}
		names  []string
	)

	// typ := reflect.Indirect(mi.addrField).Type()

	length, autoFields := sind.Len(), make([]string, 0, 1)

	for i := 1; i <= length; i++ {

		ind := reflect.Indirect(sind.Index(i - 1))

		// Is this needed ?
		// if !ind.Type().AssignableTo(typ) {
		// 	return cnt, ErrArgs
		// }

		if i == 1 {
			var (
				vus []interface{}
				err error
			)
			vus, autoFields, err = d.collectValues(mi, ind, mi.fields.dbcols, false, true, &names, tz)
			if err != nil {
				return cnt, err
			}
			values = make([]interface{}, bulk*len(vus))
			nums += copy(values, vus)
		} else {
			vus, _, err := d.collectValues(mi, ind, mi.fields.dbcols, false, true, nil, tz)
			if err != nil {
				return cnt, err
			}

			if len(vus) != len(names) {
				return cnt, ErrArgs
			}

			nums += copy(values[nums:], vus)
		}

		if i > 1 && i%bulk == 0 || length == i {
			num, err := d.InsertValue(q, mi, true, names, values[:nums])
			if err != nil {
				return cnt, err
			}
			cnt += num
			nums = 0
		}
	}

	var err error
	if len(autoFields) > 0 {
		err = d.ins.setval(q, mi, autoFields)
	}

	return cnt, err
}

// execute insert sql with given struct and given values.
// insert the given values, not the field values in struct.
func (d *dbBase) InsertValue(q dbQuerier, mi *modelInfo, isMulti bool, names []string, values []interface{}) (int64, error) {
	Q := d.ins.TableQuote()

	marks := make([]string, len(names))
	for i := range marks {
		marks[i] = "?"
	}

	sep := fmt.Sprintf("%s, %s", Q, Q)
	qmarks := strings.Join(marks, ", ")
	columns := strings.Join(names, sep)

	multi := len(values) / len(names)

	if isMulti {
		qmarks = strings.Repeat(qmarks+"), (", multi-1) + qmarks
	}

	query := fmt.Sprintf("INSERT INTO %s%s%s (%s%s%s) VALUES (%s)", Q, mi.table, Q, Q, columns, Q, qmarks)

	d.ins.ReplaceMarks(&query)

	if isMulti || !d.ins.HasReturningID(mi, &query) {
		res, err := q.Exec(query, values...)
		if err == nil {
			if isMulti {
				return res.RowsAffected()
			}
			return res.LastInsertId()
		}
		return 0, err
	}
	row := q.QueryRow(query, values...)
	var id int64
	err := row.Scan(&id)
	return id, err
}

// InsertOrUpdate a row
// If your primary key or unique column conflict will update
// If no will insert
func (d *dbBase) InsertOrUpdate(q dbQuerier, mi *modelInfo, ind reflect.Value, a *alias, args ...string) (int64, error) {
	args0 := ""
	iouStr := ""
	argsMap := map[string]string{}
	switch a.Driver {
	case DRMySQL:
		iouStr = "ON DUPLICATE KEY UPDATE"
	case DRPostgres:
		if len(args) == 0 {
			return 0, fmt.Errorf("`%s` use InsertOrUpdate must have a conflict column", a.DriverName)
		}
		args0 = strings.ToLower(args[0])
		iouStr = fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET", args0)
	default:
		return 0, fmt.Errorf("`%s` nonsupport InsertOrUpdate in beego", a.DriverName)
	}

	//Get on the key-value pairs
	for _, v := range args {
		kv := strings.Split(v, "=")
		if len(kv) == 2 {
			argsMap[strings.ToLower(kv[0])] = kv[1]
		}
	}

	isMulti := false
	names := make([]string, 0, len(mi.fields.dbcols)-1)
	Q := d.ins.TableQuote()
	values, _, err := d.collectValues(mi, ind, mi.fields.dbcols, true, true, &names, a.TZ)

	if err != nil {
		return 0, err
	}

	marks := make([]string, len(names))
	updateValues := make([]interface{}, 0)
	updates := make([]string, len(names))
	var conflitValue interface{}
	for i, v := range names {
		marks[i] = "?"
		valueStr := argsMap[strings.ToLower(v)]
		if v == args0 {
			conflitValue = values[i]
		}
		if valueStr != "" {
			switch a.Driver {
			case DRMySQL:
				updates[i] = v + "=" + valueStr
			case DRPostgres:
				if conflitValue != nil {
					//postgres ON CONFLICT DO UPDATE SET can`t use colu=colu+values
					updates[i] = fmt.Sprintf("%s=(select %s from %s where %s = ? )", v, valueStr, mi.table, args0)
					updateValues = append(updateValues, conflitValue)
				} else {
					return 0, fmt.Errorf("`%s` must be in front of `%s` in your struct", args0, v)
				}
			}
		} else {
			updates[i] = v + "=?"
			updateValues = append(updateValues, values[i])
		}
	}

	values = append(values, updateValues...)

	sep := fmt.Sprintf("%s, %s", Q, Q)
	qmarks := strings.Join(marks, ", ")
	qupdates := strings.Join(updates, ", ")
	columns := strings.Join(names, sep)

	multi := len(values) / len(names)

	if isMulti {
		qmarks = strings.Repeat(qmarks+"), (", multi-1) + qmarks
	}
	//conflitValue maybe is a int,can`t use fmt.Sprintf
	query := fmt.Sprintf("INSERT INTO %s%s%s (%s%s%s) VALUES (%s) %s "+qupdates, Q, mi.table, Q, Q, columns, Q, qmarks, iouStr)

	d.ins.ReplaceMarks(&query)

	if isMulti || !d.ins.HasReturningID(mi, &query) {
		res, err := q.Exec(query, values...)
		if err == nil {
			if isMulti {
				return res.RowsAffected()
			}
			return res.LastInsertId()
		}
		return 0, err
	}

	row := q.QueryRow(query, values...)
	var id int64
	err = row.Scan(&id)
	if err != nil && err.Error() == `pq: syntax error at or near "ON"` {
		err = fmt.Errorf("postgres version must 9.5 or higher")
	}
	return id, err
}

// execute update sql dbQuerier with given struct reflect.Value.
func (d *dbBase) Update(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location, cols []string) (int64, error) {
	pkName, pkValue, ok := getExistPk(mi, ind)
	if !ok {
		return 0, ErrMissPK
	}

	var setNames []string

	// if specify cols length is zero, then commit all columns.
	if len(cols) == 0 {
		cols = mi.fields.dbcols
		setNames = make([]string, 0, len(mi.fields.dbcols)-1)
	} else {
		setNames = make([]string, 0, len(cols))
	}

	setValues, _, err := d.collectValues(mi, ind, cols, true, false, &setNames, tz)
	if err != nil {
		return 0, err
	}

	setValues = append(setValues, pkValue)

	Q := d.ins.TableQuote()

	sep := fmt.Sprintf("%s = ?, %s", Q, Q)
	setColumns := strings.Join(setNames, sep)

	query := fmt.Sprintf("UPDATE %s%s%s SET %s%s%s = ? WHERE %s%s%s = ?", Q, mi.table, Q, Q, setColumns, Q, Q, pkName, Q)

	d.ins.ReplaceMarks(&query)

	res, err := q.Exec(query, setValues...)
	if err == nil {
		return res.RowsAffected()
	}
	return 0, err
}

// generate sql with replacing operator string placeholders and replaced values.
func (d *dbBase) GenerateOperatorSQL(mi *modelInfo, fi *fieldInfo, operator string, args []interface{}, tz *time.Location) (string, []interface{}) {
	var sql string
	params := getFlatParams(fi, args, tz)

	if len(params) == 0 {
		panic(fmt.Errorf("operator `%s` need at least one args", operator))
	}
	arg := params[0]

	switch operator {
	case "in":
		marks := make([]string, len(params))
		for i := range marks {
			marks[i] = "?"
		}
		sql = fmt.Sprintf("IN (%s)", strings.Join(marks, ", "))
	case "between":
		if len(params) != 2 {
			panic(fmt.Errorf("operator `%s` need 2 args not %d", operator, len(params)))
		}
		sql = "BETWEEN ? AND ?"
	default:
		if len(params) > 1 {
			panic(fmt.Errorf("operator `%s` need 1 args not %d", operator, len(params)))
		}
		sql = d.ins.OperatorSQL(operator)
		switch operator {
		case "exact":
			if arg == nil {
				params[0] = "IS NULL"
			}
		case "iexact", "contains", "icontains", "startswith", "endswith", "istartswith", "iendswith":
			param := strings.Replace(ToStr(arg), `%`, `\%`, -1)
			switch operator {
			case "iexact":
			case "contains", "icontains":
				param = fmt.Sprintf("%%%s%%", param)
			case "startswith", "istartswith":
				param = fmt.Sprintf("%s%%", param)
			case "endswith", "iendswith":
				param = fmt.Sprintf("%%%s", param)
			}
			params[0] = param
		case "isnull":
			if b, ok := arg.(bool); ok {
				if b {
					sql = "IS NULL"
				} else {
					sql = "IS NOT NULL"
				}
				params = nil
			} else {
				panic(fmt.Errorf("operator `%s` need a bool value not `%T`", operator, arg))
			}
		}
	}
	return sql, params
}

// gernerate sql string with inner function, such as UPPER(text).
func (d *dbBase) GenerateOperatorLeftCol(*fieldInfo, string, *string) {
	// default not use
}

// set values to struct column.
func (d *dbBase) setColsValues(mi *modelInfo, ind *reflect.Value, cols []string, values []interface{}, tz *time.Location) {
	for i, column := range cols {
		val := reflect.Indirect(reflect.ValueOf(values[i])).Interface()

		fi := mi.fields.GetByColumn(column)

		field := ind.FieldByIndex(fi.fieldIndex)

		value, err := d.convertValueFromDB(fi, val, tz)
		if err != nil {
			panic(fmt.Errorf("Raw value: `%v` %s", val, err.Error()))
		}

		_, err = d.setFieldValue(fi, value, field)

		if err != nil {
			panic(fmt.Errorf("Raw value: `%v` %s", val, err.Error()))
		}
	}
}

// convert value from database result to value following in field type.
func (d *dbBase) convertValueFromDB(fi *fieldInfo, val interface{}, tz *time.Location) (interface{}, error) {
	if val == nil {
		return nil, nil
	}

	var value interface{}
	var tErr error

	var str *StrTo
	switch v := val.(type) {
	case []byte:
		s := StrTo(string(v))
		str = &s
	case string:
		s := StrTo(v)
		str = &s
	}

	fieldType := fi.fieldType

setValue:
	switch {
	case fieldType == TypeBooleanField:
		if str == nil {
			switch v := val.(type) {
			case int64:
				b := v == 1
				value = b
			default:
				s := StrTo(ToStr(v))
				str = &s
			}
		}
		if str != nil {
			b, err := str.Bool()
			if err != nil {
				tErr = err
				goto end
			}
			value = b
		}
	case fieldType == TypeCharField || fieldType == TypeTextField || fieldType == TypeJSONField || fieldType == TypeJsonbField:
		if str == nil {
			value = ToStr(val)
		} else {
			value = str.String()
		}
	case fieldType == TypeTimeField || fieldType == TypeDateField || fieldType == TypeDateTimeField:
		if str == nil {
			switch t := val.(type) {
			case time.Time:
				d.ins.TimeFromDB(&t, tz)
				value = t
			default:
				s := StrTo(ToStr(t))
				str = &s
			}
		}
		if str != nil {
			s := str.String()
			var (
				t   time.Time
				err error
			)
			if len(s) >= 19 {
				s = s[:19]
				t, err = time.ParseInLocation(formatDateTime, s, tz)
			} else if len(s) >= 10 {
				if len(s) > 10 {
					s = s[:10]
				}
				t, err = time.ParseInLocation(formatDate, s, tz)
			} else if len(s) >= 8 {
				if len(s) > 8 {
					s = s[:8]
				}
				t, err = time.ParseInLocation(formatTime, s, tz)
			}
			t = t.In(DefaultTimeLoc)

			if err != nil && s != "00:00:00" && s != "0000-00-00" && s != "0000-00-00 00:00:00" {
				tErr = err
				goto end
			}
			value = t
		}
	case fieldType&IsIntegerField > 0:
		if str == nil {
			s := StrTo(ToStr(val))
			str = &s
		}
		if str != nil {
			var err error
			switch fieldType {
			case TypeBitField:
				_, err = str.Int8()
			case TypeSmallIntegerField:
				_, err = str.Int16()
			case TypeIntegerField:
				_, err = str.Int32()
			case TypeBigIntegerField:
				_, err = str.Int64()
			case TypePositiveBitField:
				_, err = str.Uint8()
			case TypePositiveSmallIntegerField:
				_, err = str.Uint16()
			case TypePositiveIntegerField:
				_, err = str.Uint32()
			case TypePositiveBigIntegerField:
				_, err = str.Uint64()
			}
			if err != nil {
				tErr = err
				goto end
			}
			if fieldType&IsPositiveIntegerField > 0 {
				v, _ := str.Uint64()
				value = v
			} else {
				v, _ := str.Int64()
				value = v
			}
		}
	case fieldType == TypeFloatField || fieldType == TypeDecimalField:
		if str == nil {
			switch v := val.(type) {
			case float64:
				value = v
			default:
				s := StrTo(ToStr(v))
				str = &s
			}
		}
		if str != nil {
			v, err := str.Float64()
			if err != nil {
				tErr = err
				goto end
			}
			value = v
		}
	case fieldType&IsRelField > 0:
		fi = fi.relModelInfo.fields.pk
		fieldType = fi.fieldType
		goto setValue
	}

end:
	if tErr != nil {
		err := fmt.Errorf("convert to `%s` failed, field: %s err: %s", fi.addrValue.Type(), fi.fullName, tErr)
		return nil, err
	}

	return value, nil

}

// set one value to struct column field.
func (d *dbBase) setFieldValue(fi *fieldInfo, value interface{}, field reflect.Value) (interface{}, error) {

	fieldType := fi.fieldType
	isNative := !fi.isFielder

setValue:
	switch {
	case fieldType == TypeBooleanField:
		if isNative {
			if nb, ok := field.Interface().(sql.NullBool); ok {
				if value == nil {
					nb.Valid = false
				} else {
					nb.Bool = value.(bool)
					nb.Valid = true
				}
				field.Set(reflect.ValueOf(nb))
			} else if field.Kind() == reflect.Ptr {
				if value != nil {
					v := value.(bool)
					field.Set(reflect.ValueOf(&v))
				}
			} else {
				if value == nil {
					value = false
				}
				field.SetBool(value.(bool))
			}
		}
	case fieldType == TypeCharField || fieldType == TypeTextField || fieldType == TypeJSONField || fieldType == TypeJsonbField:
		if isNative {
			if ns, ok := field.Interface().(sql.NullString); ok {
				if value == nil {
					ns.Valid = false
				} else {
					ns.String = value.(string)
					ns.Valid = true
				}
				field.Set(reflect.ValueOf(ns))
			} else if field.Kind() == reflect.Ptr {
				if value != nil {
					v := value.(string)
					field.Set(reflect.ValueOf(&v))
				}
			} else {
				if value == nil {
					value = ""
				}
				field.SetString(value.(string))
			}
		}
	case fieldType == TypeTimeField || fieldType == TypeDateField || fieldType == TypeDateTimeField:
		if isNative {
			if value == nil {
				value = time.Time{}
			} else if field.Kind() == reflect.Ptr {
				if value != nil {
					v := value.(time.Time)
					field.Set(reflect.ValueOf(&v))
				}
			} else {
				field.Set(reflect.ValueOf(value))
			}
		}
	case fieldType == TypePositiveBitField && field.Kind() == reflect.Ptr:
		if value != nil {
			v := uint8(value.(uint64))
			field.Set(reflect.ValueOf(&v))
		}
	case fieldType == TypePositiveSmallIntegerField && field.Kind() == reflect.Ptr:
		if value != nil {
			v := uint16(value.(uint64))
			field.Set(reflect.ValueOf(&v))
		}
	case fieldType == TypePositiveIntegerField && field.Kind() == reflect.Ptr:
		if value != nil {
			if field.Type() == reflect.TypeOf(new(uint)) {
				v := uint(value.(uint64))
				field.Set(reflect.ValueOf(&v))
			} else {
				v := uint32(value.(uint64))
				field.Set(reflect.ValueOf(&v))
			}
		}
	case fieldType == TypePositiveBigIntegerField && field.Kind() == reflect.Ptr:
		if value != nil {
			v := value.(uint64)
			field.Set(reflect.ValueOf(&v))
		}
	case fieldType == TypeBitField && field.Kind() == reflect.Ptr:
		if value != nil {
			v := int8(value.(int64))
			field.Set(reflect.ValueOf(&v))
		}
	case fieldType == TypeSmallIntegerField && field.Kind() == reflect.Ptr:
		if value != nil {
			v := int16(value.(int64))
			field.Set(reflect.ValueOf(&v))
		}
	case fieldType == TypeIntegerField && field.Kind() == reflect.Ptr:
		if value != nil {
			if field.Type() == reflect.TypeOf(new(int)) {
				v := int(value.(int64))
				field.Set(reflect.ValueOf(&v))
			} else {
				v := int32(value.(int64))
				field.Set(reflect.ValueOf(&v))
			}
		}
	case fieldType == TypeBigIntegerField && field.Kind() == reflect.Ptr:
		if value != nil {
			v := value.(int64)
			field.Set(reflect.ValueOf(&v))
		}
	case fieldType&IsIntegerField > 0:
		if fieldType&IsPositiveIntegerField > 0 {
			if isNative {
				if value == nil {
					value = uint64(0)
				}
				field.SetUint(value.(uint64))
			}
		} else {
			if isNative {
				if ni, ok := field.Interface().(sql.NullInt64); ok {
					if value == nil {
						ni.Valid = false
					} else {
						ni.Int64 = value.(int64)
						ni.Valid = true
					}
					field.Set(reflect.ValueOf(ni))
				} else {
					if value == nil {
						value = int64(0)
					}
					field.SetInt(value.(int64))
				}
			}
		}
	case fieldType == TypeFloatField || fieldType == TypeDecimalField:
		if isNative {
			if nf, ok := field.Interface().(sql.NullFloat64); ok {
				if value == nil {
					nf.Valid = false
				} else {
					nf.Float64 = value.(float64)
					nf.Valid = true
				}
				field.Set(reflect.ValueOf(nf))
			} else if field.Kind() == reflect.Ptr {
				if value != nil {
					if field.Type() == reflect.TypeOf(new(float32)) {
						v := float32(value.(float64))
						field.Set(reflect.ValueOf(&v))
					} else {
						v := value.(float64)
						field.Set(reflect.ValueOf(&v))
					}
				}
			} else {

				if value == nil {
					value = float64(0)
				}
				field.SetFloat(value.(float64))
			}
		}
	case fieldType&IsRelField > 0:
		if value != nil {
			fieldType = fi.relModelInfo.fields.pk.fieldType
			mf := reflect.New(fi.relModelInfo.addrField.Elem().Type())
			field.Set(mf)
			f := mf.Elem().FieldByIndex(fi.relModelInfo.fields.pk.fieldIndex)
			field = f
			goto setValue
		}
	}

	if !isNative {
		fd := field.Addr().Interface().(Fielder)
		err := fd.SetRaw(value)
		if err != nil {
			err = fmt.Errorf("converted value `%v` set to Fielder `%s` failed, err: %s", value, fi.fullName, err)
			return nil, err
		}
	}

	return value, nil
}

// query sql, read values , save to *[]ParamList.

// flag of update joined record.
func (d *dbBase) SupportUpdateJoin() bool {
	return true
}

func (d *dbBase) MaxLimit() uint64 {
	return 18446744073709551615
}

// return quote.
func (d *dbBase) TableQuote() string {
	return "`"
}

// replace value placeholder in parametered sql string.
func (d *dbBase) ReplaceMarks(query *string) {
	// default use `?` as mark, do nothing
}

// flag of RETURNING sql.
func (d *dbBase) HasReturningID(*modelInfo, *string) bool {
	return false
}

// sync auto key
func (d *dbBase) setval(db dbQuerier, mi *modelInfo, autoFields []string) error {
	return nil
}

// convert time from db.
func (d *dbBase) TimeFromDB(t *time.Time, tz *time.Location) {
	*t = t.In(tz)
}

// convert time to db.
func (d *dbBase) TimeToDB(t *time.Time, tz *time.Location) {
	*t = t.In(tz)
}

// get database types.
func (d *dbBase) DbTypes() map[string]string {
	return nil
}

// gt all tables.
func (d *dbBase) GetTables(db dbQuerier) (map[string]bool, error) {
	tables := make(map[string]bool)
	query := d.ins.ShowTablesQuery()
	rows, err := db.Query(query)
	if err != nil {
		return tables, err
	}

	defer rows.Close()

	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			return tables, err
		}
		if table != "" {
			tables[table] = true
		}
	}

	return tables, nil
}

// get all cloumns in table.
func (d *dbBase) GetColumns(db dbQuerier, table string) (map[string][3]string, error) {
	columns := make(map[string][3]string)
	query := d.ins.ShowColumnsQuery(table)
	rows, err := db.Query(query)
	if err != nil {
		return columns, err
	}

	defer rows.Close()

	for rows.Next() {
		var (
			name string
			typ  string
			null string
		)
		err := rows.Scan(&name, &typ, &null)
		if err != nil {
			return columns, err
		}
		columns[name] = [3]string{name, typ, null}
	}

	return columns, nil
}

// not implement.
func (d *dbBase) OperatorSQL(operator string) string {
	panic(ErrNotImplement)
}

// not implement.
func (d *dbBase) ShowTablesQuery() string {
	panic(ErrNotImplement)
}

// not implement.
func (d *dbBase) ShowColumnsQuery(table string) string {
	panic(ErrNotImplement)
}

// not implement.
func (d *dbBase) IndexExists(dbQuerier, string, string) bool {
	panic(ErrNotImplement)
}

// get table alias.
func getDbAlias(name string) *alias {
	if al, ok := dataBaseCache.get(name); ok {
		return al
	}
	panic(fmt.Errorf("unknown DataBase alias name %s", name))
}

// get pk column info.
func getExistPk(mi *modelInfo, ind reflect.Value) (column string, value interface{}, exist bool) {
	fi := mi.fields.pk

	v := ind.FieldByIndex(fi.fieldIndex)
	if fi.fieldType&IsPositiveIntegerField > 0 {
		vu := v.Uint()
		exist = vu > 0
		value = vu
	} else if fi.fieldType&IsIntegerField > 0 {
		vu := v.Int()
		exist = true
		value = vu
	} else if fi.fieldType&IsRelField > 0 {
		_, value, exist = getExistPk(fi.relModelInfo, reflect.Indirect(v))
	} else {
		vu := v.String()
		exist = vu != ""
		value = vu
	}

	column = fi.column
	return
}

// get fields description as flatted string.
func getFlatParams(fi *fieldInfo, args []interface{}, tz *time.Location) (params []interface{}) {

outFor:
	for _, arg := range args {
		val := reflect.ValueOf(arg)

		if arg == nil {
			params = append(params, arg)
			continue
		}

		kind := val.Kind()
		if kind == reflect.Ptr {
			val = val.Elem()
			kind = val.Kind()
			arg = val.Interface()
		}

		switch kind {
		case reflect.String:
			v := val.String()
			if fi != nil {
				if fi.fieldType == TypeTimeField || fi.fieldType == TypeDateField || fi.fieldType == TypeDateTimeField {
					var t time.Time
					var err error
					if len(v) >= 19 {
						s := v[:19]
						t, err = time.ParseInLocation(formatDateTime, s, DefaultTimeLoc)
					} else if len(v) >= 10 {
						s := v
						if len(v) > 10 {
							s = v[:10]
						}
						t, err = time.ParseInLocation(formatDate, s, tz)
					} else {
						s := v
						if len(s) > 8 {
							s = v[:8]
						}
						t, err = time.ParseInLocation(formatTime, s, tz)
					}
					if err == nil {
						if fi.fieldType == TypeDateField {
							v = t.In(tz).Format(formatDate)
						} else if fi.fieldType == TypeDateTimeField {
							v = t.In(tz).Format(formatDateTime)
						} else {
							v = t.In(tz).Format(formatTime)
						}
					}
				}
			}
			arg = v
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			arg = val.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			arg = val.Uint()
		case reflect.Float32:
			arg, _ = StrTo(ToStr(arg)).Float64()
		case reflect.Float64:
			arg = val.Float()
		case reflect.Bool:
			arg = val.Bool()
		case reflect.Slice, reflect.Array:
			if _, ok := arg.([]byte); ok {
				continue outFor
			}

			var args []interface{}
			for i := 0; i < val.Len(); i++ {
				v := val.Index(i)

				var vu interface{}
				if v.CanInterface() {
					vu = v.Interface()
				}

				if vu == nil {
					continue
				}

				args = append(args, vu)
			}

			if len(args) > 0 {
				p := getFlatParams(fi, args, tz)
				params = append(params, p...)
			}
			continue outFor
		case reflect.Struct:
			if v, ok := arg.(time.Time); ok {
				if fi != nil && fi.fieldType == TypeDateField {
					arg = v.In(tz).Format(formatDate)
				} else if fi != nil && fi.fieldType == TypeDateTimeField {
					arg = v.In(tz).Format(formatDateTime)
				} else if fi != nil && fi.fieldType == TypeTimeField {
					arg = v.In(tz).Format(formatTime)
				} else {
					arg = v.In(tz).Format(formatDateTime)
				}
			} else {
				typ := val.Type()
				name := getFullName(typ)
				var value interface{}
				if mmi, ok := modelCache.getByFullName(name); ok {
					if _, vu, exist := getExistPk(mmi, val); exist {
						value = vu
					}
				}
				arg = value

				if arg == nil {
					panic(fmt.Errorf("need a valid args value, unknown table or value `%s`", name))
				}
			}
		}

		params = append(params, arg)
	}
	return
}

// DriverType database driver constant int.
type DriverType int

// Enum the Database driver
const (
	_          DriverType = iota // int enum type
	DRMySQL                      // mysql
	DRSqlite                     // sqlite
	DROracle                     // oracle
	DRPostgres                   // pgsql
	DRTiDB                       // TiDB
)

// database driver string.
type driver string

// get type constant int of current driver..
func (d driver) Type() DriverType {
	a, _ := dataBaseCache.get(string(d))
	return a.Driver
}

// get name of current driver
func (d driver) Name() string {
	return string(d)
}

// check driver iis implemented Driver interface or not.
var _ Driver = new(driver)

var (
	dataBaseCache = &_dbCache{cache: make(map[string]*alias)}
	drivers       = map[string]DriverType{
		"mysql":    DRMySQL,
		"postgres": DRPostgres,
		"sqlite3":  DRSqlite,
		"tidb":     DRTiDB,
		"oracle":   DROracle,
		"oci8":     DROracle, // github.com/mattn/go-oci8
		"ora":      DROracle, //https://github.com/rana/ora
	}
	dbBasers = map[DriverType]dbBaser{
		DRMySQL:    newdbBaseMysql(),
		DRSqlite:   newdbBaseSqlite(),
		DROracle:   newdbBaseOracle(),
		DRPostgres: newdbBasePostgres(),
		DRTiDB:     newdbBaseTidb(),
	}
)

// database alias cacher.
type _dbCache struct {
	mux   sync.RWMutex
	cache map[string]*alias
}

// add database alias with original name.
func (ac *_dbCache) add(name string, al *alias) (added bool) {
	ac.mux.Lock()
	defer ac.mux.Unlock()
	if _, ok := ac.cache[name]; !ok {
		ac.cache[name] = al
		added = true
	}
	return
}

// get database alias if cached.
func (ac *_dbCache) get(name string) (al *alias, ok bool) {
	ac.mux.RLock()
	defer ac.mux.RUnlock()
	al, ok = ac.cache[name]
	return
}

// get default alias.
func (ac *_dbCache) getDefault() (al *alias) {
	al, _ = ac.get("default")
	return
}

type alias struct {
	Name         string
	Driver       DriverType
	DriverName   string
	DataSource   string
	MaxIdleConns int
	MaxOpenConns int
	DB           *sql.DB
	DbBaser      dbBaser
	TZ           *time.Location
	Engine       string
}

func detectTZ(al *alias) {
	// orm timezone system match database
	// default use Local
	al.TZ = time.Local

	if al.DriverName == "sphinx" {
		return
	}

	switch al.Driver {
	case DRMySQL:
		row := al.DB.QueryRow("SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP)")
		var tz string
		row.Scan(&tz)
		if len(tz) >= 8 {
			if tz[0] != '-' {
				tz = "+" + tz
			}
			t, err := time.Parse("-07:00:00", tz)
			if err == nil {
				al.TZ = t.Location()
			} else {

			}
		}

		// get default engine from current database
		row = al.DB.QueryRow("SELECT ENGINE, TRANSACTIONS FROM information_schema.engines WHERE SUPPORT = 'DEFAULT'")
		var engine string
		var tx bool
		row.Scan(&engine, &tx)

		if engine != "" {
			al.Engine = engine
		} else {
			al.Engine = "INNODB"
		}

	case DRSqlite, DROracle:
		al.TZ = time.UTC

	case DRPostgres:
		row := al.DB.QueryRow("SELECT current_setting('TIMEZONE')")
		var tz string
		row.Scan(&tz)
		loc, err := time.LoadLocation(tz)
		if err == nil {
			al.TZ = loc
		} else {

		}
	}
}

func addAliasWthDB(aliasName, driverName string, db *sql.DB) (*alias, error) {
	al := new(alias)
	al.Name = aliasName
	al.DriverName = driverName
	al.DB = db

	if dr, ok := drivers[driverName]; ok {
		al.DbBaser = dbBasers[dr]
		al.Driver = dr
	} else {
		return nil, fmt.Errorf("driver name `%s` have not registered", driverName)
	}

	err := db.Ping()
	if err != nil {
		return nil, fmt.Errorf("register db Ping `%s`, %s", aliasName, err.Error())
	}

	if !dataBaseCache.add(aliasName, al) {
		return nil, fmt.Errorf("DataBase alias name `%s` already registered, cannot reuse", aliasName)
	}

	return al, nil
}

// AddAliasWthDB add a aliasName for the drivename
func AddAliasWthDB(aliasName, driverName string, db *sql.DB) error {
	_, err := addAliasWthDB(aliasName, driverName, db)
	return err
}

// RegisterDataBase Setting the database connect params. Use the database driver self dataSource args.
func RegisterDataBase(aliasName, driverName, dataSource string, params ...int) error {
	var (
		err error
		db  *sql.DB
		al  *alias
	)

	db, err = sql.Open(driverName, dataSource)
	if err != nil {
		err = fmt.Errorf("register db `%s`, %s", aliasName, err.Error())
		goto end
	}

	al, err = addAliasWthDB(aliasName, driverName, db)
	if err != nil {
		goto end
	}

	al.DataSource = dataSource

	detectTZ(al)

	for i, v := range params {
		switch i {
		case 0:
			SetMaxIdleConns(al.Name, v)
		case 1:
			SetMaxOpenConns(al.Name, v)
		}
	}

end:
	if err != nil {
		if db != nil {
			db.Close()
		}

	}

	return err
}

// RegisterDriver Register a database driver use specify driver name, this can be definition the driver is which database type.
func RegisterDriver(driverName string, typ DriverType) error {
	if t, ok := drivers[driverName]; !ok {
		drivers[driverName] = typ
	} else {
		if t != typ {
			return fmt.Errorf("driverName `%s` db driver already registered and is other type", driverName)
		}
	}
	return nil
}

// SetDataBaseTZ Change the database default used timezone
func SetDataBaseTZ(aliasName string, tz *time.Location) error {
	if al, ok := dataBaseCache.get(aliasName); ok {
		al.TZ = tz
	} else {
		return fmt.Errorf("DataBase alias name `%s` not registered", aliasName)
	}
	return nil
}

// SetMaxIdleConns Change the max idle conns for *sql.DB, use specify database alias name
func SetMaxIdleConns(aliasName string, maxIdleConns int) {
	al := getDbAlias(aliasName)
	al.MaxIdleConns = maxIdleConns
	al.DB.SetMaxIdleConns(maxIdleConns)
}

// SetMaxOpenConns Change the max open conns for *sql.DB, use specify database alias name
func SetMaxOpenConns(aliasName string, maxOpenConns int) {
	al := getDbAlias(aliasName)
	al.MaxOpenConns = maxOpenConns
	// for tip go 1.2
	if fun := reflect.ValueOf(al.DB).MethodByName("SetMaxOpenConns"); fun.IsValid() {
		fun.Call([]reflect.Value{reflect.ValueOf(maxOpenConns)})
	}
}

// GetDB Get *sql.DB from registered database by db alias name.
// Use "default" as alias name if you not set.
func GetDB(aliasNames ...string) (*sql.DB, error) {
	var name string
	if len(aliasNames) > 0 {
		name = aliasNames[0]
	} else {
		name = "default"
	}
	al, ok := dataBaseCache.get(name)
	if ok {
		return al.DB, nil
	}
	return nil, fmt.Errorf("DataBase of alias name `%s` not found", name)
}

// mysql operators.
var mysqlOperators = map[string]string{}

// mysql dbBaser implementation.
type dbBaseMysql struct {
	dbBase
}

var _ dbBaser = new(dbBaseMysql)

// get mysql operator.
func (d *dbBaseMysql) OperatorSQL(operator string) string {
	return mysqlOperators[operator]
}

// create new mysql dbBaser.
func newdbBaseMysql() dbBaser {
	b := new(dbBaseMysql)
	b.ins = b
	return b
}

// oracle dbBaser
type dbBaseOracle struct {
	dbBase
}

var _ dbBaser = new(dbBaseOracle)

// create oracle dbBaser.
func newdbBaseOracle() dbBaser {
	b := new(dbBaseOracle)
	b.ins = b
	return b
}

type dbBaseSqlite struct {
	dbBase
}

var _ dbBaser = new(dbBaseSqlite)

func (d *dbBaseSqlite) GenerateOperatorLeftCol(fi *fieldInfo, operator string, leftCol *string) {
	if fi.fieldType == TypeDateField {
		*leftCol = fmt.Sprintf("DATE(%s)", *leftCol)
	}
}

func newdbBaseSqlite() dbBaser {
	b := new(dbBaseSqlite)
	b.ins = b
	return b
}

type dbBasePostgres struct {
	dbBase
}

var _ dbBaser = new(dbBasePostgres)

// create new postgresql dbBaser.
func newdbBasePostgres() dbBaser {
	b := new(dbBasePostgres)
	b.ins = b
	return b
}

type dbBaseTidb struct {
	dbBase
}

// create new mysql dbBaser.
func newdbBaseTidb() dbBaser {
	b := new(dbBaseTidb)
	b.ins = b
	return b
}
