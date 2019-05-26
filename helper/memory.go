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

package helper

import (
	"database/sql"
	"errors"
	"reflect"
)

//NewMap
func NewMap(dst interface{}) error {
	dscValue := reflect.ValueOf(dst)
	if dscValue.Elem().Kind() != reflect.Map {
		return errors.New("dst error")
	}
	result := reflect.MakeMap(reflect.TypeOf(dst).Elem())
	dscValue.Elem().Set(result)
	return nil
}

// Memcpy
func Memcpy(dest interface{}, src interface{}) (err error) {
	var (
		isSlice  bool
		sliceLen = 1
		srcVal   = indirect(reflect.ValueOf(src))
		destVal  = indirect(reflect.ValueOf(dest))
	)

	if !destVal.CanAddr() {
		return errors.New("copy dest value is unaddressable")
	}

	if !srcVal.IsValid() {
		return
	}

	if srcVal.Type().AssignableTo(destVal.Type()) {
		destVal.Set(srcVal)
		return
	}

	srcValType := indirectType(srcVal.Type())
	destType := indirectType(destVal.Type())

	if srcValType.Kind() != reflect.Struct || destType.Kind() != reflect.Struct {
		return
	}

	if destVal.Kind() == reflect.Slice {
		isSlice = true
		if srcVal.Kind() == reflect.Slice {
			sliceLen = srcVal.Len()
		}
	}

	for i := 0; i < sliceLen; i++ {
		var dest, source reflect.Value

		if isSlice {
			// source
			if srcVal.Kind() == reflect.Slice {
				source = indirect(srcVal.Index(i))
			} else {
				source = indirect(srcVal)
			}

			// dest
			dest = indirect(reflect.New(destType).Elem())
		} else {
			source = indirect(srcVal)
			dest = indirect(destVal)
		}

		for _, field := range deepFields(srcValType) {
			name := field.Name

			if srcValField := source.FieldByName(name); srcValField.IsValid() {
				if toField := dest.FieldByName(name); toField.IsValid() {
					if toField.CanSet() {
						if !set(toField, srcValField) {
							if err := Memcpy(toField.Addr().Interface(), srcValField.Interface()); err != nil {
								return err
							}
						}
					}
				} else {
					var toMethod reflect.Value
					if dest.CanAddr() {
						toMethod = dest.Addr().MethodByName(name)
					} else {
						toMethod = dest.MethodByName(name)
					}

					if toMethod.IsValid() && toMethod.Type().NumIn() == 1 && srcValField.Type().AssignableTo(toMethod.Type().In(0)) {
						toMethod.Call([]reflect.Value{srcValField})
					}
				}
			}
		}

		for _, field := range deepFields(destType) {
			name := field.Name

			var srcValMethod reflect.Value
			if source.CanAddr() {
				srcValMethod = source.Addr().MethodByName(name)
			} else {
				srcValMethod = source.MethodByName(name)
			}

			if srcValMethod.IsValid() && srcValMethod.Type().NumIn() == 0 && srcValMethod.Type().NumOut() == 1 {
				if toField := dest.FieldByName(name); toField.IsValid() && toField.CanSet() {
					values := srcValMethod.Call([]reflect.Value{})
					if len(values) >= 1 {
						set(toField, values[0])
					}
				}
			}
		}

		if isSlice {
			if dest.Addr().Type().AssignableTo(destVal.Type().Elem()) {
				destVal.Set(reflect.Append(destVal, dest.Addr()))
			} else if dest.Type().AssignableTo(destVal.Type().Elem()) {
				destVal.Set(reflect.Append(destVal, dest))
			}
		}
	}
	return
}

func deepFields(reflectType reflect.Type) []reflect.StructField {
	var fields []reflect.StructField

	if reflectType = indirectType(reflectType); reflectType.Kind() == reflect.Struct {
		for i := 0; i < reflectType.NumField(); i++ {
			v := reflectType.Field(i)
			if v.Anonymous {
				fields = append(fields, deepFields(v.Type)...)
			} else {
				fields = append(fields, v)
			}
		}
	}

	return fields
}

func indirect(reflectValue reflect.Value) reflect.Value {
	for reflectValue.Kind() == reflect.Ptr {
		reflectValue = reflectValue.Elem()
	}
	return reflectValue
}

func indirectType(reflectType reflect.Type) reflect.Type {
	for reflectType.Kind() == reflect.Ptr || reflectType.Kind() == reflect.Slice {
		reflectType = reflectType.Elem()
	}
	return reflectType
}

func set(destVal, srcVal reflect.Value) bool {
	if srcVal.IsValid() {
		if destVal.Kind() == reflect.Ptr {
			if srcVal.Kind() == reflect.Ptr && srcVal.IsNil() {
				destVal.Set(reflect.Zero(destVal.Type()))
				return true
			} else if destVal.IsNil() {
				destVal.Set(reflect.New(destVal.Type().Elem()))
			}
			destVal = destVal.Elem()
		}

		if srcVal.Type().ConvertibleTo(destVal.Type()) {
			destVal.Set(srcVal.Convert(destVal.Type()))
		} else if scanner, ok := destVal.Addr().Interface().(sql.Scanner); ok {
			err := scanner.Scan(srcVal.Interface())
			if err != nil {
				return false
			}
		} else if srcVal.Kind() == reflect.Ptr {
			return set(destVal, srcVal.Elem())
		} else {
			return false
		}
	}
	return true
}
