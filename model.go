package qbs

import (
	"bytes"
	"database/sql"
	"reflect"
	"strconv"
	"strings"
	"time"
)

//convert struct field name to column name.
var FieldNameToColumnName func(string) string = toSnake

//convert struct name to table name.
var StructNameToTableName func(string) string = toSnake

//onvert column name to struct field name.
var ColumnNameToFieldName func(string) string = snakeToUpperCamel

//convert table name to struct name.
var TableNameToStructName func(string) string = snakeToUpperCamel

// Index represents a table index and is returned via the Indexed interface.
type Index struct {
	Name    string
	Columns []string
	Unique  bool
}

// Indexes represents an array of indexes.
type Indexes []*Index

type Indexed interface {
	Indexes(indexes *Indexes)
}

// Add adds an index
func (ix *Indexes) Add(columns ...string) {
	name := strings.Join(columns, "_")
	*ix = append(*ix, &Index{Name: name, Columns: columns, Unique: false})
}

// AddUnique adds an unique index
func (ix *Indexes) AddUnique(columns ...string) {
	name := strings.Join(columns, "_")
	*ix = append(*ix, &Index{Name: name, Columns: columns, Unique: true})
}

// ModelField represents a schema field of a parsed model.
type ModelField struct {
	Name      string // Column name
	CamelName string
	value     interface{} // Value
	pk        bool
	notnull   bool
	index     bool
	unique    bool
	updated   bool
	created   bool
	size      int
	dfault    string
	fk        string
	join      string
}

// Model represents a parsed schema interface{}.
type Model struct {
	Pk      *ModelField
	Table   string
	Fields  []*ModelField
	Refs    map[string]*Reference
	Indexes Indexes
}

type Reference struct {
	RefKey     string
	Model      *Model
	ForeignKey bool
}

func (model *Model) columnsAndValues(forUpdate bool) ([]string, []interface{}) {
	columns := make([]string, 0, len(model.Fields))
	values := make([]interface{}, 0, len(columns))
	for _, column := range model.Fields {
		var include bool
		if forUpdate {
			include = column.value != nil && !column.pk
		} else {
			include = true
			if column.value == nil {
				include = false
			} else if column.pk {
				if intValue, ok := column.value.(int64); ok {
					include = intValue != 0
				} else if strValue, ok := column.value.(string); ok {
					include = strValue != ""
				}
			}
		}
		if include {
			columns = append(columns, column.Name)
			values = append(values, column.value)
		}
	}
	return columns, values
}

func (model *Model) timeField(name string) *ModelField {
	for _, v := range model.Fields {
		if _, ok := v.value.(time.Time); ok {
			if name == "created" {
				if v.created {
					return v
				}
			} else if name == "updated" {
				if v.updated {
					return v
				}
			}
			if v.Name == name {
				return v
			}
		}
	}
	return nil
}

func (model *Model) pkZero() bool {
	if model.Pk == nil {
		return true
	}
	switch model.Pk.value.(type) {
	case string:
		return model.Pk.value.(string) == ""
	case int8:
		return model.Pk.value.(int8) == 0
	case int16:
		return model.Pk.value.(int16) == 0
	case int32:
		return model.Pk.value.(int32) == 0
	case int64:
		return model.Pk.value.(int64) == 0
	case uint8:
		return model.Pk.value.(uint8) == 0
	case uint16:
		return model.Pk.value.(uint16) == 0
	case uint32:
		return model.Pk.value.(uint32) == 0
	case uint64:
		return model.Pk.value.(uint64) == 0
	}
	return true
}

func StructPtrToModel(f interface{}, root bool, omitFields []string) *Model {
	model := &Model{
		Pk:      nil,
		Table:   tableName(f),
		Fields:  []*ModelField{},
		Indexes: Indexes{},
	}
	structType := reflect.TypeOf(f).Elem()
	structValue := reflect.ValueOf(f).Elem()
	for i := 0; i < structType.NumField(); i++ {
		structField := structType.Field(i)
		omit := false
		for _, v := range omitFields {
			if v == structField.Name {
				omit = true
			}
		}
		if omit {
			continue
		}
		fieldValue := structValue.FieldByName(structField.Name)
		if !fieldValue.CanInterface() {
			continue
		}
		sqlTag := structField.Tag.Get("qbs")
		if sqlTag == "-" {
			continue
		}
		kind := structField.Type.Kind()
		switch kind {
		case reflect.Ptr, reflect.Map:
			continue
		case reflect.Slice:
			elemKind := structField.Type.Elem().Kind()
			if elemKind != reflect.Uint8 {
				continue
			}
		}

		fd := new(ModelField)
		parseTags(fd, sqlTag)
		fd.CamelName = structField.Name
		fd.Name = FieldNameToColumnName(structField.Name)
		fd.value = fieldValue.Interface()
		if _, ok := fd.value.(int64); ok && fd.CamelName == "Id" {
			fd.pk = true
		}
		if fd.pk {
			model.Pk = fd
		}
		model.Fields = append(model.Fields, fd)
		// fill in references map only in root model.
		if root {
			var fk, explicitJoin, implicitJoin bool
			var refName string
			if fd.fk != "" {
				refName = fd.fk
				fk = true
			} else if fd.join != "" {
				refName = fd.join
				explicitJoin = true
			}
			if len(fd.CamelName) > 3 && strings.HasSuffix(fd.CamelName, "Id") {
				fdValue := reflect.ValueOf(fd.value)
				if _, ok := fd.value.(sql.NullInt64); ok || fdValue.Kind() == reflect.Int64 {
					i := strings.LastIndex(fd.CamelName, "Id")
					refName = fd.CamelName[:i]
					implicitJoin = true
				}
			}
			if fk || explicitJoin || implicitJoin {
				omit := false
				for _, v := range omitFields {
					if v == refName {
						omit = true
					}
				}
				if field, ok := structType.FieldByName(refName); ok && !omit {
					fieldValue := structValue.FieldByName(refName)
					if fieldValue.Kind() == reflect.Ptr {
						model.Indexes.Add(fd.Name)
						if fieldValue.IsNil() {
							fieldValue.Set(reflect.New(field.Type.Elem()))
						}
						refModel := StructPtrToModel(fieldValue.Interface(), false, nil)
						ref := new(Reference)
						ref.ForeignKey = fk
						ref.Model = refModel
						ref.RefKey = fd.Name
						if model.Refs == nil {
							model.Refs = make(map[string]*Reference)
						}
						model.Refs[refName] = ref
					} else if !implicitJoin {
						panic("Referenced field is not pointer")
					}
				} else if !implicitJoin {
					panic("Can not find referenced field")
				}
			}
			if fd.unique {
				model.Indexes.AddUnique(fd.Name)
			} else if fd.index {
				model.Indexes.Add(fd.Name)
			}
		}
	}
	if root {
		if indexed, ok := f.(Indexed); ok {
			indexed.Indexes(&model.Indexes)
		}
	}
	return model
}

func tableName(talbe interface{}) string {
	if t, ok := talbe.(string); ok {
		return t
	}
	t := reflect.TypeOf(talbe).Elem()
	for {
		c := false
		switch t.Kind() {
		case reflect.Array, reflect.Chan, reflect.Map, reflect.Ptr, reflect.Slice:
			t = t.Elem()
			c = true
		}
		if !c {
			break
		}
	}
	return StructNameToTableName(t.Name())
}

func parseTags(fd *ModelField, s string) {
	if s == "" {
		return
	}
	c := strings.Split(s, ",")
	for _, v := range c {
		c2 := strings.Split(v, ":")
		if len(c2) == 2 {
			switch c2[0] {
			case "fk":
				fd.fk = c2[1]
			case "size":
				fd.size, _ = strconv.Atoi(c2[1])
			case "default":
				fd.dfault = c2[1]
			case "join":
				fd.join = c2[1]
			default:
				panic(c2[0] + " tag syntax error")
			}
		} else {
			switch c2[0] {
			case "created":
				fd.created = true
			case "pk":
				fd.pk = true
			case "updated":
				fd.updated = true
			case "index":
				fd.index = true
			case "unique":
				fd.unique = true
			case "notnull":
				fd.notnull = true
			default:
				panic(c2[0] + " tag syntax error")
			}
		}
	}
	return
}

func toSnake(s string) string {
	buf := new(bytes.Buffer)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			if i > 0 {
				buf.WriteByte('_')
			}
			buf.WriteByte(c + 32)
		} else {
			buf.WriteByte(c)
		}
	}
	return buf.String()
}

func snakeToUpperCamel(s string) string {
	buf := new(bytes.Buffer)
	first := true
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' && first {
			buf.WriteByte(c - 32)
			first = false
		} else if c == '_' {
			first = true
			continue
		} else {
			buf.WriteByte(c)
		}
	}
	return buf.String()
}

var ValidTags = map[string]bool{
	"pk":      true, //primary key
	"fk":      true, //foreign key
	"size":    true,
	"default": true,
	"join":    true,
	"-":       true, //ignore
	"index":   true,
	"unique":  true,
	"notnull": true,
	"updated": true,
	"created": true,
}
