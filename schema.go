package qbs

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"errors"
	"runtime/debug"
)

//ReversibleMigration is an interface that allows the creation, deletion, and modification
//of tables/columns and data within a databse.
type ReversibleMigration interface {
	Structure(*Schema) error
	Data(*Schema, bool, bool) (int, error)
}

//SimpleMigration is an implementation of ReversibleMigration that allows the functions
//that will compute the parts of the Migration to be added separately or
//omitted.
type SimpleMigration struct {
	S func(*Schema) error
	D func(*Schema, bool, bool) (int, error)
}

type SimpleMigrationList []*SimpleMigration

func (self SimpleMigrationList) All() []ReversibleMigration {
	result := []ReversibleMigration{}
	for _, i := range self {
		result = append(result, ReversibleMigration(i))
	}
	return result
}

func (self *SimpleMigration) Structure(m *Schema) error {
	if self.S == nil {
		return nil
	}
	return self.S(m)
}

func (self *SimpleMigration) Data(m *Schema, haveDropCol bool, reverse bool) (int, error) {
	if self.D == nil {
		return 0, nil
	}
	return self.D(m, haveDropCol, reverse)
}

type nameOp int
type changeDirection int

const (
	//UNLIMITED_MIGRATIONS means that all migrations that can be found in
	//forward direction are desired.  This is often used when moving from
	//an empty database to "current state."
	UNLIMITED_MIGRATIONS = 10000

	OLD="OLD"
	NEW="NEW"
)

type BaseSchema struct {
	ReversedSemantics bool
	NameOnly          bool
}

type nameStructPair struct {
	name       string
	typeRep 	reflect.Value
}

type Schema struct {
	prev                      map[string]*nameStructPair
	curr                      map[string]*nameStructPair
	m                         *Migration
	oldFieldNameToColumnName  func (string)string
	oldColumnNameToFieldName  func (string)string	
	reverse                   map[string]string
}

func NewBaseSchema() *BaseSchema {
	return &BaseSchema{}
	}

func NewSchema(m *Migration) *Schema {
	result := &Schema{m: m, prev: make(map[string]*nameStructPair),
		curr: make(map[string]*nameStructPair)}
	return result
}

//ParseMigrationFlag is a convenience to help those writing migrations.  It
//adds some default flags to the flag set to allow the user to specify a
//migration.
func (self *BaseSchema) ParseMigrationFlags(fset *flag.FlagSet) int {

	migration := -1

	fset.IntVar(&migration, "m", UNLIMITED_MIGRATIONS, "migration number to change to")
	if err := fset.Parse(os.Args[1:]); err != nil {
		errmsg := fmt.Sprintf("failed to parse arguments: %s", err)
		panic(errmsg)
	}

	if migration < 0 {
		panic("you must supply a migration number with a value of at least 0 with -m flag")
	}

	return migration
}

//Dont' call this directly, call Run() so you get the transactions.
func (self *Schema) migrate(info ReversibleMigration, reverse bool) error {
	self.clear()

	if err := info.Structure(self); err != nil {
		return err
	}
	
	if reverse {
		self.flipOver()
	}

	//move logical names to a temp name
	self.renameCurrentTablesAddColumns(reverse)

	count, err := info.Data(self, self.m.HasColumnOperations(), reverse)
	if err != nil {
		return err
	}
	
	if err:=self.removeOldRenameColumns(); err!=nil {
		return err
	}
	
	if count>0 {
		self.m.log(fmt.Sprintf("SUCCESS! Data migration of %d rows\n", count))
	} else {
		self.m.log("SUCCESS! Adjusted schema successfully")
	}	

	return nil
}

func (self *Schema) removeOldRenameColumns() error {
		for _, pair := range self.prev {
			self.m.DropTable(pair.typeRep.Interface())
		}
		for logicalName, pair := range self.curr {
			//move this new version into place
			newName := StructNameToTableName(logicalName)
			oldName := StructNameToTableName(pair.name)
			if err := self.m.RenameTable(oldName, newName); err != nil {
				return err
			}				
		}
		return nil
}

func (self *Schema) clear() {
	self.curr = make(map[string]*nameStructPair)
	self.prev = make(map[string]*nameStructPair)
}

func (self *Schema) flipOver() {
	tmp := self.curr
	self.curr = self.prev
	self.prev = tmp
}

func (self *Schema) Run(info []ReversibleMigration, from int, to int) (e error) {
	
	if from == to {
		return nil
	}

	self.m.Begin()

	defer func() {
		if x:=recover(); x!=nil {
			self.untrapColumnsForSqlite3()
			self.m.Rollback()
			self.	Close()
			fmt.Printf("Panic trapped during execution of migrations: %v\n",x)
			debug.PrintStack()
			e = errors.New(fmt.Sprintf("%v",x))			
		}
	}()
	
	if from < to {
		for i := from; i < to; i++ {
			if err := self.migrate(info[i], false); err != nil {
				self.m.Rollback()
				self.Close()
				return err
			}
		}
	} else {
		for i := from - 1; i >= to; i-- {
			if err := self.migrate(info[i], true); err != nil {
				self.m.Rollback()
				self.Close()
				return err
			}
		}
	}
	self.m.Commit()
	return nil
}

func (self *Schema) renameCurrentTablesAddColumns(reverse bool) error {
	suffix := OLD
	trap := NEW

	for logicalName, pair := range self.prev {
		//ok, we need to do the rename to prepare for a data migration
		oldName := StructNameToTableName(logicalName)
		newName := StructNameToTableName(pair.name)
		if err := self.m.RenameTable(oldName, newName); err != nil {
			return err
		}	
	}
	for _, pair := range self.curr {				
		//we use this table's name as is and make it empty f	or use by the data migration
		self.trapColumnsForSqlite3(trap)
		err := self.m.CreateTable("", pair.typeRep.Interface(), 
				fieldsWithSuffix(pair.typeRep.Interface(), 	suffix)); 		
		self.untrapColumnsForSqlite3()
		if err != nil {
			return err
		}
	}
	return nil
}

func fieldsWithSuffix(structPtr interface{}, suffix string) []string {
	result := []string{}
	t := reflect.TypeOf(structPtr).Elem()		
	for i := 0; i < t.NumField(); i++ {
		n := t.Field(i).Name
		if strings.HasSuffix(n, suffix) {
			result = append(result, n)
		}
	}
	return result
}

func (self *Schema) ChangeTable(logicalName string, prev_raw interface{},
	curr_raw interface{}) error {
	
	if prev_raw == nil {
		self.curr[logicalName] = structPair(curr_raw)
		return nil
	}
	if curr_raw == nil {
		self.prev[logicalName] = structPair(prev_raw)
		return nil
	}

	curr_pair := structPair(curr_raw)
	prev_pair := structPair(prev_raw)

	self.prev[logicalName] = prev_pair
	self.curr[logicalName] = curr_pair
	return nil
}

func structPair(i interface{}) *nameStructPair {
	v := reflect.ValueOf(i)
	t := v.Type()
	if t.Kind() != reflect.Ptr {
		panic("expected a POINTER to a slice of pointers to structs or POINTER to struct")
	}
	v2 := v.Elem()
	e := v2.Type()
	if e.Kind() != reflect.Slice && e.Kind() != reflect.Struct {
		panic("expected a pointer to a SLICE of pointers to structs or pointer to STRUCT")
	}
	if e.Kind() == reflect.Struct {
		return &nameStructPair{e.Name(), v} //return PTR!
	}

	//reset v to be the type of the slice elements, creating one if necessary
	if v2.Len()==0 {
		elemOfSliceType:=v2.Type().Elem()
		if elemOfSliceType.Kind() != reflect.Ptr {
			panic("expected a pointer to slice of POINTERS to structs")
		}
		v = reflect.New(elemOfSliceType.Elem())
	} else {
		v = v2.Index(0)
	}
	p := v.Type()
	if p.Kind() != reflect.Ptr {
		panic("expected a pointer to a slice of POINTERS to structs")
	}
	v2 = v.Elem()
	s := v2.Type()
	if s.Kind() != reflect.Struct {
		panic("expected a pointer to a slice of pointers to STRUCTS")
	}	
	return &nameStructPair{s.Name(), v} //return PTR!
}

func (self *Schema) checkLogicalName(logical string, i interface{}) {
	pair := structPair(i)

	pieces := strings.SplitN(pair.name, "_migration", 2)
	if pieces[0] != logical {
		panic("can't understand logical name " + logical + " with struct " + pair.name)
	}
	if _, err := strconv.ParseInt(pieces[1], 10, 64); err != nil {
		panic("can't understand migration number " + pieces[1] + " in struct name " + pair.name)
	}
}

func (self *Schema) FindAll(logicalName string) (interface{}, error) {
	q := self.m.GetQbsSameTransaction()
	pair := self.prev[logicalName]	

	sliceVal := reflect.MakeSlice(reflect.SliceOf(pair.typeRep.Type()	)	, 0, 0)
	
	ptrForSet := reflect.New(reflect.SliceOf(pair.typeRep.Type()))
	reflect.Indirect(ptrForSet).Set(sliceVal)

	
	self.trapColumnsForSqlite3(NEW)
	q.OmitFields(fieldsWithSuffix(pair.typeRep.Interface(), 	OLD)...)

	err := q.FindAll(ptrForSet.Interface()	)
	self.untrapColumnsForSqlite3()
	
	if err != nil {
		self.m.Rollback()
		return nil, err
	}
	return ptrForSet.Interface(),nil
}

func (self *Schema) Save(logicalName string, curr interface{}) (int64, error) {
	self.checkLogicalName(logicalName, curr)
	q := self.m.GetQbsSameTransaction()
	q.OmitFields(fieldsWithSuffix(curr, 	OLD)...)
	
	self.trapColumnsForSqlite3(NEW)		
	res, err := q.Save(curr)
	self.untrapColumnsForSqlite3()
	
	if err != nil {
		self.m.Rollback()
		return 0, err
	}
	return res, nil
}

func (self *Schema) Close() {
	self.m.Close()
	self.m = nil
}
func (self *Schema) trapColumnsForSqlite3(suffix string) {
	self.oldFieldNameToColumnName = FieldNameToColumnName
	self.oldColumnNameToFieldName = ColumnNameToFieldName
	self.reverse = make(map[string]string)

	//fmt.Printf("TRAP %s\n", suffix)	
	v:=func(s string) string {
		if strings.HasSuffix(s, suffix) {
			t := s[0: len(s)-len(suffix)]
			self.reverse[self.oldFieldNameToColumnName(t)]=
				strings.ToLower(s[0:1])+s[1:len(s)]
			//fmt.Printf("-->SAVE %s %+v\n", s, self.reverse)
			return self.oldFieldNameToColumnName(t)
		}
		//fmt.Printf("MISS -> Col $$$ %s\n",s)
		return self.oldFieldNameToColumnName(s)
	}
	FieldNameToColumnName = v
	v=func(s string) string {
		t, ok:=self.reverse[s]
		if ok {
			//fmt.Printf("-->LOAD %s %+v\n", s, self.reverse)
			return self.oldColumnNameToFieldName(t)
		}
		//fmt.Printf("MISS -> Field $$$ %s\n",s)
		return self.oldColumnNameToFieldName(s)
	}
	ColumnNameToFieldName = v
}

func (self *Schema) untrapColumnsForSqlite3() {
	//fmt.Printf("UNTRAP %v %v\n", self.oldFieldNameToColumnName !=nil, 
	//	self.oldColumnNameToFieldName !=nil)
	if self.oldFieldNameToColumnName !=nil {
		FieldNameToColumnName = self.oldFieldNameToColumnName
		self.oldFieldNameToColumnName = nil
	}
	if self.oldColumnNameToFieldName !=nil {
		ColumnNameToFieldName = self.oldColumnNameToFieldName
		self.oldColumnNameToFieldName = nil
	}
}