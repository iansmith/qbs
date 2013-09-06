package qbs

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
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

	FINDING nameOp = iota
	SAVING

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
	state                     nameOp
}

func NewBaseSchema() *BaseSchema {
	return &BaseSchema{}
	}

func NewSchema(m *Migration) *Schema {
	result := &Schema{m: m, prev: make(map[string]*nameStructPair),
		curr: make(map[string]*nameStructPair)}
	return result
}

func (self *Schema) Log(query string, args... interface{}) {
	self.m.log(query,args...)
}

func (self *Schema) toLogicalName(s string, current bool) string {
	ref := self.curr
	if !current {
		ref = self.prev
	}
	logicalName := ""

	if self.state==FINDING {
		fmt.Printf("ignoring logical name because we have done the renaming\n")
		
	}

	//this search is ok because the number of tables is small
	for candidate, pair := range ref {
		if pair.name != s {
			continue
		}
		logicalName = candidate
		break
	}
	if logicalName == "" {
		panic("can't understand true name " + s)
	}
	return logicalName
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
		self.m.log("SUCCESS! Adjust schema successfully")
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

func (self *Schema) Run(info []ReversibleMigration, from int, to int) error {
	if from == to {
		return nil
	}

	self.m.Begin()

	if from < to {
		for i := from; i < to; i++ {
			if err := self.migrate(info[i], false); err != nil {
				self.m.Rollback()
				return err
			}
		}
	} else {
		for i := from - 1; i >= to; i-- {
			if err := self.migrate(info[i], true); err != nil {
				self.m.Rollback()
				return err
			}
		}
	}
	self.m.Commit()
	return nil
}

func (self *Schema) renameCurrentTablesAddColumns(reverse bool) error {
	for logicalName, pair := range self.prev {
		_, in_both := self.curr[logicalName]
		if !in_both {
			continue
		}
		//ok, we need to do the rename to prepare for a data migration
		oldName := StructNameToTableName(logicalName)
		newName := StructNameToTableName(pair.name)
		if err := self.m.RenameTable(oldName, newName); err != nil {
			return err
		}	
	}
	for _, pair := range self.curr {		
		//we use this table's name as is and make it empty for use by the data migration
		if err := self.m.CreateTable("", pair.typeRep.Interface(), 
				fieldsWithSuffix(pair.typeRep.Interface(), 	"_old")); err != nil {
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
	
	err := q.FindAll(ptrForSet.Interface()	)
	if err != nil {
		self.m.Rollback()
		return nil, err
	}
	return ptrForSet.Interface(),nil
}

func (self *Schema) Save(logicalName string, curr interface{}) (int64, error) {
	self.state = SAVING
	self.checkLogicalName(logicalName, curr)
	q := self.m.GetQbsSameTransaction()
	q.OmitFields(fieldsWithSuffix(curr, 	"_old")...)
	res, err := q.Save(curr)
	if err != nil {
		self.m.Rollback()
		return 0, err
	}
	return res, nil
}
