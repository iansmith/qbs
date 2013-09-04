package qbs

import (
    "strings"
    "reflect"
    "flag"
    "os"
    "fmt"
    "strconv"
)

//ReversibleMigration is an interface that allows the creation, deletion, and modification
//of tables/columns and data within a databse.
type ReversibleMigration interface {
	Structure(*Schema) error
  Data(*Schema, bool, bool) (int,error)
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
	for	_,i:=range self {
		result = append(result, ReversibleMigration(i))
	}
	return result
}

func (self *SimpleMigration) Structure(m *Schema) error {
    if self.S==nil {
        return nil
    }
    return self.S(m)
}

func (self *SimpleMigration) Data(m *Schema, haveDropCol bool, reverse bool) (int, error) {
    if self.D==nil {
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

	FORWARD changeDirection = iota
	REVERSE
)


type BaseSchema struct {
    ReversedSemantics bool
    NameOnly          bool
}

type nameStructPair struct {
	name string
	struct_ptr reflect.Value
}


type Schema struct {
    prevFieldNameToColumnName func (string) string
    prevColumnNameToFieldName func (string) string
    prevTableNameToStructName func (string) string
    prevStructNameToTableName func (string) string
    prev map[string]*nameStructPair
    curr map[string]*nameStructPair
    m *Migration
    state nameOp
    direction changeDirection
}

func NewBaseSchema() *BaseSchema {
	return &BaseSchema{}
}

func NewSchema(m *Migration) *Schema  {
	result := &Schema{m:m, prev: make(map[string]*nameStructPair), 
		curr: make(map[string]*nameStructPair)}

	result.prevFieldNameToColumnName = FieldNameToColumnName
	result.prevColumnNameToFieldName = ColumnNameToFieldName
	result.prevTableNameToStructName = TableNameToStructName
	result.prevStructNameToTableName = StructNameToTableName

	FieldNameToColumnName = func (s string) string {
		return result.FieldNameToColumnName(s)
	}
	ColumnNameToFieldName = func (s string) string {
		return result.ColumnNameToFieldName(s)
	}
	StructNameToTableName = func (s string) string {
		return result.StructNameToTableName(s)
	}
	TableNameToStructName = func (s string) string {
		return result.TableNameToStructName(s)
	}
	return result
}

func (self *Schema) FieldNameToColumnName(s string) string {
	fmt.Printf("FN->CN %s -> %s\n",s, self.prevFieldNameToColumnName(s))
	return self.prevFieldNameToColumnName(s)
}

func (self *Schema) ColumnNameToFieldName(s string) string {
	fmt.Printf("CN->FN %s -> %s\n",s, self.prevColumnNameToFieldName(s))
	return self.prevColumnNameToFieldName(s)
}

func (self *Schema) TableNameToStructName(s string) string {
	fmt.Printf("TN->SN %s -> %s\n",s, self.prevTableNameToStructName(s))
	return self.prevTableNameToStructName(s)
}
func (self *Schema) StructNameToTableName(s string) string {
	fmt.Printf("SN->TN %s -> %s\n",s, self.prevStructNameToTableName(s))
	ref:= self.curr
	if self.state==FINDING {
		ref=self.prev
	}
	
	logicalName := ""
	
	//this search is ok because the number of tables is small
	for candidate, pair := range ref {
		if pair.name != s {
			continue
		}
		logicalName = candidate			
		break
	}
	if logicalName == "" {
		panic("can't understand true name "+s)
	}
	return self.prevStructNameToTableName(logicalName)
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
	
	if err:=info.Structure(self); err!=nil {
		return err
	}

	if reverse {
		self.flipOver()
	}
	
	isSqlite3 := !self.m.HasColumnOperations()
	
	//are there any tables that need to be constructed from scratch?
	if err:=self.createCurrent(); err!=nil {
		return err
	}
	
	fmt.Printf("finished create current\n")
	
	//move logical names to a temp name
	self.renameCurrentTablesAddColumns(reverse)
	
	count, err:=info.Data(self, !isSqlite3, reverse)
	if err != nil {
		return err
	}
	fmt.Printf("Data migration of %d rows\n",	 count)
	return nil
}	

func (self *Schema) createCurrent () error {
	self.state = SAVING
	
	for name, pair := range self.curr {
		_, present := self.prev[name]
			if present {
			continue
		}
		
		//no need to worry about any "new" or "old" beause creating from
		//scratch implies that the state is ok
		fmt.Printf("creating table %T because it's new\n", pair.struct_ptr)
		if err:=self.m.CreateTable(pair.struct_ptr.Interface()); err!=nil {
			fmt.Printf("failed creating table--->%s\n",err)
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
		tmp:=self.curr
		self.curr=self.prev
		self.prev=tmp
		self.direction = REVERSE
}

func (self *Schema) Run(info []ReversibleMigration, from int, to int) error {
	if from==to { 
		return nil
	}
	
	self.m.Begin()
	
	
	if from < to {
		self.direction = FORWARD
		for i:=from; i<to; i++ {
			if err:=self.migrate(info[i], false); err!=nil {
				self.m.Rollback()
				return err
			}	
		}
	} else {
		
		for i:=from-1; i>=to; i-- {
			if err:=self.migrate(info[i], true); err!=nil {
				self.m.Rollback()
				return err
			}
		}
	}
	//	self.m.Commit()
	return nil
}

func (self *Schema) renameCurrentTablesAddColumns(reverse bool) error {
	for logicalName, pair := range self.prev {
		tblName := StructNameToTableName(pair.name)
		fmt.Printf("YYY renaming! %s\n", tblName)
		if err:= self.m.RenameTable(logicalName, tblName); err!=nil {
			return err
		}
		
		t:=pair.struct_ptr.Type()
		for i:=0; i<t.NumField(); i++ {	
			n:=t.Name()
			if self.direction==FORWARD {
				if strings.HasSuffix(n, "_new") {
					if err:=self.m.AddColumn(pair.struct_ptr.Interface(), n); err!=nil {
						return err
					}					
				}
			} else {
				if strings.HasSuffix(n, "_old") {
					if err:=self.m.AddColumn(pair.struct_ptr.Interface(), n); err!=nil {
						return err
					}
				}
			}
		}	
	}
	return nil
}

func (self *Schema) ChangeTable(logicalName string, prev_raw interface{}, 
	curr_raw interface{}) error {
	if prev_raw==nil {
		self.curr[logicalName] = self.structPair(curr_raw)
		return nil
	} 
	if curr_raw==nil {
		self.prev[logicalName] = self.structPair(prev_raw)	
		return nil
	} 

	curr_pair:= self.structPair(curr_raw)
	prev_pair:= self.structPair(prev_raw)
	
				
	self.prev[logicalName]=prev_pair
	self.curr[logicalName]=curr_pair
	return nil
}

func (self *Schema) structPair(i interface{}) *nameStructPair {
	v:=reflect.ValueOf(i)
	t := v.Type()
	if t.Kind()!=reflect.Ptr {
		panic("expected a POINTER to a slice of pointers to structs or POINTER to struct")
	}
	v2 := v.Elem()
	e:= v2.Type()
	if e.Kind()!=reflect.Slice  && e.Kind()!=reflect.Struct {
		panic("expected a pointer to a SLICE of pointers to structs or pointer to STRUCT")
	}			
	if e.Kind()==reflect.Struct {
		return &nameStructPair{e.Name(), v}//return PTR!
	}	
	
	//reset v because we dealt with the slice part, now considering element type of slice
	v = v2.Index(0)
	p:=v.Type()
	if p.Kind()!=reflect.Ptr{
		panic("expected a pointer to a slice of POINTERS to structs")
	}
	v2 = v.Elem()
	s:= v2.Type()
	if s.Kind()!=reflect.Struct {
		panic("expected a pointer to a slice of pointers to STRUCTS")
	}
	return &nameStructPair{s.Name(), v} //return PTR!
}

func (self *Schema) checkLogicalName(logical string, i interface{}) {
	pair:= self.structPair(i)
	
	pieces := strings.SplitN(pair.name, "_migration", 2)
	if pieces[0]!=logical {
		panic("can't understand logical name " + logical +" with struct "+ pair.name)
	}
	if _, err := strconv.ParseInt(pieces[1], 10, 64); err!=nil {
		panic("can't understand migration number "+pieces[1]+" in struct name "+ pair.name)
	}
}

func (self *Schema) FindAll(logicalName string, prev interface{}) error {
	self.state = FINDING 
	self.checkLogicalName(logicalName, prev)
	q := self.m.GetQbsSameTransaction()
	err := q.FindAll(prev)		
	if err!=nil {
		self.m.Rollback()
		return err
	}
	q.Close()
	return nil	
} 

func (self *Schema) Save(logicalName string, curr interface{}) (int64, error) {
	self.state = SAVING
	self.checkLogicalName(logicalName, curr)
	q := self.m.GetQbsSameTransaction()
	res, err:= q.Save(curr)
	if err!=nil {
		self.m.Rollback()
		return 0, err
	}
	q.Close()
	return res, nil		
} 
	 