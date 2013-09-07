package qbs

import (
	"database/sql"
	"fmt"
	"strings"
)

type Migration struct {
	db      *sql.DB
	dbName  string
	dialect Dialect
	Log     bool
	tx      *sql.Tx
	qbs     *Qbs
}

func (mg *Migration) Begin() {
	copy, err := mg.db.Begin()
	if err != nil {
		panic("Unable to start transaction in migration : %s" + err.Error())
	}
	mg.log("NEW TRANSACTION")
	mg.tx = copy
	
	mg.qbs = &Qbs{Dialect: mg.dialect, Log: mg.Log, tx: mg.tx, 
		txStmtMap: make(map[string]*sql.Stmt), criteria: new(criteria)}
}

func (mg *Migration) GetQbsSameTransaction() *Qbs {
	if mg.tx==nil {
		panic("cant make a new qbs on same transaction because tx is nil!")
	}
	return mg.qbs
}

func (mg *Migration) HasColumnOperations() bool {
	_, ok := mg.dialect.(*sqlite3)
	return !ok
}

func (mg *Migration) Rollback() {
	if mg.tx==nil {
		panic("no transaction in progress, cannot rollback!")
	}
	if err := mg.tx.Rollback(); err != nil {
		panic("unable to rollback transaction in migration: " + err.Error())
	}
	mg.log("ROLLBACK!")
	mg.tx = nil
	mg.qbs = nil
}

func (mg *Migration) Commit() {
	if mg.tx==nil {
		panic("no transaction in progress, cannot commit")
	}
	if err := mg.tx.Commit(); err != nil {
		panic("unable to commit transaction in migration: " + err.Error())
	}
	mg.log("COMMIT!")
	mg.tx = nil
	mg.qbs = nil
}

// CreateTableIfNotExists creates a new table and its indexes based on the table struct type
// It will panic if table creation failed, and it will return error if the index creation failed.
func (mg *Migration) CreateTableIfNotExists(structPtr interface{}) error {
	return mg.createTableBase("", structPtr, true, true, nil)
}

func (mg *Migration) CreateTable(tblName string, structPtr interface{}, omitFields []string) error {
	return mg.createTableBase(tblName, structPtr, false, false, omitFields)
}

func (mg *Migration) createTableBase(overrideName string, structPtr interface{}, 
	ifexist bool, wantColumnMods bool, omitFields []string)	 error {
	model := structPtrToModel(structPtr, true, omitFields)
	if overrideName!="" {
		model.table = StructNameToTableName(overrideName)	
	}
	sql := mg.dialect.createTableSql(model, ifexist)
	sqls := strings.Split(sql, ";")
	for _, v := range sqls {
		var err error
		mg.log(v)
		if mg.tx != nil {
			_, err = mg.tx.Exec(v)
		} else {
			_, err = mg.db.Exec(v)
		}

		if err != nil && !mg.dialect.catchMigrationError(err) {
			panic(err)
		}
	}
	if wantColumnMods {
		columns := mg.dialect.columnsInTable(mg, model.table)
		if len(model.fields) > len(columns) {
			oldFields := []*modelField{}
			newFields := []*modelField{}
			for _, v := range model.fields {
				if _, ok := columns[v.name]; ok {
					oldFields = append(oldFields, v)
				} else {
					newFields = append(newFields, v)
				}
			}
			if len(oldFields) != len(columns) {
				panic("Column name has changed, rename column migration is not supported.")
			}
			for _, v := range newFields {
				mg.addColumn(model.table, v)
			}
		}
	}
	var indexErr error
	for _, i := range model.indexes {
		indexErr = mg.CreateIndexIfNotExists(model.table, i.name, i.unique, i.columns...)
	}
	return indexErr
}

//Used for testing
func (mg *Migration) DropTable(strutPtr interface{}) {
	mg.dropTableIfExists(strutPtr)
}

func (mg *Migration) log(query string, args ...interface{}) {
	if mg.Log && queryLogger != nil {
		queryLogger.Print(query)
		if len(args)>0 {
			queryLogger.Println(args...)
		}
	}
}

// this is only used for testing.
func (mg *Migration) dropTableIfExists(structPtr interface{}) {
	tn := StructNameToTableName(tableName(structPtr))

	var err error
	sql:= mg.dialect.dropTableSql(tn)
	mg.log(sql)
	
	if mg.tx!=nil {
		_, err = mg.tx.Exec(sql)
	} else {
		_, err = mg.db.Exec(sql)
	}
	
	if err != nil && !mg.dialect.catchMigrationError(err) {
		panic(err)
	}
}

// this is an unconditional drop of the table name.
func (mg *Migration) DropTableByName(name string) {
	var err error
	
	sql:= mg.dialect.dropTableSql(name)
	mg.log(sql)

	if mg.tx!=nil {
		_, err = mg.tx.Exec(sql)
	} else {
		_, err = mg.db.Exec(sql)
	}	
	if err != nil && !mg.dialect.catchMigrationError(err) {
		panic(err)
	}
}

//Add a new column to a table.
func (mg *Migration) AddColumn(structPtr interface{}, name string) error {
	tn := tableName(structPtr)
	model := structPtrToModel(structPtr, true, nil)
	var target *modelField
	for _, f := range model.fields {
		if f.name == name {
			target = f
			break
		}
	}
	if target == nil {
		panic("can't find field " + name)
	}
	mg.addColumn(tn, target)
	return nil
}

func (mg *Migration) addColumn(table string, column *modelField) {
	sql := mg.dialect.addColumnSql(table, column.name, column.value, column.size)
		if mg.Log {
		fmt.Println(sql)
	}
	_, err := mg.db.Exec(sql)
	if err != nil {
		panic(err)
	}
}

func (mg *Migration) RenameTable(oldname, newname string) error {
	sql := mg.dialect.renameTableSql(oldname, newname)
	mg.log(sql)
	
	if mg.tx!=nil {
		_, err:=mg.tx.Exec(sql)
		return err
	}		
	_, err:=mg.db.Exec(sql)
	return err
}

// CreateIndex creates the specified index on table.
// Some databases like mysql do not support this feature directly,
// So dialect may need to query the database schema table to find out if an index exists.
func (mg *Migration) CreateIndexIfNotExists(table interface{}, name string, unique bool, columns ...string) error {
	tn := tableName(table)
	name = tn + "_" + name
	if !mg.dialect.indexExists(mg, tn, name) {
		sql := mg.dialect.createIndexSql(name, tn, unique, columns...)
		if mg.Log {
			fmt.Println(sql)
		}
		_, err := mg.db.Exec(sql)
		return err
	}
	return nil
}

func (mg *Migration) Close() {
	if mg.db != nil {
		err := mg.db.Close()
		if err != nil {
			panic(err)
		}
	}
}

// Get a Migration instance should get closed like Qbs instance.
func GetMigration() (mg *Migration, err error) {
	if driver == "" || dial == nil {
		panic("database driver has not been registered, should call Register first.")
	}
	db, err := sql.Open(driver, driverSource)
	if err != nil {
		return nil, err
	}
	return &Migration{db, dbName, dial, false, nil, nil}, nil
}

// A safe and easy way to work with Migration instance without the need to open and close it.
func WithMigration(task func(mg *Migration) error) error {
	mg, err := GetMigration()
	if err != nil {
		return err
	}
	defer mg.Close()
	return task(mg)
}
