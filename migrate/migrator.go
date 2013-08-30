package migrate

import (
    "github.com/iansmith/qbs"
)

//Migration is an interface that allows the creation, deletion, and modification
//of schema and data within a databse.
type Migration interface {
    Add(Migrator) error
    Drop(Migrator) error
    Data(*qbs.Qbs) (int,error)
}

//SimpleMigration is an implementation of Migration that allows the functions
//that will compute the parts of the Migration to be added separately or
//omitted.
type SimpleMigration struct {
    Adder func(Migrator) error
    Dropper func(Migrator) error
    DataMover func(Migrator) (int, error)
}

func (self *SimpleMigration) Add(m Migrator) error {
    if self.Adder==nil {
        return nil
    }
    return self.Adder(m)
}

func (self *SimpleMigration) Drop(m Migrator) error {
    if self.Dropper==nil {
        return nil
    }
    return self.Dropper(m)
}

func (self *SimpleMigration) Data(q *qbs.Qbs) (int,error) {
    if self.DataMover==nil {
        return 0, nil
    }
    return self.DataMover(q)
}

const (
    //UNLIMITED_MIGRATIONS means that all migrations that can be found in
    //forward direction are desired.  This is often used when moving from
    //an empty database to "current state."
    UNLIMITED_MIGRATIONS = 10000
)

type Migrator interface {
    AddTable(interface{})
    AddColumnToTable(interface{}, string)

    DropTable(interface{})
    DropColumnFromTable(interface{}, string)
}

type BaseMigrator struct {
    ReversedSemantics bool
    NameOnly          bool
}

type QbsMigrator struct {
    *BaseMigrator
}

//ParseMigrationFlag is a convenience to help those writing migrations.  It
//adds some default flags to the flag set to allow the user to specify a
//migration and then parses the program's arguments and
func (self *BaseMigrator) ParseMigrationFlags(fset *flag.FlagSet) int {

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

func (self *QbsMigrator) AddTable(struct_ptr interface{}) error {
    q, err := qbs.GetQbs()
    if err != nil {
        return err
    }
    if self.NameOnly {
        model := qbs.StructPtrToModel(struct_ptr)
        return model.
    }
    m, err := qbs.GetMigration()
    if err != nil {
        return err
    }
    defer m.Close()
    return m.CreateTableIfNotExists(struct_ptr)
}

func (self *QbsMigrator) DropTableIfExists(struct_ptr interface{}) error {
    m, err := qbs.GetMigration()
    if err != nil {
        return err
    }
    defer m.Close()
    m.DropTableIfExists(struct_ptr)
    return nil
}

//NewQbsMigrator returns a new migrator implementation based on Qbs for the ORM.
func NewQbsMigrator(s *QbsStore, verbose bool, log bool) *QbsMigrator {
    result := &QbsMigrator{BaseMigrator: &BaseMigrator{}, Store: s}
    result.Store.Q.Log = (os.Getenv("QBS_LOG") != "")
    result.Verbose = verbose
    return result
}
