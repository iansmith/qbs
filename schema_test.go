package qbs

import (
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"testing"
	"time"
	"fmt"
)

/**
 * Current:3
 */
type Article struct {
	Id       int64
	AuthorId int64
	Author   *User
	Content  string
	Created  time.Time
	Updated  time.Time
}

type User struct {
	Id        int64
	FirstName string `qbs:"size:127"`
	LastName  string `qbs:"size:127"`
	Email     string `qbs:"size:127"`
}

/**
 * For getting from empty to 1
 */
type Article_migration1 struct {
	Id      int64
	Author  string `qbs:"size:127"`
	Content string `qbs:"size:255"`
}

/**
 * For getting from 1 to 2
 */
type Article_migration2 struct {
	Id          int64
	Author      string `qbs:"size:127"`
	Content_old string `qbs:"size:255"`
	Content_new string
	Created_new time.Time `qbs:"created"`
	Updated_new time.Time `qbs:"updated"`
}

/**
 * For getting from 2 to 3
 */
type Article_migration3 struct {
	Id           int64
	Author_old   string `qbs:"size:127"`
	AuthorId_new int64
	Author_new   *User
	Content      string
	Created      time.Time
	Updated      time.Time
}

type User_migration3 User

var myMigrations = SimpleMigrationList{
	/* one */
	&SimpleMigration{
		Migrate1_AddArticleTable, nil,
	},
	/* two */
	&SimpleMigration{
		Migrate2_ChangeContent, Migrate2_MoveContent,
	},
	/* three */
	&SimpleMigration{
		Migrate3_ConvertAuthorToUser, Migrate3_MoveAuthor,
	},
}

func Migrate1_AddArticleTable(m *Schema) error {
	return m.ChangeTable("Article", nil, &Article_migration1{})
}

func Migrate2_ChangeContent(m *Schema) error {
	return m.ChangeTable("Article", &Article_migration1{}, &Article_migration2{})
}

/**
 * We have move the data between the two versions of the Content column.
 */
func Migrate2_MoveContent(m *Schema, haveDropCol bool, reverse bool) (int, error) {

	var err error

 	raw, err := m.FindAll("Article"); 
 	if err != nil {
		return 0, err
	}
	
	
	if reverse {
		allRows := raw.(*[]*Article_migration2)
		for _, newRow := range *allRows {
			oldRow := &Article_migration1{Content: newRow.Content_new, Author: newRow.Author} 
			if _, err = m.Save("Article", oldRow); err!=nil {
				return 0, err
			}
		}
		return len(*allRows),nil
	}
	allRows := raw.(*[]*Article_migration1)
	for _, oldRow := range *allRows {
		newRow := &Article_migration2{Content_new: oldRow.Content, 
			Author: oldRow.Author, Content_old: "fart"} 
		if _, err = m.Save("Article", newRow); err!=nil {
			return 0, err
		}
	}
	return len(*allRows), nil
}

func Migrate3_ConvertAuthorToUser(m *Schema) error {
		if err := m.ChangeTable("Article", &Article_migration2{}, &Article_migration3{}); err != nil {
		return err
	}
	return m.ChangeTable("User", nil, &User_migration3{})
}

/**
 * Convenience func for mapping a string to a first name and last name
 * in our User struct.
 */
func authorToUser(row *Article_migration3) *User_migration3 {

	u := &User_migration3{}
	s := strings.SplitN(row.Author_old, " ", 2)
	u.FirstName = s[0]
	u.LastName = s[1]
	return u
}

/**
 * This migration requires us to pull out the user into a separate table
 * make the Article point to it.
 */
func Migrate3_MoveAuthor(m *Schema, haveDropCol bool, reverse bool) (int, error) {
	/*var prev []*Article_migration3
	if err := m.FindAll("Article", &prev); err != nil {
		return 0, err
	}
	for _, row := range prev {
		var newrow Article_migration3
		newrow = *row
		if reverse {
			newrow.Author_old = row.Author_new.FirstName + " " + row.Author_new.LastName
		} else {
			u := authorToUser(row)
			uid, err := m.Save("User", u)
			if err != nil {
				return 0, err
			}
			u.Id = uid
			newrow.AuthorId_new = u.Id
			tmp := User(*u)
			newrow.Author_new = &tmp
		}
		if _, err := m.Save("Author", newrow); err != nil {
			return 0, err
		}
	}
	return len(prev), nil*/
	return 0,nil
}

func setup(T *testing.T) *Migration{

	RegisterSqlite3("/tmp/bletch.db")
	m, err := GetMigration()
	if err != nil {
		T.Fatalf("unable to get the database connection open: %s", err)
	}
	m.Log = true
	return m
}

func TestSchemaCreateBasic(T *testing.T) {

	m:=setup(T)
	s := NewSchema(m)
	
	//
	// TEST PREP
	//
	m.DropTableByName(StructNameToTableName("Article")) 


	//
	// CODE UNDER TEST
	//
	err := s.Run(myMigrations.All(), 0, 1)
	if err != nil {
		T.Fatalf("failed trying the 0th migration: " + err.Error())
	}

	//
	// DB CONFIRMATION OF TABLES CREATED/NOT CREATED
	//
	confirmTableDoesntExist(T, s, "Article_migration1")
	confirmTableDoesntExist(T, s, "Article_migration2")
	confirmTableExists(T, s, "Article")
}

func confirmTableDoesntExist(T *testing.T, s *Schema, name string) {
	_, err := s.m.db.Query("select ? from ?", FieldNameToColumnName("Id"),
		StructNameToTableName(name))
	if err == nil {
		T.Fatalf("did not expect to be able to find Article_migration1")
	}
}

func confirmTableExists(T *testing.T, s *Schema, name string) {
	_, err := s.m.db.Query(fmt.Sprintf("select %s from %s", FieldNameToColumnName("Id"),
		StructNameToTableName(name)))
	if err != nil {
		T.Fatalf("expected to be able to find Article %s", err)
	}
}

func createRowAtMigration1(T *testing.T, author, content string, s *Schema) {
	sql:=fmt.Sprintf("insert into %s (%s, %s) values (\"%s\", \"%s\")",
		 StructNameToTableName("Article"), FieldNameToColumnName("Author"),
		 FieldNameToColumnName("Content"), author, content)	

	_, err:= s.m.db.Exec(sql)
	if err!=nil {
		T.Fatalf("failed trying to insert row (%s,%s): %s", author, content, err)
	}
}

func TestSchemaDataSimple(T *testing.T) {

	m:=setup(T)
	s := NewSchema(m)

	//
	// TEST SETUP
	//
	m.DropTableByName(StructNameToTableName("Article")) 
	err := s.Run(myMigrations.All(), 0, 1)
	if err != nil {
		T.Fatalf("failed trying the 0th migration: " + err.Error())
	}
	
	createRowAtMigration1(T, "Dick Cheney", "The Presidency", s)
	createRowAtMigration1(T, 	"David Maurer", "The Big Con", s)

	//
	// TEST
	//		 
	err = s.Run(myMigrations.All(), 1, 2)
	if err != nil {
		T.Fatalf("failed trying the migration 1: " + err.Error())
	}

	//
	// CHECK DATA OK AFTER
	//	
	//
	// DB CONFIRMATION OF TABLE CREATED/NOT CREATED
	//
	confirmTableDoesntExist(T, s, "Article_migration1")
	confirmTableDoesntExist(T, s, "Article_migration2")
	confirmTableExists(T, s, "Article")

	_, err = s.m.db.Query(fmt.Sprintf("select %s, %s from %s", 
		FieldNameToColumnName("Author"), FieldNameToColumnName("Content_new"),
		StructNameToTableName("Article")))
		
	if err != nil {
		T.Fatalf("error trying to find Article content %s", err)
	}

	m.Close()	
}
