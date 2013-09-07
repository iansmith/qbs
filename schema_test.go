package qbs

import (
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"testing"
	"time"
	"fmt"
	"errors"
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
	ContentOLD string `qbs:"size:255"`
	ContentNEW string
	CreatedNEW time.Time `qbs:"created"`
	UpdatedNEW time.Time `qbs:"updated"`
}

/**
 * For getting from 2 to 3
 */
type Article_migration3 struct {
	Id           int64
	AuthorOLD   string `qbs:"size:127"`
	AuthorIdNEW int64 `qbs:"fk:AuthorNEW"`
	AuthorNEW   *User_migration3 `qbs:"join:AuthorNEW"`
	Content      string
	Created      time.Time
	Updated      time.Time
}

/**
  * User migration3 is the same at user for the
  * moment.
  */
type User_migration3 User;

/**
  * List of migrations.
  */
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

/**
  * Create the article table
  */
func Migrate1_AddArticleTable(m *Schema) error {
	if simulatePanic {
		panic("simulated to check transactions")
	}	
	if simulateError {
		return errors.New("simulated to check transactions")
	}
	return m.ChangeTable("Article", nil, &Article_migration1{})
}

/**
  * Register a change from migration 1 to 2 
  */
func Migrate2_ChangeContent(m *Schema) error {
	return m.ChangeTable("Article", &Article_migration1{}, &Article_migration2{})
}

// used for testing
var simulatePanic bool
var simulateError bool

const (
	cheney = "Dick Cheney"
	veep = "The Vice Presidency"
	prez = "The Presidency"
)

/**
 * We have move the data between the two versions of the Content column.
 */
func Migrate2_MoveContent(m *Schema, haveColumnOps bool, reverse bool) (int, error) {

	var err error

	if simulatePanic {
		panic("simulated to check transactions")
	}
	
	if simulateError {
		return 0, errors.New("simulated to check transactions")
	}

 	raw, err := m.FindAll("Article"); 
 	if err != nil {
		return 0, err
	}
		
	if reverse {
		allRows := raw.(*[]*Article_migration2)
		for _, newRow := range *allRows {
			var oldRow *Article_migration1
			//semantic change on the name of the book between v1 and v2
			if newRow.ContentNEW==prez && newRow.Author==cheney {
				oldRow =&Article_migration1{Content: veep, Author: cheney}		
			} else {
				oldRow = &Article_migration1{Content: newRow.ContentNEW, Author: newRow.Author} 
			}
			if _, err = m.Save("Article", oldRow); err!=nil {
				return 0, err
			}
		}
		return len(*allRows),nil
	}
	allRows := raw.(*[]*Article_migration1)
	for _, oldRow := range *allRows {
		var newRow *Article_migration2
		//semantic change on the name of the book between v1 and v2
		if oldRow.Content==veep && oldRow.Author==cheney {
			newRow = &Article_migration2{ContentNEW: prez, Author: cheney}
		} else {
			newRow = &Article_migration2{ContentNEW: oldRow.Content, 
				Author: oldRow.Author} 
		}
		if _, err = m.Save("Article", newRow); err!=nil {
			return 0, err
		}
	}
	return len(*allRows), nil
}

/**
  * Register a change between migration 2 and 3 of article, plus
  * create the new User table.
  */
func Migrate3_ConvertAuthorToUser(m *Schema) error {
	// NOTE: Child tables must come before parent tables
	if err := m.ChangeTable("User", nil, &User_migration3{}); err != nil {
		return err
	}	
	return m.ChangeTable("Article", &Article_migration2{}, &Article_migration3{})
}

/**
 * Convenience func for mapping a string to a first name and last name
 * in our User struct.
 */
func authorToUser(row *Article_migration2) *User_migration3 {
	u := &User_migration3{}
	s := strings.SplitN(row.Author, " ", 2)
	u.FirstName = s[0]
	u.LastName = s[1]
	u.Email = "example@example.com" //default value
	return u
}

/**
 * This migration requires us to pull out the user into a separate table
 * make the Article point to it.
 */
func Migrate3_MoveAuthor(m *Schema, haveDropCol bool, reverse bool) (int, error) {

 	raw, err := m.FindAll("Article"); 
 	if err != nil {
		return 0, err
	}
		
	if reverse {
		allRows := raw.(*[]*Article_migration3)
		for _, newRow := range *allRows {
			var oldRow *Article_migration2
			name := newRow.AuthorNEW.FirstName + " "+newRow.AuthorNEW.LastName
			oldRow = &Article_migration2 { ContentNEW : newRow.Content,
				CreatedNEW :newRow.Created, Author:name}		
			//oldRow.UpdatedNEW will be overwritten when we save
			if _, err := m.Save("Article", oldRow); err != nil {
				return 0, err
			}
		}
		return len(*allRows), nil
	}

	allRows := raw.(*[]*Article_migration2)
	for _, oldRow := range *allRows {
		var newrow *Article_migration3
		u := authorToUser(oldRow)
		_, err := m.Save("User", u)
		if err != nil {
			return 0, err
		}
		newrow = &Article_migration3{AuthorIdNEW:u.Id, AuthorNEW: u	, 
			Content: oldRow.ContentNEW}
		if _, err := m.Save("Article", newrow); err != nil {
			return 0, err
		}
	}
	return len(*allRows), nil
}

func setup(T *testing.T) *Schema{

	RegisterSqlite3("/tmp/bletch.db")
	m, err := GetMigration()
	if err != nil {
		T.Fatalf("unable to get the database connection open: %s", err)
	}
	m.Log = true
	simulateError = false
	simulatePanic = false
	
	s:= NewSchema(m)
	m.DropTableByName(StructNameToTableName("Article")) 
	m.DropTableByName(StructNameToTableName("User")) 
	return 	s
}

func TestSchemaCreateBasic(T *testing.T) {

	s:=setup(T)

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

	s.Close()
}

func TestSchemaPanicRollbackInDefChange(T *testing.T) {

	s:=setup(T)
	simulatePanic = true
	errExpectedTest(T, s, 0, 1)	
}

func TestSchemaErrorRollbackInDefChange(T *testing.T) {

	s:=setup(T)
	simulateError = true
	errExpectedTest(T, s, 0, 1)	
}

func setupAfterMigration(T *testing.T, n int) *Schema {
	s:=setup(T)
	err := s.Run(myMigrations.All(), 0, n)
	if err!=nil {
		T.Fatalf("Failed to setup for Panic in Data Move test", err)
	}
	return s
}

func TestSchemaPanicRollbackInDataMove(T *testing.T) {
	s:=setupAfterMigration(T, 1)
	simulatePanic = true
	errExpectedTest(T, s, 1, 2)	
}

func TestSchemaErrorRollbackInDataMove(T *testing.T) {

	s:=setupAfterMigration(T, 1)
	simulateError = true
	errExpectedTest(T, s, 1, 2)	
}

func errExpectedTest(T *testing.T, s *Schema, from int, to int) {	
	err := s.Run(myMigrations.All(), from, to)
	if err == nil {
		T.Fatalf("expected to get a failure in migration")
	}

	//the migration is already closed by here
	if s.m!=nil {
		T.Fatalf("Expected the close method to already be called")
	}

	//have to reconnect
	m,err:=GetMigration()
	if err != nil {
		T.Fatalf("Error trying to reconnect to database %v", err)
	}
		
		s=NewSchema(m)	
	confirmNoTables(T, s)
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

func confirmNoTables(T *testing.T, s *Schema) {
	confirmTableDoesntExist(T, s, "Article_migration1")
	confirmTableDoesntExist(T, s, "Article_migration2")
	confirmTableDoesntExist(T, s, "Article")
	confirmTableDoesntExist(T, s, "User")
	confirmTableDoesntExist(T, s, "User_migration3")
}

func TestSchemaReversalToZero(T *testing.T) {
	s:=setupAfterMigration(T, 1)
	createRowAtMigration1(T, 	"Joe Blow", "Some book", s)
	createRowAtMigration1(T, cheney, veep, s)
	s.Run(myMigrations.All(), 1, 2)
	title:=confirmCanReadContent(T,s)
	if title!=prez{
		T.Fatalf("Bad title on book after reversal: %s", title)
	}

	//
	//now reverse
	//	
	s.Run(myMigrations.All(), 2, 1)
	title=confirmCanReadContent(T,s)
	if title!=veep {
		T.Fatalf("Bad title on book after reversal: %s", title)
	}
	s.Run(myMigrations.All(), 1, 0)	
	confirmNoTables(T, s)
}

func TestSchemaDataSimple(T *testing.T) {

	s:=setup(T)

	err := s.Run(myMigrations.All(), 0, 1)
	if err != nil {
		T.Fatalf("failed trying the 0th migration: " + err.Error())
	}
	
	createRowAtMigration1(T, cheney, veep, s)
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

	title:=confirmCanReadContent(T, s)
	if title!=prez {
		T.Fatalf("Book in wrong state: %s", title)
	}
	
	s.Close()	
}

func TestSchemaNewUserTable(T *testing.T) {
	s:=setup(T)
	err:=s.Run(myMigrations.All(), 0, 1)
	if err!=nil {
		T.Fatal("Error running migrations 0->1: %s",err)
	}
	createRowAtMigration1(T, cheney, veep, s)
	createRowAtMigration1(T, 	"Anthony Bourdain", "Kitchen Confidential", s)

	err=s.Run(myMigrations.All(), 1, 3)
	if err!=nil {
		T.Fatal("Error running migrations 1->3: %s",err)
	}
	
	err=s.Run(myMigrations.All(), 3, 1)
	if err!=nil {
		T.Fatal("Error running migrations 3->1		: %s",err)
	}
	
	if confirmCanReadContent(T, s)!=veep {
		T.Fatalf("Didn't find correct book by Cheney")
	}

	err=s.Run(myMigrations.All(), 1, 0)
	if err!=nil {	
		T.Fatal("Error running migrations 1->0		: %s",err)
	}
	confirmNoTables(T,s)	
}


func confirmCanReadContent(T *testing.T, s *Schema) string {
	//check can read content that was changed over
	rows, err := s.m.db.Query(
		fmt.Sprintf("select %s from %s where author = \"Dick Cheney\"", 
			FieldNameToColumnName("Content"),
			StructNameToTableName("Article")))
		
	if err!=nil {
		T.Fatalf("Expected to be able to read from Article table: %v", err)
	}
	
	if !rows.Next() {
		T.Fatalf("can't find Dick Cheney's book")
	}
	var result string
	err=rows.Scan(&result)
	if err!=nil {
		T.Fatalf("Error scanning for book title: %v\n", err)
	}
	if rows.Next() {
		T.Fatalf("found too many books by Dick Cheney")
	}
	return result
}
