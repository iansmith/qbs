package qbs

import (
		"time"
	"testing"
	"strings"
	_ "github.com/mattn/go-sqlite3"
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
	Created     time.Time
	Updated     time.Time
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

var myMigrations = SimpleMigrationList {
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
  * We have move the data between "Content" columns in the old and new version because the
  * the type of the column changed.
  */
func Migrate2_MoveContent(m *Schema, haveDropCol bool, reverse bool) (int, error) {
	var prev []*Article_migration2
	if err := m.FindAll("Article", &prev); err != nil {
		return 0, err
	}
	for _, row := range prev {
		var newrow Article_migration2
		newrow = *row
		if reverse {
			newrow.Content_old = row.Content_new
		} else {
			newrow.Content_new = row.Content_old
		}
		if _, err := m.Save("Article", newrow); err != nil {
			return 0, err
		}
	}
	return len(prev), nil
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
	var prev []*Article_migration3
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
	return len(prev), nil
}	

func TestSchemaBasic(T *testing.T) {
	RegisterSqlite3("/tmp/bletch.db")
	m, err := GetMigration()
	if err!=nil {
		T.Fatalf("unable to get the database connection open: %s", err)
	}
	m.Log = true
	
	m.DropTable(&Article_migration1{}) //test setup	
	
	s := NewSchema(m)

	err=s.Run(myMigrations.All(), 0, 1)
	if err!=nil {
		T.Fatalf("failed trying the 0th migration: "+err.Error())
	}
	
	_, err =s.m.db.Exec("select Id from Article_migration1")
	if err==nil {
		T.Fatalf("did not expect to be able to find Article_migration1")
	}

	result, err := s.m.db	.Exec("select Id from Article")
	if err!=nil {
		T.Fatalf("expected to be able to find Article %s", err)
	}
	r, err := result.RowsAffected()
	if err!=nil {
		T.Fatalf("rows affected failed, %s", err)
	}
	
	if r!=0 {
		T.Fatalf("unexpected number of rows in query result, expected 0 but got %d", result.RowsAffected)
	}	
	
}		