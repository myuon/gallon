package main

import (
	"database/sql"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

type UserTable struct {
	ID        string `json:"id" fake:"{uuid}"`
	Name      string `json:"name" fake:"{firstname}"`
	Age       int    `json:"age" fake:"{number:1,100}"`
	CreatedAt int64  `json:"createdAt" fake:"{number:949720320,1896491520}"`
}

func NewFakeUserTable() (UserTable, error) {
	v := UserTable{}
	if err := gofakeit.Struct(&v); err != nil {
		return v, err
	}

	return v, nil
}

func run(db *sql.DB) error {
	if _, err := db.Query(strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS users (",
		"id VARCHAR(255) NOT NULL,",
		"name VARCHAR(255) NOT NULL,",
		"age INT NOT NULL,",
		"created_at INT NOT NULL,",
		"PRIMARY KEY (id)",
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	}, "\n")); err != nil {
		return err
	}

	query, err := db.Prepare("INSERT INTO users (id, name, age, created_at) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer func() {
		if err := query.Close(); err != nil {
			panic(err)
		}
	}()

	for i := 0; i < 1000; i++ {
		v, err := NewFakeUserTable()
		if err != nil {
			return err
		}

		if _, err := query.Exec(v.ID, v.Name, v.Age, v.CreatedAt); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	conf := mysql.Config{
		User:                 "root",
		Passwd:               "root",
		Addr:                 "localhost:3306",
		DBName:               "test",
		ParseTime:            true,
		AllowNativePasswords: true,
	}
	db, err := sql.Open("mysql", conf.FormatDSN())
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			panic(err)
		}
	}()

	if err := run(db); err != nil {
		panic(err)
	}
}
