package main

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

func main() {

	db, err := sql.Open("mysql", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph3")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	db2, err := sql.Open("mysql", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph2")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db2.Close()

	fmt.Printf("db type: %T\n", db)

	// Open doesn't open a connection. Validate DSN data:
	err = db.Ping()
	if err != nil {
		fmt.Println(err.Error()) // proper error handling instead of panic in your app
	} else {
		fmt.Println("Successfully pinged database.... db")
	}
	err = db2.Ping()
	if err != nil {
		fmt.Println(err.Error()) // proper error handling instead of panic in your app
	} else {
		fmt.Println("Successfully pinged database.... db2")
	}
}
