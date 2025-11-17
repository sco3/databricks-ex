package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"

	_ "github.com/databricks/databricks-sql-go"
)

func main() {
	godotenv.Load()
	token := os.Getenv("DATABRICKS_TOKEN")
	host := os.Getenv("DATABRICKS_HOST")
	path := os.Getenv("DATABRICKS_HTTP_PATH")

	dsn := fmt.Sprintf("token:%s@%s:443%s", token, host, path)
	fmt.Printf(" Token: %v... \n Host: %v \n Path: %v \n", token[0:10], host, path)

	db, err := sql.Open("databricks", dsn)
	if err != nil {
		panic(err)
	}

	rows, err := db.QueryContext(context.Background(), "SELECT * from dz.dz.data_rescue")
	defer rows.Close()
	var id int
	var name string
	var quantity sql.NullInt64
	var _rescue interface{}

	for rows.Next() {

		err := rows.Scan(&id, &name, &quantity, &_rescue)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
			continue // Skip to the next row on error
		}
		fmt.Printf("%10v %10v %10v %20v\n", id, name, quantity, _rescue)
	}
}
