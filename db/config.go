package db

import (
	"fmt"
	"os"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

var host, port, dbname, user, password string

func init() {
	err := godotenv.Load(".env")
	if err != nil {
		panic("Подключите файл .env с переменными окружения на примере .env.example")
	}

	host = os.Getenv("POSTGRES_HOST")
	port = os.Getenv("POSTGRES_PORT")
	user = os.Getenv("POSTGRES_USER")
	dbname = os.Getenv("POSTGRES_DBNAME")
	password = os.Getenv("POSTGRES_PASSWORD")
}

func ConnectToPostgres() *sqlx.DB{
	connectionUrl := fmt.Sprintf("user=edwica_root password=9k35XQ&s dbname=edwica port=5432 host=94.250.253.88 sslmode=disable")
	db, err := sqlx.Open("postgres", connectionUrl)
	checkErr(err)
	return db
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}