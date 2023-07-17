package db

import (
	"fmt"
	"os"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var psg_host, psg_port, psg_dbname, psg_user, psg_password string
var msq_host, msq_port, msq_dbname, msq_user, msq_password string

func Init() {
	err := godotenv.Load(".env")
	if err != nil {
		panic("Подключите файл .env с переменными окружения на примере .env.example")
	}
	psg_host = os.Getenv("POSTGRES_HOST")
	psg_port = os.Getenv("POSTGRES_PORT")
	psg_user = os.Getenv("POSTGRES_USER")
	psg_dbname = os.Getenv("POSTGRES_DBNAME")
	psg_password = os.Getenv("POSTGRES_PASSWORD")

	msq_host = os.Getenv("MYSQL_HOST")
	msq_port = os.Getenv("MYSQL_PORT")
	msq_user = os.Getenv("MYSQL_USER")
	msq_dbname = os.Getenv("MYSQL_DBNAME")
	msq_password = os.Getenv("MYSQL_PASSWORD")
}

func ConnectToPostgres() *sqlx.DB{
	Init()
	connectionUrl := fmt.Sprintf("user=%s password=%s dbname=%s port=%s host=%s sslmode=disable", psg_user, psg_password, psg_dbname, psg_port, psg_host)
	db, err := sqlx.Open("postgres", connectionUrl)
	checkErr(err)
	return db
}

func ConnectToMySQL() *sql.DB{
	Init()
	url := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", msq_user, msq_password, msq_host, msq_port, msq_dbname)
	connection, err := sql.Open("mysql", url)
	checkErr(err)
	return connection
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}