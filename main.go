package main

import (
	"fmt"
	"move_csv_vacancies_to_db/db"
	"time"
)

func main() {
	s := time.Now().Unix()
	db.MoveVacanciesFromCsvToPostgres()
	fmt.Println("time:", time.Now().Unix() - s)
}