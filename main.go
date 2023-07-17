package main

import (
	"fmt"
	"move_csv_vacancies_to_db/db"
	"os"
	"time"
)

func main() {
	s := time.Now().Unix()
	
	if len(os.Args) == 1 || os.Args[1] != "-mysql" && os.Args[1] != "-csv" && os.Args[1] != "-postgres" {
		panic("Запустите программу с дополнительным параметром: 'go run main.go -mysql OR -csv'")
	} else if os.Args[1] == "-mysql" {
		fmt.Println("Перемещаем вакансии из MySQL в Postgres...")
		MoveToPostgresFromMySQL()
	} else if os.Args[1] == "-csv" {
		fmt.Println("Перемещаем вакансии из csv-файлов в Postgres...")
		MoveCSV()
	} else if os.Args[1] == "-postgres" {
		fmt.Println("Перемещаем вакансии из Postgres в MySQL...")
		MoveToMySQLFromPostgres()
	}

	fmt.Println("Задача завершена...")
	fmt.Println("time:", time.Now().Unix() - s)
}

func MoveCSV() {
	db.MoveVacanciesFromCsvToPostgres()
}

func MoveToPostgresFromMySQL() {
	var lastId = 0
	const limit = 10000 
	vacancies := db.GetVacanciesFromMYSQL(lastId, limit)
	for {
		if len(vacancies) == 0 {
			break
		}
		db.SaveVacanciesToPostgres(vacancies)
		lastId = vacancies[len(vacancies)-1].Id
		vacancies = db.GetVacanciesFromMYSQL(lastId, limit)

	}
}


func MoveToMySQLFromPostgres() {
	var lastId = 0
	const limit = 10000
	vacancies := db.GetVacanciesFromPostgres(lastId, limit)
	for {
		if len(vacancies) == 0 {
			break
		}
		db.SaveVacanciesToMySQL(vacancies)
		lastId = vacancies[len(vacancies)-1].Id
		vacancies = db.GetVacanciesFromPostgres(lastId, limit)

	}
}