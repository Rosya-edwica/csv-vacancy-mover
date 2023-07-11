package db

import (
	"encoding/csv"
	"fmt"
	"move_csv_vacancies_to_db/models"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

const Folder = "Vacancies"
var Positions = GetPositions()
const DefaultDate = "2022-05-25 18:02:04.000"

func MoveVacanciesFromCsvToPostgres() {
	files, err := os.ReadDir(Folder)
	if err != nil {
		if err.Error() == "open Vacancies: The system cannot find the file specified." {
			panic("Создайте папку Vacancies и поместите туда csv-файлы")
		} else {
			panic(err)
		}
	}
	for _, item := range files {
		if strings.HasSuffix(item.Name(), ".csv") {
			filePath := path.Join(Folder, item.Name())
			fmt.Println(filePath)
			vacancies := GetVacanciesFromFile(filePath)
			SaveVacancies(vacancies)
			os.Remove(filePath)
		}
	}
}


func GetVacanciesFromFile(file string) (vacancies []models.Vacancy) {
	f, err := os.Open(file)
	checkErr(err)
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	reader.Comma = ';'
	records, err := reader.ReadAll()
	checkErr(err)

	for row_num, row := range records {
		if row_num == 0 {
			continue
		}
		id, err := strconv.Atoi(row[0])
		checkErr(err)

		vacancy :=  models.Vacancy{
			Id: id,
			Url: row[1],
			Title: row[2],
			PositionName: row[3],
			Areas: row[4],
			Specs: row[5],
			Experience: row[6],
			SalaryFrom: row[7],
			SalaryTo: row[8],
			Skills: row[9],
			VacancyDate: DefaultDate,
			ParsingDate: time.Now().String(),
		}
		vacancy.PositionId= getPositionIdByName(vacancy.PositionName)
		if vacancy.SalaryFrom == "None" || len(vacancy.SalaryFrom) == 0{
			vacancy.SalaryFrom = "0"
		}
		if vacancy.SalaryTo == "None" || len(vacancy.SalaryTo) == 0{
			vacancy.SalaryTo = "0"
		}
		if vacancy.PositionId != 0 {
			vacancies = append(vacancies, vacancy)
		}
	}
	return
}

func getPositionIdByName(name string) (id int) {
	for _, item := range Positions {
		if strings.ToLower(item.Name) == strings.ToLower(name) {
			return item.Position_Id
		}
	}
	return
}

