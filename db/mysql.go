package db

import (
	"database/sql"
	"fmt"
	"move_csv_vacancies_to_db/models"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func ConnectToMySQL() *sql.DB{
	url := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", "edwica_root", "b00m5gQ40WB1", "83.220.175.75", "3306", "edwica")
	connection, err := sql.Open("mysql", url)
	checkErr(err)
	return connection
}

func GetVacanciesFromMYSQL(lastId int, limit int) (vacancies []models.Vacancy) {
	query := fmt.Sprintf("SELECT id, position_id, city_id, salary_from, salary_to,  name, url, prof_areas, specs, experience, key_skills, vacancy_date, parsing_date, platform FROM h_vacancy WHERE position_id != 0 AND id > %d ORDER BY id ASC LIMIT %d", lastId, limit)
	connection := ConnectToMySQL()
	rows, err := connection.Query(query)
	checkErr(err)
	defer rows.Close()

	for rows.Next() {
		var city_id, position_id, id int
		var name, url, prof_areas, specs, experience, vacancy_date, parsing_date, key_skills, salary_from, salary_to, platform  string
		err = rows.Scan(&id, &position_id, &city_id, &salary_from, &salary_to, &name, &url, &prof_areas, &specs, &experience, &key_skills, &vacancy_date, &parsing_date, &platform)
		checkErr(err)
		if vacancy_date == "" {
			vacancy_date = "2022-05-25 18:02:04.000"
		} else if strings.Contains(vacancy_date, ".") {
			date, err := time.Parse("02.01.2006 15:04:05", vacancy_date)
			checkErr(err)
			vacancy_date = strings.Split(date.String(), "+")[0]  // Обрезаем строку 2023-04-19 13:08:33 +0000 UTC
		}
		vacancies = append(vacancies, models.Vacancy{
			Id: id,
			PositionId: position_id,
			CityId: city_id, 
			SalaryFrom: salary_from,
			SalaryTo: salary_to,
			Title: name,
			Url: url,
			Areas: prof_areas,
			Specs: specs,
			Experience: experience,
			Skills: key_skills,
			VacancyDate: vacancy_date,
			ParsingDate: parsing_date,
			Platform: platform,
		})
	}
	connection.Close()
	return
}