package db

import (
	"fmt"
	"move_csv_vacancies_to_db/models"
	"strings"
	"time"

)



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

func SaveVacanciesToMySQL(vacancies []models.Vacancy) {
	db := ConnectToMySQL()
	defer db.Close()
	for i:=0; i<len(vacancies); i+=2000 {
		group := vacancies[i:]
		if len(group) > 2000 {
			group = group[:2000]
		}
		query, valArgs := createQueryForMultipleInsertVacanciesMYSQL(group)


		tx, _ := db.Begin()
		_, err := db.Exec(query, valArgs...)
		checkErr(err)
		tx.Commit()
		fmt.Printf("Сохранили %d вакансий\n", len(group))
	}
}

func createQueryForMultipleInsertVacanciesMYSQL(vacancies []models.Vacancy) (query string, valArgs []interface{}) {
	query = "INSERT IGNORE INTO h_vacancy (id, name, url, city_id, position_id, prof_areas, specs, experience, salary_from, salary_to, key_skills, vacancy_date, platform, parsing_date) VALUES "
	for _, v := range vacancies {
		if v.SalaryFrom == "" {
			v.SalaryFrom = "0"
		}
		if v.SalaryTo == "" {
			v.SalaryTo = "0"
		}
		query += "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),"
		valArgs = append(valArgs,  v.Id, v.Title, v.Url, v.CityId, v.PositionId, v.Areas, v.Specs, v.Experience, v.SalaryFrom, v.SalaryTo, v.Skills, v.VacancyDate, v.Platform, v.ParsingDate)
	}
	query = query[0:len(query)-1]
	return
}