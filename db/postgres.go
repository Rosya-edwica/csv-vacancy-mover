package db

import (
	"fmt"
	"move_csv_vacancies_to_db/models"
	"strings"
)

func GetPositions() (positions []models.Position) {
	db := ConnectToPostgres()
	defer db.Close()
	rows, err := db.Queryx("SELECT position_id, name FROM position")
	checkErr(err)
	for rows.Next() {
		var position models.Position
		rows.StructScan(&position)
		positions = append(positions, position)
	}
	
	return
}

func SaveVacanciesToPostgres(vacancies []models.Vacancy) {
	db := ConnectToPostgres()
	defer db.Close()
	for i:=0; i<len(vacancies); i+=2000 {
		group := vacancies[i:]
		if len(group) > 2000 {
			group = group[:2000]
		}
		query, valArgs := createQueryForMultipleInsertVacanciesPostgres(group)


		tx, _ := db.Begin()
		_, err := db.Exec(query, valArgs...)
		checkErr(err)
		tx.Commit()
		fmt.Printf("Сохранили %d вакансий", len(group))
	}

}

func createQueryForMultipleInsertVacanciesPostgres(vacancies []models.Vacancy) (query string, valArgs []interface{}) {
	valStrings := []string{}
	valInsertCount := 1
	for _, v := range vacancies {
		if v.SalaryFrom == "" {
			v.SalaryFrom = "0"
		}
		if v.SalaryTo == "" {
			v.SalaryTo = "0"
		}
		valStrings = append(valStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)", valInsertCount, valInsertCount+1, valInsertCount+2, valInsertCount+3, valInsertCount+4, valInsertCount+5, valInsertCount+6, valInsertCount+7, valInsertCount+8, valInsertCount+9, valInsertCount+10, valInsertCount+11, valInsertCount+12, valInsertCount+13))
		valArgs = append(valArgs,  v.Id, v.Title, v.Url, v.CityId, v.PositionId, v.Areas, v.Specs, v.Experience, v.SalaryFrom, v.SalaryTo, v.Skills, v.VacancyDate, v.Platform, v.ParsingDate)
		valInsertCount += 14
	}
	query = `INSERT INTO vacancy (id, name, url, city_id, position_id, prof_areas, specs, experience, salary_from, salary_to, key_skills, vacancy_date, platform, parsing_date) 
		VALUES` + strings.Join(valStrings, ",") + "ON CONFLICT DO NOTHING;"

	fmt.Println("Сохранили: ", len(vacancies))
	return
}

func GetVacanciesFromPostgres(lastId int, limit int) (vacancies []models.Vacancy) {
	query := fmt.Sprintf("SELECT id, position_id, city_id, salary_from, salary_to,  name, url, prof_areas, specs, experience, key_skills, vacancy_date, parsing_date, platform FROM vacancy WHERE id > %d ORDER BY id ASC LIMIT %d", lastId, limit)
	connection := ConnectToPostgres()
	rows, err := connection.Query(query)
	checkErr(err)
	defer rows.Close()

	for rows.Next() {
		var city_id, position_id, id int
		var name, url, prof_areas, specs, experience, vacancy_date, parsing_date, key_skills, salary_from, salary_to, platform  string
		err = rows.Scan(&id, &position_id, &city_id, &salary_from, &salary_to, &name, &url, &prof_areas, &specs, &experience, &key_skills, &vacancy_date, &parsing_date, &platform)
		checkErr(err)
		
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