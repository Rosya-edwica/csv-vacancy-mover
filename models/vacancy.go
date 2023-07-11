package models


type Vacancy struct {
	Id           int
	Url          string
	Title        string
	PositionName string
	PositionId   int
	CityId       int
	Experience   string
	SalaryFrom   string
	SalaryTo     string
	Areas        string
	Specs        string
	Skills    	string
	VacancyDate  string
	ParsingDate  string
	Platform     string
}