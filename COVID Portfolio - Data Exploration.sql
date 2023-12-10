-----tạo table, import dữ liệu từ cvs--
CREATE TABLE CovidDeaths
(
    iso_code varchar,
    continent varchar,
    location varchar,
    date  date,
    population numeric,
    total_cases numeric,
    new_cases numeric,
    new_cases_smoothed float,
    total_deaths numeric,
	new_deaths numeric,
	new_deaths_smoothed float,
	total_cases_per_million float,
	new_cases_per_million float,
	new_cases_smoothed_per_million float,
	total_deaths_per_million float,
	new_deaths_per_million float,
	new_deaths_smoothed_per_million float, 
	reproduction_rate float
);

CREATE TABLE CovidVaccinations
(
    iso_code varchar,
    continent varchar,
    location varchar,
    date  date,
    new_vaccinations varchar
);
--- --Select * from PortfolioProjects..CovidVaccination
--order by 3,4---

Select * from CovidDeaths
where continent is not null
order by location, date

  
--- Select Data that we are going to be starting with--

Select Location, date, total_cases, new_cases, total_deaths, population
From CovidDeaths
Where continent is not null 
order by 1,2

  
--Looking at total cases vs total deaths
---Shows likelihood of dying if you contract covid in Vietnam---
  
Select Location, date, total_cases,total_deaths, (total_deaths/total_cases)*100 as DeathPercentage
from CovidDeaths
where location = 'Vietnam'
order by 1,2
