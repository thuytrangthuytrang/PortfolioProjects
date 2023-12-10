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


-- Total Cases vs Population
-- Shows what percentage of population infected with Covid

Select Location, date, Population, total_cases,  
(total_cases/population)*100 as PercentPopulationInfected
From CovidDeaths
Where location like 'Vietnam'
order by 1,2


-- Countries with Highest Infection Rate compared to Population
Select Location, Population, MAX(total_cases) as HighestInfectionCount, 
Max((total_cases/population))*100 as PercentPopulationInfected
From CovidDeaths
where total_cases is not null
Group by Location, Population
order by MAX(total_cases) desc


-- Countries with Highest Death Count per Population--

Select Location, MAX(Total_deaths) as TotalDeathCount
From CovidDeaths
Where continent is not null and Total_deaths is not null 
Group by Location
order by TotalDeathCount desc


-- BREAKING THINGS DOWN BY CONTINENT

-- Showing contintents with the highest death count per population

Select continent, MAX(cast(Total_deaths as int)) as TotalDeathCount
From CovidDeaths
Where continent is not null 
Group by continent
order by TotalDeathCount desc

	
-- GLOBAL NUMBERS

Select SUM(new_cases) as total_cases, 
	SUM(cast(new_deaths as int)) as total_deaths, 
	SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
From CovidDeaths
where continent is not null 
Group By date
order by 1,2


-- Total Population vs Vaccinations
-- Shows Percentage of Population that has recieved at least one Covid Vaccine

Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(cast(vac.new_vaccinations as int )) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From CovidDeaths dea
Join CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null and vac.new_vaccinations is not null
order by 2,3

-- Total Population vs Vaccinations
-- Shows Percentage of Population that has recieved at least one Covid Vaccine

Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(cast(vac.new_vaccinations as int )) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From CovidDeaths dea
Join CovidVaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null and vac.new_vaccinations is not null
order by 2,3

