-- DATA CLEANING

select * from layoffs

-- 1. Remove Duplicates
-- 2. Standardise the data
-- 3. NULL/blank values
-- 4. Remove unneccesary columns 

create table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert layoffs_staging
select * from layoffs;

select *,
row_number() over(partition by company, location, total_laid_off, percentage_laid_off, `date`) as row_num -- `date` cos date is a function, so `date` indentified as a column name
from layoffs_staging;

with duplicate_cte as 
(select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging)
select * from duplicate_cte
where row_num >= 2;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

delete from layoffs_staging2
where row_num > 1;

select * from layoffs_staging2;

-- STANDARDISE THE DATA

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select `date`
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

-- NULL & BLANK Values
select *
from layoffs_staging2
where industry IS NULL
or industry = '';
-- individually populate null & blank values
select *
from layoffs_staging2
where company = 'Airbnb';

update layoffs_staging2
set industry = 'Travel'
where company = 'Airbnb';

-- update all null & blank values 
update layoffs_staging2
set industry = null 
where industry = '';

select t1.company, t1.location, t1.industry, t2.company, t2.location, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
	and t1.location = t2.location
where t1.industry is null
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
	and t1.location = t2.location
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select *
from layoffs_staging2
where industry is null; -- unpopulated row

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- DELETE UNNECESSARY COLUMNS & ROWS
delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;


-- EDA

select * 
from layoffs_staging2;

select min(total_laid_off), max(total_laid_off)
from layoffs_staging2;

select industry, sum(total_laid_off) 
from layoffs_staging2
group by industry
order by 2 desc;

select country, sum(total_laid_off) 
from layoffs_staging2
group by country
order by 2 desc;

select year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by year(`date`)
order by 2 desc;

-- progression of world layoffs using rolling total

select substring(`date`,1,7) as `month`, sum(total_laid_off) as sum_laid_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1;

with rolling_total as 
(select substring(`date`,1,7) as `month`, sum(total_laid_off) as sum_laid_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1)
select `month`, sum_laid_off, sum(sum_laid_off) over(order by `month`) as rolling_total
from rolling_total
order by 1;

-- companies' layoffs by years

select company, sum(total_laid_off)
from layoffs_staging2
group by company
having sum(total_laid_off) is not null;

-- rank the companies by total_laid_off in each year
with company_by_year as 
(select company, year(`date`) as `year`, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by company, year(`date`))
select *, dense_rank() over(partition by `year` order by total_laid_off desc) as ranking
from company_by_year
where `year` is not null
order by ranking;

with company_by_year as 
(select company, year(`date`) as `year`, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by company, year(`date`)),
company_layoffs_ranking as
(select *, dense_rank() over(partition by `year` order by total_laid_off desc) as ranking
from company_by_year
where `year` is not null)
select * 
from company_layoffs_ranking
where ranking <= 5;

