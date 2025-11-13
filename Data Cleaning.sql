create table layoffs_staging like layoffs;
select * from layoffs;

-- step 1: create a new table so that our raw table isnt hurt

insert layoffs_staging
select * from layoffs;

select * from layoffs_staging;

-- step2: remove duplicates

With Duplicates_Cte as
( 
select *, row_number() over( Partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
SELECT * FROM Duplicates_CTE WHERE row_num > 1;

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
select *, row_number() over( Partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

select * from layoffs_staging2 where row_num>1;

delete from layoffs_staging2 where row_num>1;

select * from layoffs_staging2 where row_num>1;

-- step 3: standardizing the data


update layoffs_staging2 set Industry = "Crypto" where industry like "Crypto%";

select * from layoffs_staging2;

select distinct company from layoffs_staging2 order by company;
update layoffs_staging2 set company = trim(company);

select distinct country from layoffs_staging2 order by country;
update layoffs_staging2 set country = 'United States' where country like 'United States%';

select `date` from layoffs_staging2;
Update layoffs_staging2 set date = str_to_date(`date`, '%m/%d/%Y') where date not like str_to_date(`date`, '%m/%d/%Y');
alter table layoffs_staging2 modify column 	`date` date;

select count(*) from layoffs_staging2;
select * from layoffs_staging2 where industry is null or industry = '';

select industry from layoffs_staging2 where company = 'airbnb';
update layoffs_staging2 set industry = 'Travel' where company = 'airbnb';

update layoffs_staging2 set industry = null where industry = '';

update layoffs_staging2 t1 
join layoffs_staging2 t2 
	on t1.company = t2.company
    and t1.location = t2.location
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

select * from layoffs_staging2;

alter table layoffs_staging2 drop column row_num; 

delete from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null