-- SELECT * 
-- FROM world_layoffs.layoffs; 

-- 			OR

-- SELECT * 
-- FROM layoffs;

SELECT * 
FROM layoffs;

-- Step 1: Remove Duplicates if there are any
-- Step 2: Standarize the Data (means if there are any spelling mistake or something like that)
-- Step 3: Null Values or blank values 
-- Step 4: Remove any columns (i.e. unnecessary columns )

-- NOTE for step 4: When we work in a company than most of the times the data are automatically imported in our database at that time we cannot 
-- 				remove any columns because if we remove the columns for data cleaning process than it may reflact in the system from where the data
--              is imported so to solve this problem we can create a copy of the data and than we can remove the columns if we want. 

-- Here we will create another table so that our main data cannot be affected.

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

-- Now the "layoffs_staging" table has been created but the values inside the table is not inserted.

-- Now we will insert the values or records into the layoffs_staging table from our main table (i.e. "layoffs").

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs;

-- Now the values from layoffs table has been insertd into the layoffs_staging table.

-- Now we will work only with the 'layoffs_staging' table so that our raw data cannot be affected (i.e. in 'layoffs' table)

SELECT *
FROM layoffs_staging;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;


CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging3;

INSERT INTO layoffs_staging3
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging3
WHERE row_num > 1;

DELETE
FROM layoffs_staging3
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging3
WHERE row_num > 1;

SELECT *
FROM layoffs_staging3;


-- Standardizing the Data

SELECT company, TRIM(company)
FROM layoffs_staging3;

UPDATE layoffs_staging3
SET company = TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_staging3
ORDER BY industry;

SELECT *
FROM layoffs_staging3
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging3
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT * 
FROM layoffs_staging3
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT(country)
FROM layoffs_staging3
ORDER BY country;

SELECT *
FROM layoffs_staging3
WHERE country LIKE 'United States%'
ORDER BY country;


SELECT DISTINCT(country), TRIM(TRAILING '.' FROM country)
FROM layoffs_staging3
ORDER BY country;

UPDATE layoffs_staging3
SET country = 'United States'
WHERE country LIKE 'United States%';

SELECT DISTINCT(country), TRIM(TRAILING '.' FROM country)
FROM layoffs_staging3
ORDER BY country;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging3;

UPDATE layoffs_staging3
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date` 
FROM layoffs_staging3;

-- NOTE: After updating the date column still the data type of date cloumn is not change (i.e  text). 
-- 	   To change the data type of date column we need to use the ALTER.

ALTER TABLE layoffs_staging3
MODIFY COLUMN `date` DATE;
 
SELECT * 
FROM layoffs_staging3;

SELECT *
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging3
WHERE industry IS NULL 
OR industry = '';

SELECT *
FROM layoffs_staging3
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging3 AS t1
JOIN layoffs_staging3 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging3 AS t1
JOIN layoffs_staging3 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging3 AS t1
JOIN layoffs_staging3 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- NOTE: We have updated the industry column but the records are not updated in it.
-- To update the records in it first we need to UPDATE that particular as NULL from blank row than only we can UPDATE the new records 

UPDATE layoffs_staging3
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging3 AS t1
JOIN layoffs_staging3 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging3 AS t1
JOIN layoffs_staging3 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging3
WHERE industry IS NULL 
OR industry = '';

SELECT *
FROM layoffs_staging3
WHERE company LIKE 'Bally%';

SELECT * 
FROM layoffs_staging3
WHERE totaL_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging3
WHERE totaL_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging3
WHERE totaL_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging3;

-- Now we want to drop the column "row_num" which is of no use so first we will ALTER the table than we will DROP the column

ALTER TABLE layoffs_staging3
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging3;
