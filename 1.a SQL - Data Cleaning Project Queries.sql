-- DATA CLEANINING PROJECT FOR FIRING DATA RECORDS

-- N.B: Project description in the comment above 


-- Steps:
-- 1. Remove duplicates
-- 2. Standarize the Data
-- 3. Null or blank values
-- 4. Remove any columns


SELECT *
FROM layoffs_staging;

-- 1. Remove duplicates

-- create the exact format table
CREATE TABLE layoffs_staging
LIKE layoffs;

-- backup table
INSERT layoffs_staging
SELECT *
FROM layoffs;


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

-- this easy way does not work on CTE table
DELETE 
FROM duplicate_cte
WHERE row_num > 1;

-- create new table with extra row for row_num
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;


DELETE 
FROM layoffs_staging2
WHERE row_num > 1;


SELECT *
FROM layoffs_staging2;


SELECT COUNT(*)
FROM layoffs_staging2;


--  2. Standarize the Data

-- company column

SELECT company, TRIM(company)
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET company = TRIM(company);

-- industry column
SELECT DISTINCT industry 
FROM layoffs_staging2
ORDER BY 1;


SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- location column
SELECT DISTINCT location 
FROM layoffs_staging2
ORDER BY 1;


SELECT * 
FROM layoffs_staging2
WHERE location LIKE 'bru%';

-- country column
SELECT country 
FROM layoffs_staging2
WHERE country = 'United States'
ORDER BY 1;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

-- you can also use SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) and then FROM ...
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);


--  3. Null or blank values

-- date column
SELECT DISTINCT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- dealing with null dates that are not null in their formats

UPDATE layoffs_staging2
SET `date` = NULL
WHERE `date` IS NOT NULL AND (`date` = 'N/A' OR `date` = 'Invalid' or `date` = 'NULL' OR TRIM(`date`) = '');


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- total_laid_off
SELECT COUNT(*)
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

UPDATE layoffs_staging2
SET total_laid_off = NULL
WHERE total_laid_off IS NOT NULL AND total_laid_off = 'NULL';

UPDATE layoffs_staging2
SET percentage_laid_off = NULL
WHERE percentage_laid_off IS NOT NULL AND percentage_laid_off = 'NULL';


-- or another way by using cases to do them at once
UPDATE layoffs_staging2
SET 
    total_laid_off = CASE
                        WHEN total_laid_off = 'NULL' THEN NULL
                        ELSE total_laid_off
                     END,
    percentage_laid_off = CASE
                             WHEN percentage_laid_off = 'NULL' THEN NULL
                             ELSE percentage_laid_off
                          END,
    stage = CASE
               WHEN stage = 'NULL' THEN NULL
               ELSE stage
            END,
	industry = CASE
               WHEN industry = 'NULL' THEN NULL
               ELSE industry
            END,
    funds_raised_millions = CASE
                               WHEN funds_raised_millions = 'NULL' THEN NULL
                               ELSE funds_raised_millions
                            END
WHERE 
    total_laid_off = 'NULL' OR
    percentage_laid_off = 'NULL' OR
    stage = 'NULL' OR
    industry = 'NULL' OR
    funds_raised_millions = 'NULL';


-- Industry
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = ''
;


SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


--  4. Remove any columns

SELECT *
FROM layoffs_staging2;


DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;


ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
