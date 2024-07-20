#Data cleaning
#SELECT *
#FROM layoffs;
#1.Remove Duplicates
#2.standardize the Data
#3.Null values or blank values
#4.Remove Any Columns
CREATE TABLE layoffs_Staging
LIKE layoffs;
SELECT *
FROM layoffs_staging;


INSERT layoffs_Staging
SELECT *
FROM layoffs;
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num>1;

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
SELECT *
FROM layoffs_staging2
WHERE row_num >1;
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num >1;
SELECT *
FROM layoffs_staging2;

#Standardizing data
SELECT company,trim(company)
FROM layoffs_staging2;
UPDATE layoffs_staging2
SET company =trim(company);


SELECT *
FROM layoffs_staging2
WHERE industry LIKE'Crypto';




UPDATE layoffs_staging2
SET industry ='Crypto'
WHERE industry LIKE 'Crypto%';


SELECT DISTINCT Country, trim(TRAILING '.'FROM Country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET Country = trim(TRAILING '.'FROM Country)
WHERE Country LIKE 'United States%' ;
SELECT DISTINCT Country
FROM layoffs_staging2
ORDER BY 1;

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;
#REMOVING NULL AND BLANK VAUES
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';


SELECT *
FROM layoffs_staging2 
WHERE industry = null
OR industry = '';

SELECT *
FROM layoffs_staging2 
WHERE company LIKE 'Bally%';


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
   ON t1.company = t2.company
   SET t1.industry = t2.industry
   WHERE t1.industry IS NULL
   AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2 ;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

