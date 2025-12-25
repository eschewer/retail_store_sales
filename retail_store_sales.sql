-- Createad table for clean and smooth importing csv avoiding error,copy paste the data afterwards
CREATE TABLE `retail_store_sales` (
  `Transaction ID` text,
  `Customer ID` text,
  `Category` text,
  `Item` text,
  `Price Per Unit` text,
  `Quantity` text,
  `Total Spent` text,
  `Payment Method` text,
  `Location` text,
  `Transaction Date` text,
  `Discount Applied` text
);

SELECT *
FROM retail_store_sales;

-- Backup the dataset 
SELECT *
FROM bkretail_store_sales;

CREATE TABLE bkretail_store_sales
SELECT *
FROM retail_store_sales;

-- Standardized the format
ALTER TABLE retail_store_sales
MODIFY COLUMN `Price Per Unit` DECIMAL(10,2),
MODIFY COLUMN `Total Spent` DECIMAL(10,2),
MODIFY COLUMN `Quantity` INT;

ALTER TABLE retail_store_sales
ADD COLUMN order_date_clean DATE;

UPDATE retail_store_sales
SET order_date_clean = STR_TO_DATE(`Transaction Date`, '%m/%d/%Y');

ALTER TABLE retail_store_sales
MODIFY COLUMN order_date_clean DATE AFTER `Location`;

ALTER TABLE retail_store_sales
DROP COLUMN `Transaction Date`;

ALTER TABLE retail_store_sales
RENAME COLUMN order_date_clean TO `Transaction Date`;

SELECT *
FROM retail_store_sales;

-- Removed unnecessary records after verifying that rows with null 'Total Spent'
-- values also lacked 'Item' and 'Price Per Unit' data, ensuring no valuable information was lost.

SELECT *
FROM retail_store_sales
WHERE `Total Spent` is null;

DELETE
FROM retail_store_sales
WHERE `Total Spent` is null;

SELECT *
FROM retail_store_sales;

-- Identified the duplicate records
WITH DuplicateCte AS (
SELECT `Transaction ID`, ROW_NUMBER() OVER(PARTITION BY `Transaction ID`, `Customer ID`, `Category`, `Item`) as duplicate 
FROM retail_store_sales)
SELECT *
FROM DuplicateCte
WHERE duplicate>1;

-- Implemented a CTE for data validation to isolate NULL values.
-- This approach ensures data integrity by verifying target rows prior to the UPDATE rather than using COALESCE

SELECT
    `Category`,
    COUNT(*) AS null_item_count
FROM retail_store_sales
WHERE `Item` IS NULL AND `Price Per Unit` IS NULL
GROUP BY `Category`;

-- Fill NULL Price Per Unit by calculating (Total_Spent / Quantity)
SELECT *, ROUND((`Total Spent`/`Quantity`),2) AS Price
FROM retail_store_sales
WHERE `Item` IS NULL AND `Price Per Unit` IS Null
AND (`Quantity`>0 AND `Quantity` IS NOT NULL)
AND (`Total Spent`>0 AND `Total Spent` IS NOT NULL);

WITH PriceNull(`Transaction ID`,Price) AS (
SELECT `Transaction ID`,ROUND((`Total Spent`/`Quantity`),2) AS Price
FROM retail_store_sales
WHERE `Item` IS NULL AND `Price Per Unit` IS Null
AND (`Quantity`>0 AND `Quantity` IS NOT NULL)
AND (`Total Spent`>0 AND `Total Spent` IS NOT NULL)
)
UPDATE retail_store_sales r
RIGHT JOIN PriceNull p
    ON r.`Transaction ID` = p.`Transaction ID`
SET r.`Price Per Unit`= CONVERT(p.Price, DECIMAL(10,2));

-- Populate missing Item Names by looking up other records with the same Price Per Unit
SELECT `Transaction ID`, `Item`, `Category`, `Quantity`, `Price Per Unit`, `Total Spent`
FROM retail_store_sales
WHERE `Item` IS NULL
ORDER BY Category,`Price Per Unit`;

SELECT r1.`Item`, r1.`Category`, r1.`Quantity`, r1.`Price Per Unit`, r1.`Total Spent`,r2.`Item`, r2.`Category`, r2.`Quantity`, r2.`Price Per Unit`, r2.`Total Spent`
FROM retail_store_sales r1
LEFT JOIN retail_store_sales r2
	ON r1.`Category` = r2.`Category` AND r1.`Price Per Unit` = r2.`Price Per Unit` 
WHERE  r1.`Item` IS NULL
AND r2.`Item` IS NOT NULL
ORDER BY r1.Category,r1.`Price Per Unit`;

UPDATE retail_store_sales r1
LEFT JOIN retail_store_sales r2
	ON r1.`Category` = r2.`Category` AND r1.`Price Per Unit` = r2.`Price Per Unit` 
SET r1.`Item`= r2.`Item`
WHERE  r1.`Item` IS NULL
AND r2.`Item` IS NOT NULL;

SELECT `Item`,`Category`, `Price Per Unit`
FROM retail_store_sales
GROUP BY `Item`,`Category`,`Price Per Unit`
ORDER BY SUBSTRING(`Item`,6)+0,`Category`,`Price Per Unit`;
;

-- Featuring engineering
SELECT *, DAYNAME(`Transaction Date`) as day_of_theWeek, MONTHNAME(`Transaction Date`) as transaction_month
FROM retail_store_sales
ORDER BY (`Transaction Date`);

-- Exploratory Data Analysis

-- Ranking customers by their total spending
WITH findingRank AS (
SELECT `Customer ID`, SUM(`Total Spent`) as Total_Spent
FROM retail_store_sales
GROUP BY `Customer ID`
ORDER BY Total_Spent DESC)
SELECT *, DENSE_RANK() OVER(ORDER BY Total_Spent DESC) AS Ranking
FROM findingRank;

-- Top 5 Products by Sales Volume per Year
WITH top5_item(Item,`Year`,sold_qty) AS (
SELECT Item, YEAR(`Transaction Date`) as `Year`, SUM(Quantity) as sold_qty
FROM retail_store_sales
GROUP BY Item, `Year`),
findingRank AS
(SELECT * , DENSE_RANK() OVER(PARTITION BY `Year` ORDER BY sold_qty DESC) as ranking
FROM top5_item
)
SELECT *
FROM findingRank
WHERE ranking<=5;

-- Top 5 Products By Total Sales Volume
WITH top5_item(Item,sold_qty) AS (
SELECT Item, SUM(Quantity) as sold_qty
FROM retail_store_sales
GROUP BY Item),
findingRank AS
(SELECT * , DENSE_RANK() OVER(ORDER BY sold_qty DESC) as ranking
FROM top5_item
)
SELECT *
FROM findingRank
WHERE ranking<=5;

-- Top 5 Products By Total Sales Per Year
WITH top5_item(Item,`Year`,`Total Sales`) AS (
SELECT Item, YEAR(`Transaction Date`) as `Year`, SUM(`Total Spent`) as `Total Sales`
FROM retail_store_sales
GROUP BY Item, `Year`),
findingRank AS
(SELECT * , DENSE_RANK() OVER(PARTITION BY `Year` ORDER BY `Total Sales` DESC) as ranking
FROM top5_item
)
SELECT *
FROM findingRank
WHERE ranking<=5;

-- Top 5 Products By Total Sales
WITH top5_item(Item,`Total Sales`) AS (
SELECT Item, SUM(`Total Spent`) as `Total Sales`
FROM retail_store_sales
GROUP BY Item),
findingRank AS
(SELECT * , DENSE_RANK() OVER(ORDER BY `Total Sales` DESC) as ranking
FROM top5_item
)
SELECT *
FROM findingRank
WHERE ranking<=5;

-- Total Sales By Transaction Date
SELECT  `Transaction Date`, SUM(`Total Spent`) AS `Total Sales`
FROM retail_store_sales
GROUP BY `Transaction Date`
ORDER BY `Transaction Date`;

-- Monthly Total Sales
SELECT SUBSTRING(`Transaction Date`,1,7) AS `Month`, SUM(`Total Spent`) AS `Total Sales`
FROM retail_store_sales
GROUP BY `Month`
ORDER BY `Month`;

-- Rolling Total Monthly Sales
WITH Rolling_Total AS(
SELECT SUBSTRING(`Transaction Date`,1,7) AS `Month`, SUM(`Total Spent`) AS `Total Sales`
FROM retail_store_sales
GROUP BY `Month`
ORDER BY `Month`)
SELECT `Month`, `Total Sales`, SUM(`Total Sales`) OVER(ORDER BY `Month`) as Rolling_Total
FROM Rolling_Total;

-- Annual Total Sales
SELECT YEAR(`Transaction Date`) as `Year`, SUM(`Total Spent`) AS `Total Sales`
FROM retail_store_sales
GROUP BY `Year`
ORDER BY `Year`;

-- Top 2 Months By Total Sales
WITH rankingMonth(`Year`,`Month`,`Total Sales`) AS (
SELECT YEAR(`Transaction Date`) as `Year`, MONTHNAME(`Transaction Date`) as `Month`, SUM(`Total Spent`) AS `Total Sales`
FROM retail_store_sales
GROUP BY `Month`, `Year`
ORDER BY `Total Sales` DESC),
findingRank AS(
SELECT *, DENSE_RANK() OVER(PARTITION BY `Year` ORDER BY `Total Sales` DESC) as Ranking
FROM rankingMonth
)
SELECT *
FROM findingRank
WHERE Ranking<3;

-- Ranking of Days of the Week by Total Sales
SELECT DAYNAME(`Transaction Date`) as `Day`, SUM(`Total Spent`) AS `Total Sales`
FROM retail_store_sales
GROUP BY `Day`
ORDER BY `Total Sales` DESC;

-- Payment Method Count
SELECT
  `Payment Method`,
  COUNT(*) AS PymntMthd_Count,
  DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS Ranking
FROM retail_store_sales
GROUP BY `Payment Method`
ORDER BY Ranking;

-- In-Store vs. Online Sales Comparison
SELECT `Location`, SUM(`Total Spent`) AS `Total Sales`
FROM retail_store_sales
GROUP BY `Location`
ORDER BY `Total Sales` DESC;

