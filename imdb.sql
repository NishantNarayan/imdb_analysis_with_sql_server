CREATE DATABASE imdb

USE imdb

USE [imdb]
GO

-- 1. Select 100 rows from all tables 
SELECT TOP 100 * FROM artist_name
SELECT TOP 100 * FROM crew
SELECT TOP 100 * FROM principals 
SELECT TOP 100 * FROM ratings
SELECT TOP 100 * FROM titleDetail

-- 2. View all the constraints
SELECT * FROM sys.objects
WHERE type_desc LIKE '%CONSTRAINT'

-- 3. Adding Foregin Key in the principals table (title)
ALTER TABLE principals
ADD CONSTRAINT fk_title_principal 
FOREIGN KEY (tconst) REFERENCES titleDetail(tconst)

-- 4. Adding Foregin Key in the principals table (artist)
ALTER TABLE principals
ADD CONSTRAINT fk_artist_principal 
FOREIGN KEY (nconst) REFERENCES artist_name(nconst) -- doesn't work due to discrepancy

-- 5. See what doesn't match between artist table and principal table
SELECT nconst FROM principals
WHERE nconst NOT IN
(SELECT nconst FROM artist_name)-- this is why a foreign key constraint failed! Never mind :P


-- 6. Handle \N
-- OR a better way to handle \N in this case would be to delete all those rows 
-- that doesn't have a start or a end year (to cut down size)
BEGIN TRAN
DELETE FROM titleDetail WHERE tconst IN (Select tconst FROM titleDetail WHERE startYear = '\N' OR endYear = '\N')
ROLLBACK TRAN

-- 7. Deleting title info related to the ones deleted in the previous line of code on other tables as well
-- CREW
DELETE FROM crew WHERE tconst NOT IN (SELECT tconst FROM titleDetail)
-- PRINCIPALS 
DELETE FROM principals WHERE tconst NOT IN (SELECT tconst FROM titleDetail)
-- RATINGS
DELETE FROM ratings WHERE tconst NOT IN (SELECT tconst FROM titleDetail)

-- Now, titleDetail table has no NULL in the year columns, so we go ahead and convert it into small int :)
ALTER TABLE titleDetail
ALTER COLUMN 
startYear smallint

ALTER TABLE titleDetail
ALTER COLUMN 
endYear smallint


-- Artist table
SELECT * FROM artist_name WHERE birthYear ='\N' AND deathYear = '\N'
SELECT * FROM artist_name WHERE birthYear ='\N' OR deathYear = '\N'

UPDATE artist_name 
SET birthYear= NULL
WHERE birthYear = '\N'

ALTER TABLE artist_name
ALTER COLUMN 
birthYear smallint

UPDATE artist_name 
SET deathYear=  NULL
WHERE deathYear = '\N'

ALTER TABLE artist_name
ALTER COLUMN 
deathYear smallint

-- Principals table
-- SELECT DISTINCT(job) FROM principals
SELECT * FROM principals WHERE job != '\N' AND characters != '\N' -- no rows returned
-- But it is better to keep them as it is for now!

-- 8. Row count after size cut down
SELECT SUM(@@ROWCOUNT) FROM artist_name -- 1656
SELECT SUM(@@ROWCOUNT) FROM principals -- 529260
SELECT SUM(@@ROWCOUNT) FROM ratings -- 35658
SELECT SUM(@@ROWCOUNT) FROM crew -- 85782
SELECT SUM(@@ROWCOUNT) FROM titleDetail -- 85782

-- 9. Title with its cast 

SELECT t.primaryTitle AS 'Title Name', category, characters AS 'Character Name', 
primaryName AS 'Artist Name', t.startYear as 'Year'
FROM artist_name a 
JOIN principals p
ON a.nconst = p.nconst
JOIN titleDetail t
ON p.tconst = t.tconst
ORDER BY t.primaryTitle 


-- 10. Unpivot using STRING SPLIT function and create new tables for title and profession
SELECT [nconst] ,[primaryName], value AS Title
INTO artist_and_title 
FROM artist_name CROSS APPLY STRING_SPLIT(knownForTitles, ',')

SELECT [nconst] ,[primaryName], value Profession
INTO artist_and_profession
FROM artist_name CROSS APPLY STRING_SPLIT(primaryProfession, ',')

SELECT COUNT(*) AS actors_count FROM artist_and_profession 
WHERE Profession = 'actor'

SELECT COUNT(*) AS musicDept_count FROM artist_and_profession 
WHERE Profession LIKE 'music%'

-- 11. Unpivot using STRING SPLIT function and create new tables for writers and directors
SELECT tconst ,value AS writer
INTO title_and_writer
FROM crew CROSS APPLY STRING_SPLIT(writers, ',')

SELECT tconst , value AS director
INTO title_and_director
FROM crew CROSS APPLY STRING_SPLIT(directors, ',')


SELECT tconst, value as director , writer 
INTO title_directorWithWriter
FROM title_and_writer CROSS APPLY STRING_SPLIT(directors, ',')

SELECT TOP 100 * FROM title_directorWithWriter

-- 12. Title & Combined Directors using CTE and JOIN
WITH tempTable (Title, Director) AS
(SELECT primaryTitle, a.primaryName AS director_name
FROM 
titleDetail t
JOIN
title_and_director d
ON t.tconst=d.tconst
JOIN
artist_name a
ON a.nconst=d.director)

SELECT Title, STRING_AGG(Director, ', ') AS Combined_Directors
FROM tempTable
GROUP BY Title;

-- 13. Title & Combined Writers using CTE and JOIN
WITH tempTableW (Title, Writer) AS
(SELECT TOP 100 primaryTitle, b.primaryName AS writer_name 
FROM 
titleDetail t
JOIN
title_and_writer d
ON t.tconst=d.tconst
JOIN
artist_name b
ON b.nconst = d.writer)

SELECT TOP 10 Title, STRING_AGG(Writer, ', ') AS Combined_Writers
FROM tempTableW
GROUP BY Title;

-- 14. Ratings table analysis using Bayesian average

-- Range (5-1922255)
SELECT numVotes FROM ratings WHERE numVotes IN (SELECT MAX (numVotes) FROM ratings)
SELECT numVotes FROM ratings WHERE numVotes IN (SELECT MIN (numVotes) FROM ratings)

-- To find 25th percentile for Bayesian average (17 or 18, let's set c as 18) 
SELECT numVotes, cast(percent_rank()  over (ORDER BY numVotes) AS decimal(3,2)) AS 'Percentile'
FROM ratings 

-- To find m (overall average m= 6.9)
SELECT avg(averageRating) AS average FROM ratings

-- Sorting ratings by bayesian average and creating a view with encryption and index creation
GO
CREATE VIEW dbo.RatingSummaryByTitle WITH SCHEMABINDING AS
SELECT t.tconst, titleType, primaryTitle, genres, averageRating, numVotes, startYear, endYear,
cast((((averageRating*numVotes)+(18*6.9))/(numVotes+18)) AS decimal(3,2)) AS Bays_Rating 
FROM dbo.titleDetail t
JOIN dbo.ratings r
ON t.tconst = r.tconst
--ORDER BY Bays_Rating DESC
GO

DROP VIEW RatingSummaryByTitle

SELECT TOP 10 * FROM RatingSummaryByTitle 
ORDER BY Bays_Rating DESC

CREATE UNIQUE CLUSTERED INDEX indx_RatingSummary on dbo.RatingSummaryByTitle(tconst)

-- 15. Title Detail table genre string split

SELECT tconst, primaryTitle, value as genre 
INTO titleDetailGenre
FROM titleDetail CROSS APPLY STRING_SPLIT(genres, ',')

SELECT TOP 100 * FROM titleDetailGenre

-- 16. Analyse the genre from the above created table

SELECT genre, COUNT(*) AS genre_count 
FROM titleDetailGenre
WHERE genre != '\N'
GROUP BY genre
ORDER BY genre_count DESC

-- 17. Creating a basic Delete Trigger to cascade the delete action in the dependent tables
GO
CREATE OR ALTER TRIGGER CascaseDeletion
    ON [dbo].[artist_name]
    AFTER DELETE
    AS
    BEGIN
		DECLARE @id as varchar(50)
		SELECT @id = nconst FROM deleted
		DELETE FROM principals WHERE nconst = @id
		SELECT *, 'Deleted from dependent table too' AS 'Message' FROM deleted
    SET NOCOUNT ON
    END

BEGIN TRAN
DELETE FROM artist_name WHERE nconst = 'nm0000001'
COMMIT TRAN

SELECT * FROM artist_name WHERE nconst = 'nm0000001' -- deleted 
SELECT * FROM principals WHERE nconst = 'nm0000001' -- no records after trigger was exectued


SELECT * FROM title_directorWithWriter

-- 18. Using UNION to combine title and director table
WITH cte_directorOrWriter (title_id, lead_crew) AS 
(SELECT * FROM title_and_director
UNION
SELECT * FROM title_and_writer)


SELECT title_id, lead_crew, primaryName  
FROM cte_directorOrWriter c
JOIN artist_name a
ON c.lead_crew = a.nconst
WHERE lead_crew != '\N' AND title_id = 'tt0056785'

-- 19. Using switch case to categorize the titles based on Bays Rating
SELECT primaryTitle, Bays_Rating, 
CASE WHEN Bays_Rating> 9 THEN 'Highly rated'
			WHEN Bays_Rating BETWEEN 7 AND 9 THEN 'Moderately Rated'
			WHEN Bays_Rating< 7 THEN 'Below Average'
END AS Category
FROM dbo.RatingSummaryByTitle 

-- 20. SELECTING NEWLY CREATED TABLES

SELECT TOP 100 * FROM [dbo].[artist_and_profession]
SELECT TOP 100 * FROM [dbo].[artist_and_title]
SELECT TOP 100 * FROM [dbo].[title_and_director]
SELECT TOP 100 * FROM [dbo].[title_and_writer]
SELECT TOP 100 * FROM [dbo].[title_directorWithWriter]
SELECT TOP 100 * FROM [dbo].[titleDetailGenre]


-- 21. Using Merge to combine the titles and characters in which an artist has worked.

SELECT * 
INTO artist_details_with_characters -- creating a copy before merging
FROM artist_name

BEGIN TRAN
MERGE INTO artist_details_with_characters as d
USING (SELECT nconst, STRING_AGG(characters, ',') AS agg_characters FROM principals
GROUP BY nconst) as w
ON d.nconst = w.nconst
WHEN MATCHED THEN
	UPDATE SET knownForTitles = CONCAT('Titles: ', knownForTitles, ',', 'Characters: ', agg_characters)
WHEN NOT MATCHED BY SOURCE THEN
	UPDATE SET knownForTitles = CONCAT('Titles: ', knownForTitles, ',', 'Characters: ', 'NULL');
COMMIT TRAN

EXEC sp_rename 'dbo.artist_details_with_characters.knownForTitles', 
'TitlesAndCharacters', 'COLUMN'; -- renaming the column to fit its content

-- 22. Using procedure with if and while
if OBJECT_ID('titleInfoBtwYears','P') IS NOT NULL
DROP PROC titleInfoBtwYears
GO
CREATE PROC titleInfoBtwYears (@YearFrom int, @YearTo int) AS
BEGIN
	if exists(SELECT * FROM titleDetail WHERE startyear BETWEEN @YearFrom AND @YearTo)
	BEGIN
		SELECT t.primaryTitle AS 'Title Name', category, characters AS 'Character Name', 
		primaryName AS 'Artist Name', t.startYear as 'Year'
		FROM artist_name a 
		JOIN principals p
		ON a.nconst = p.nconst
		RIGHT JOIN titleDetail t
		ON p.tconst = t.tconst
		WHERE t.startYear BETWEEN @YearFrom AND @YearTo
		ORDER BY t.startYear
	END
	ELSE
	BEGIN
		PRINT 'No records found!'
	END
END
GO
titleInfoBtwYears 1950, @YearTo=2000 -- Calling the procedure
GO
titleInfoBtwYears 2030, @YearTo=2036 -- Calling the procedure to test the fail case

-- 23. title type wise average rating summary using OVER()
SELECT DISTINCT titleType
, avg(Bays_Rating) OVER(PARTITION BY titleType) AS avg_rating 
FROM RatingSummaryByTitle

-- 24. year wise average rating summary using OVER()
SELECT DISTINCT startYear
, avg(Bays_Rating) OVER(PARTITION BY startYear) AS avg_rating 
FROM RatingSummaryByTitle
ORDER BY startYear DESC

