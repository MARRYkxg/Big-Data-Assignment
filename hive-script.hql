create external table mydata (ngram string,year int,occurrences float,books float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/hadoop/assign03hive';

LOAD DATA INPATH '/user/hadoop/bigrams/googlebooks-eng-us-all-2gram-20120701-i?' OVERWRITE INTO TABLE mydata;

INSERT OVERWRITE DIRECTORY 'prob2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT ngram,SUM(occurrences) AS total_occur,SUM(books) AS total_books,SUM(occurrences)/SUM(books) AS avg_occur,MIN(year) AS first_year,MAX(year) AS last_year,COUNT(year) as total_year
FROM mydata
GROUP BY ngram
HAVING first_year==1950 AND total_year==60
ORDER BY avg_occur DESC
LIMIT 10;
