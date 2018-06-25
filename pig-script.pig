mydata1 = LOAD '/user/hadoop/bigrams/googlebooks-eng-us-all-2gram-20120701-i?' AS (ngram:chararray,year:int,occurrences:float,books:float);
mydata2 = GROUP mydata1 BY ngram;
mydata3 = FOREACH mydata2 GENERATE group,SUM(mydata1.occurrences) AS total_occur,SUM(mydata1.books) AS total_books,SUM(mydata1.occurrences)/SUM(mydata1.books) AS avg_occur,MIN(mydata1.year) AS first_year,MAX(mydata1.year) AS last_year,COUNT(mydata1.year) as total_year;
mydata4 = FILTER mydata3 BY ((first_year == 1950) AND (total_year == 60));
mydata5 = ORDER mydata4 BY avg_occur DESC;
mydata6 = LIMIT mydata5 10;
STORE mydata6 INTO 'assign03pig';
