use twitter_db;
load data local infile 'p_hashtags.csv' into table popular columns terminated by '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;