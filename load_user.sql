use twitter_db;
load data local infile 'users.csv' into table user CHARACTER SET utf8mb4 columns terminated by '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;
