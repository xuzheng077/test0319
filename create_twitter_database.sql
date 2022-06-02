-- MySQL version: 5.7.11+
-- Usage:
-- mysql --username=username -h db_hostname --port db_port --password=user_password --local-infile=1 < create_twitter_database.sql

-- create the database
drop database if exists twitter_db;
create database twitter_db;
use twitter_db;

-- create the tweet table
drop table if exists `tweet`;
create table `tweet` (
	`created_at` BIGINT default null,
    `id_str` BIGINT not null,
    `tweet_text` MEDIUMTEXT default null,
    `in_reply_to_user_id_str` BIGINT default 0,
    `retweeted_user_id_str` BIGINT default 0,
    `user_id_str` BIGINT default 0,
    `hashtags` MEDIUMTEXT default null,
	primary key (id_str),
    INDEX user_id_index (user_id_str)
) engine=MyISAM DEFAULT CHARACTER SET=utf8mb4;

drop table if exists `user`;
create table `user` (
                         `created_at` BIGINT default null,
                         `user_id_str` BIGINT default 0,
                         `user_screen_name` varchar(200) default null,
                         `user_description` MEDIUMTEXT default null,
                         primary key (user_id_str)
) engine=MyISAM DEFAULT CHARACTER SET=utf8mb4;

drop table if exists `popular`;
create table `popular` (
    `tag` varchar(150)
) engine=MyISAM DEFAULT CHARACTER SET=utf8mb4;

-- load data to the tweet table
load data local infile 'tweets.csv' into table tweet CHARACTER SET utf8mb4 columns terminated by '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;


