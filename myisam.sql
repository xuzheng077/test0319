create database if not exists kaggle_db;
use kaggle_db;
drop table if exists weather_myisam;
create table weather_myisam (
    wsid INT not null,
    wsnm varchar(20),
    elvt FLOAT(10,2),
    lat  FLOAT(10,6),
    lon  FLOAT(10,6),
    inme FLOAT(10,6),
    city varchar(20),
    prov varchar(5),
    mdct DATETIME not null,
    date DATE,
    yr   INT,
    mo   TINYINT,
    da   TINYINT, 
    hr   TINYINT,
    prcp FLOAT(10,3),
    stp  FLOAT(10,1),
    smax FLOAT(10,1),
    smin FLOAT(10,1),
    gbrd FLOAT(10,3),
    temp FLOAT(10,1),
    dewp FLOAT(10,3),
    tmax FLOAT(10,3),
    dmax FLOAT(10,3),
    tmin FLOAT(10,3),
    dmin FLOAT(10,3),
    hmdy FLOAT(10,3),
    hmax FLOAT(10,3),
    hmin FLOAT(10,3),
    wdsp FLOAT(10,3),
    wdct FLOAT(10,3),
    gust FLOAT(10,3),
    primary key (wsid, mdct)
) engine=MyISAM;

load data local infile 'weather_brazil.csv' into table weather_myisam columns terminated by ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;
