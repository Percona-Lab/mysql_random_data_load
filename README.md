# Random data generator for MySQL

Many times in my job i need to generate random data for a specific table in order to reproduce an issue.  
After writing many random generators for every table, I decided to write a random data generator, able to get the table structure and generate random data for it.  
Plase take into consideration that this is the first version and it doesn't support all field types yet!  

**NOTICE**  
This is an early stage project.  

## Supported fields:

|Field type|Generated values|
|----------|----------------|
|tinyint|0 ~ 0xFF|
|smallint|0 ~ 0XFFFF|
|mediumint|0 ~ 0xFFFFFF|
|int - integer|0 ~ 0xFFFFFFFF|
|bigint|0 ~ 0xFFFFFFFFFFFFFFFF|
|float|0 ~ 1e8|
|decimal(m,n)|0 ~ 10^(m-n)|
|double|0 ~ 1000|
|char(n)|up to n random chars|
|varchar(n)|up to n random chars|
|date|NOW() - 1 year ~ NOW()|
|datetime|NOW() - 1 year ~ NOW()|
|timestamp|NOW() - 1 year ~ NOW()|
|time|00:00:00 ~ 23:59:59|
|year|Current year - 1 ~ current year|
|blob|up to 100 chars random paragraph|
|text|up to 100 chars random paragraph|
|mediumblob|up to 100 chars random paragraph|
|mediumtext|up to 100 chars random paragraph|
|longblob|up to 100 chars random paragraph|
|longtext|up to 100 chars random paragraph|
|varbinary|up to 100 chars random paragraph|
|enum|A random item from the valid items list|
|set|A random item from the valid items list|

### How strings are generated

- If field size < 10 the program generates a random "first name"
- If the field size > 10 and < 30 the program generates a random "full name"
- If the field size > 30 the program generates a "lorem ipsum" paragraph having up to 100 chars.
 
The program can detect if a field accepts NULLs and if it does, it will generate NULLs ramdomly (~ 10 % of the values).

## Usage
`random-data-generator <database> <table> <number of rows> [options...]`

## Options
|Option|Description|
|------|-----------|
|--host|Host name/ip|
|--port|Port number|
|--user|Username|
|--password|Password|
|--bulk-size|Number of rows per INSERT statement (Default: 1000)|
|--debug|Show some debug information|

### Example
```
CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE `test`.`t3` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `tcol01` tinyint(4) DEFAULT NULL,
  `tcol02` smallint(6) DEFAULT NULL,
  `tcol03` mediumint(9) DEFAULT NULL,
  `tcol04` int(11) DEFAULT NULL,
  `tcol05` bigint(20) DEFAULT NULL,
  `tcol06` float DEFAULT NULL,
  `tcol07` double DEFAULT NULL,
  `tcol08` decimal(10,2) DEFAULT NULL,
  `tcol09` date DEFAULT NULL,
  `tcol10` datetime DEFAULT NULL,
  `tcol11` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `tcol12` time DEFAULT NULL,
  `tcol13` year(4) DEFAULT NULL,
  `tcol14` varchar(100) DEFAULT NULL,
  `tcol15` char(2) DEFAULT NULL,
  `tcol16` blob,
  `tcol17` text,
  `tcol18` mediumtext,
  `tcol19` mediumblob,
  `tcol20` longblob,
  `tcol21` longtext,
  `tcol22` mediumtext,
  `tcol23` varchar(3) DEFAULT NULL,
  `tcol24` varbinary(10) DEFAULT NULL,
  `tcol25` enum('a','b','c') DEFAULT NULL,
  `tcol26` set('red','green','blue') DEFAULT NULL,
  `tcol27` float(5,3) DEFAULT NULL,
  `tcol28` double(4,2) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;
```
To generate 100K random rows, just run:
```
random-data-load test t3 100000 --max-threads=8 --user=root --password=root
```
```
mysql> select * from t3 limit 1\G
*************************** 1. row ***************************
    id: 1
tcol01: 10
tcol02: 173
tcol03: 1700
tcol04: 13498
tcol05: 33239373
tcol06: 44846.4
tcol07: 5300.23
tcol08: 11360967.75
tcol09: 2017-09-04
tcol10: 2016-11-02 23:11:25
tcol11: 2017-03-03 08:11:40
tcol12: 03:19:39
tcol13: 2017
tcol14: repellat maxime nostrum provident maiores ut quo voluptas.
tcol15: Th
tcol16: Walter
tcol17: quo repellat accusamus quidem odi
tcol18: esse laboriosam nobis libero aut dolores e
tcol19: Carlos Willia
tcol20: et nostrum iusto ipsa sunt recusa
tcol21: a accusantium laboriosam voluptas facilis.
tcol22: laudantium quo unde molestiae consequatur magnam.
tcol23: Pet
tcol24: Richard
tcol25: c
tcol26: green
tcol27: 47.430
tcol28: 6.12
1 row in set (0.00 sec)
```

## To do
- [ ] Add suport for all data types.
- [ ] Add supporrt for foreign keys.
- [ ] Support config files to override default values/ranges.
- [ ] Support custom functions via LUA plugins.
