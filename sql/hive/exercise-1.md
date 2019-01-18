# Hive练习一

SHOW DATABASES;

CREATE DATABASE IF NOT EXISTS db1 COMMENT 'Our database db1';

SHOW DATABASES;

DESCRIBE DATABASE db1;

CREATE TABLE db1.table1 (word STRING, count INT);

SHOW TABLES in db1;

DESCRIBE db1.table1;

USE db1;

SHOW TABLES;

SELECT * FROM db1.table1;

DROP TABLE table1;

DROP DATABASE db1;

USE default;


