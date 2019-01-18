# Hive练习三

## 数据
employees

## 建表 
create database practice2;

show databases;

use practice2;

CREATE TABLE IF NOT EXISTS employees (  
   name         STRING,  
   salary       FLOAT,  
   subordinates ARRAY<STRING>,  
   deductions   MAP<STRING, FLOAT>,  
   address      STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>  
 )  
 ROW FORMAT DELIMITED  
 FIELDS TERMINATED BY '\001'   
 COLLECTION ITEMS TERMINATED BY '\002'   
 MAP KEYS TERMINATED BY '\003'  
 LINES TERMINATED BY '\n'  
 STORED AS TEXTFILE;  

## 载入数据
load data local inpath '/home/bigdata/software/apache-hive-2.1.1-bin/hivedata/employees.txt' overwrite into table employees ;  

select * from employees;

## 查询
* 联邦税超过0.2的雇员  
 
SELECT name, deductions['Federal Taxes'] 
FROM employees 
WHERE deductions['Federal Taxes'] > 0.2;

SELECT name, deductions['Federal Taxes'] 
FROM employees 
WHERE deductions['Federal Taxes'] >cast( 0.2 as float);

SELECT name, deductions['Federal Taxes'] 
FROM employees 
WHERE deductions['Federal Taxes'] >0.2+1e-5;

* 查看经理的名字，他的第二个直系下属是Todd Jones

SELECT name FROM employees WHERE subordinates[1] = 'Todd Jones';

* 谁是经理  

SELECT name FROM employees WHERE size(subordinates) > 0;

* 谁住在邮编是60500的地区  

SELECT name FROM employees WHERE address.zip = 60500;

* 谁住在以字幕C开头的城市  

SELECT name, address FROM employees WHERE address.city LIKE 'C%';

* 谁住在Ontario或Chicago街道  

SELECT name, address FROM employees
WHERE address.street RLIKE '^.*(Ontario|Chicago).*$';

## 练习
（1）谁不是经理  
（2）谁住在邮编比60500大的地区  
（3）联邦税超过0.15的雇员  
（4）谁住在Drive或Park街道

