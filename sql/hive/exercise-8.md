# Hive练习八

## 数据
employees

## employees表的元信息
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

## 查询
select name,subordinate from employees LATERAL VIEW explode(subordinates) subordinates\_table AS subordinate;


