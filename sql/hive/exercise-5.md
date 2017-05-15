# Hive练习五

## 数据
logs

## 建表
create table if not exists record\_partition (  
      rid STRING,  
      uid STRING,  
      bid STRING,  
      price INT,  
      source\_province STRING,  
      target\_province STRING,  
      site STRING,  
      express\_number STRING,  
      express\_company STRING  
     )  
     partitioned by(trancation\_date DATE)  

show create table record\_partition;


## 载入数据
select * from record\_partition limit 10;

set hive.exec.dynamic.partition.mode=nonstrict;

insert into table record\_partition partition(trancation\_date) select * from record;

select * from record\_partition limit 10;


