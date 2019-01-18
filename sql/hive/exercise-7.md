# Hive练习七

## 数据
logs

## 建表
create table if not exists record\_orc (  
      rid STRING,  
      uid STRING,  
      bid STRING,  
      price INT,  
      source\_province STRING,  
      target\_province STRING,  
      site STRING,  
      express\_number STRING,  
      express\_company STRING,  
      trancation\_date DATE  
     )  
     stored as orc;  

show create table record\_orc;


## 载入数据
select * from record\_orc limit 10;

insert into table record\_orc select * from record;

select * from record\_orc limit 10;


