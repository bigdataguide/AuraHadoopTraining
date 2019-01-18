# Hive练习九

## 数据
logs

## 表信息
* user\_dimension  
create table if not exists user\_dimension (  
 uid STRING,  
 name STRING,  
 gender STRING,  
 birth DATE,  
 province STRING  
)ROW FORMAT DELIMITED  
 FIELDS TERMINATED BY ','  


* brand\_dimension  
create table if not exists brand\_dimension (  
 bid STRING,  
 category STRING,  
 brand STRING  
 )ROW FORMAT DELIMITED  
  FIELDS TERMINATED BY ','  

* record  
create table if not exists record (  
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
 ROW FORMAT DELIMITED  
 FIELDS TERMINATED BY ','  

## 查询分析  
* 查看每笔交易的金额  
explain select price from record;


* 在2017年4月1日发生交易的数量  
explain select count(*) from record where trancation\_date = '2017-04-01';
  
* 不同年龄消费的情况  
explain select cast(DATEDIFF(CURRENT\_DATE, birth)/365 as int) as age,  
sum(price) as totalPrice  
from record join user\_dimension on record.uid=user\_dimension.uid   
group by cast(DATEDIFF(CURRENT\_DATE, birth)/365 as int)   
order by totalPrice desc;  

* 不同品牌被消费的情况  
explain select brand,sum(price) as totalPrice  
from record join brand\_dimension on record.bid=brand\_dimension.bid  
group by brand\_dimension.brand  
order by totalPrice desc;  

* 不同省份消费的情况  
explain select province, sum(price) as totalPrice  
from record join user\_dimension on record.uid=user\_dimension.uid  
group by province  
order by totalPrice desc;  

* 不同年龄消费的商品类别情况(不同年龄消费不同商品类别的总价)  
explain select cast(DATEDIFF(CURRENT\_DATE, user\_dimension.birth)/365 as int) as age, category,   sum(price) as totalPrice  
from record join user\_dimension on record.uid=user\_dimension.uid  
join brand\_dimension on record.bid=brand\_dimension.bid  
group by cast(DATEDIFF(CURRENT_\DATE, user\_dimension.birth)/365 as int), category  
order by age, category, totalPrice;  


