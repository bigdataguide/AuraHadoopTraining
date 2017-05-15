# Presto练习二

## 数据
logs/record\_partition

## 建表
（要求这个表是分区表，格式为ORC）  

* record  
create table if not exists record_\partition\_orc (  
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
 stored as orc;  

* 载入数据（练习）  
在Hive中，把云盘上提供的数据载入到record\_partition\_orc表中  
logs/record\_partition   
&nbsp;&nbsp;  2017-04-02/record.list  
&nbsp;&nbsp;  2017-04-03/record.list  

（提示：创建中间表，将数据导入到中间表，然后写入到目的表） 

## 比较Hive和Presto的查询
* 在2017年4月2日发生交易的数量  
select count(*) from record\_partition\_orc where trancation\_date = date '2017-04-02';  

* 不同年龄消费的情况  
Hive:  
select cast(DATEDIFF(CURRENT\_DATE, birth)/365 as int) as age,  
sum(price) as totalPrice  
from record\_partition\_orc join user\_dimension on record\_partition\_orc.uid=user\_dimension.uid  
group by cast(DATEDIFF(CURRENT\_DATE, birth)/365 as int)  
order by totalPrice desc;  
Presto:  
select cast((year(CURRENT\_DATE)-year(birth)) as integer) as age,sum(price) as totalPrice  
from record\_partition\_orc join user\_dimension on record\_partition\_orc.uid=user\_dimension.uid  
group by cast((year(CURRENT\_DATE)-year(birth)) as integer)   
order by totalPrice desc  

* 不同品牌被消费的情况  
Hive:  
select brand,sum(price) as totalPrice  
from record\_partition\_orc join brand\_dimension on record\_partition\_orc.bid=brand\_dimension.bid  
group by brand\_dimension.brand  
order by totalPrice desc;  
Presto:  
select brand,sum(price) as totalPrice  
from record\_partition\_orc join brand\_dimension on record\_partition\_orc.bid=brand\_dimension.bid  
group by brand\_dimension.brand  
order by totalPrice desc;  

* 不同省份消费的情况  
Hive:  
select province, sum(price) as totalPrice  
from record\_partition\_orc join user\_dimension on record\_partition\_orc.uid=user\_dimension.uid  
group by province  
order by totalPrice desc;  
Presto:  
select province, sum(price) as totalPrice  
from record\_partition\_orc join user\_dimension on record\_partition\_orc.uid=user\_dimension.uid  
group by province  
order by totalPrice desc;  

* 不同年龄消费的商品类别情况(不同年龄消费不同商品类别的总价)  
Hive:  
select cast(DATEDIFF(CURRENT\_DATE, user\_dimension.birth)/365 as int) as age, category, sum(price)   as totalPrice  
from record\_partition\_orc join user\_dimension on record\_partition\_orc.uid=user\_dimension.uid  
join brand\_dimension on record\_partition\_orc.bid=brand\_dimension.bid  
group by cast(DATEDIFF(CURRENT\_DATE, user\_dimension.birth)/365 as int), category  
order by age, category, totalPrice;  
Presto:  
select cast((year(CURRENT\_DATE)-year(birth)) as integer) as age, category, sum(price) as totalPrice  
from record\_partitio\n_orc join user\_dimension on record\_partition\_orc.uid=user\_dimension.uid  
join brand\_dimension on record\_partition\_orc.bid=brand\_dimension.bid  
group by cast((year(CURRENT\_DATE)-year(birth)) as integer), category  
order by age, category, totalPrice;  

## 练习  
(分别在Hive和Presto上运行查询，比较查询性能)  
(1). 2017年4月2日各个商品品牌的交易笔数，按照销售交易从多到少排序  
(2). 不同性别消费的商品类别情况(不同性别消费不同商品类别的总价)


