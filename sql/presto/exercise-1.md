# Presto练习一

## 查询
* 在2017年4月1日发生交易的数量  
select count(*) from record where trancation\_date = date '2017-04-01';

* 不同年龄消费的情况  
select cast((year(CURRENT\_DATE)-year(birth)) as integer) as age,sum(price) as totalPrice   
from record join user\_dimension on record.uid=user\_dimension.uid  
group by cast((year(CURRENT\_DATE)-year(birth)) as integer)  
order by totalPrice desc  

* 不同品牌被消费的情况  
select brand,sum(price) as totalPrice  
from record join brand\_dimension on record.bid=brand\_dimension.bid  
group by brand\_dimension.brand  
order by totalPrice desc;  

* 不同省份消费的情况  
select province, sum(price) as totalPrice  
from record join user\_dimension on record.uid=user\_dimension.uid  
group by province  
order by totalPrice desc;  

* 不同年龄消费的商品类别情况(不同年龄消费不同商品类别的总价)  
select cast((year(CURRENT\_DATE)-year(birth)) as integer) as age, category, sum(price) as totalPrice  
from record join user\_dimension on record.uid=user\_dimension.uid  
join brand\_dimension on record.bid=brand\_dimension.bid  
group by cast((year(CURRENT\_DATE)-year(birth)) as integer), category  
order by age, category, totalPrice;  


