# Hive练习二

## 数据
日志分析数据 logs

## 建表
* user\_dimension  
create table if not exists user\_dimension (  
 uid STRING,  
 name STRING,  
 gender STRING,  
 birth DATE,  
 province STRING  
)ROW FORMAT DELIMITED  
 FIELDS TERMINATED BY ','
 
* 查看表信息  
describe user\_dimension;    
show create table user\_dimension;

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

* 查看所有表  
show tables;

## 载入数据  
### user\_dimension  
* 数据  

```
> cat /home/bigdata/software/apache-hive-2.1.1-bin/hivedata/user.data     
00000000,Lane,M,1978-01-10,GanSu
00000001,Oconnell,M,1995-11-03,NeiMengGu
00000002,Lee,M,1995-04-24,XiZang
00000003,Ibarra,M,2007-01-29,HeiLongJiang
00000004,Paul,M,1998-03-28,FuJian
00000005,Allen,F,1977-11-14,TianJin
00000006,Martin,F,1986-08-09,TaiWan
00000007,Martin,M,1983-04-13,ShanXi3
00000008,Andrews,M,1985-06-29,ShangHai
00000009,Adams,M,1989-04-11,ShanXi3
00000010,Martinez,F,1973-01-25,BeiJing
```
* 载入本地数据  
load data local inpath '/home/bigdata/software/apache-hive-2.1.1-bin/hivedata/user.data' overwrite into table user\_dimension;  

* 验证  
select * from user\_dimension;

* 载入HDFS上的数据  
load data inpath '/user/bigdata/practice\_1/user.data' overwrite into table user\_dimension;

* 查看hive在hdfs上的存储目录  
hadoop fs -ls /warehouse/  
hadoop fs -ls /warehouse/user\_dimension


### brand\_dimension  
* 数据

```
> cat /home/bigdata/software/apache-hive-2.1.1-bin/hivedata/brand.data
00000000,computer,ACER
00000001,computer,DELL
00000002,telephone,OPPO
00000003,telephone,SAMSUNG
00000004,food,WULIANGYE
00000005,sports,PUMA
00000006,food,YILI
```
* 载入本地数据  
load data local inpath '/home/bigdata/software/apache-hive-2.1.1-bin/hivedata/brand.data' overwrite into table brand\_dimension;

* 验证   
select * from brand\_dimension;

### record
* 数据   

```
> cat /home/bigdata/software/apache-hive-2.1.1-bin/hivedata/record.data
0000000000,00000001,00000002,625,HeiLongJiang,HuNan,JUHUASUAN,346684703826952,SHENTONG,2017-04-01
0000000001,00000001,00000001,252,GuangDong,GuangDong,JUHUASUAN,869941469046622,ZHONGTONG,2017-04-01
0000000002,00000004,00000003,697,JiangSu,NeiMengGu,TIANMAO,4921065098017,YUNDA,2017-04-01
0000000003,00000004,00000003,347,ShangHai,NingXia,TIANMAOCHAOSHI,502085477177,YUNDA,2017-04-01
0000000004,00000002,00000004,429,XiangGang,JiangSu,JUHUASUAN,3528831911171002,ZHONGTONG,2017-04-01
0000000005,00000008,00000005,120,HeBei,Aomen,JUHUASUAN,4143527271101182,ZHONGTONG,2017-04-01
```
* 载入本地数据   
load data local inpath '/home/bigdata/software/apache-hive-2.1.1-bin/hivedata/record.data' overwrite into table record;

* 验证
select * from record;

## 查询
* 在2017年4月1日发生交易的数量  
select count(*) from record where trancation\_date = '2017-04-01';

* 不同年龄消费的情况  
select cast(DATEDIFF(CURRENT\_DATE, birth)/365 as int) as age,
sum(price) as totalPrice
from record join user\_dimension on record.uid=user\_dimension.uid
group by cast(DATEDIFF(CURRENT\_DATE, birth)/365 as int)
order by totalPrice desc;

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
select cast(DATEDIFF(CURRENT\_DATE, user\_dimension.birth)/365 as int) as age, category, sum(price) as totalPrice
from record join user\_dimension on record.uid=user\_dimension.uid
join brand\_dimension on record.bid=brand\_dimension.bid
group by cast(DATEDIFF(CURRENT\_DATE, user\_dimension.birth)/365 as int), category
order by age, category, totalPrice;

* 练习  
(1). 2017年4月1日各个商品品牌的交易笔数，按照销售交易从多到少排序  
(2). 不同性别消费的商品类别情况(不同性别消费不同商品类别的总价)


