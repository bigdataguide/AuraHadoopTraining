# Hive练习六

## 数据
stocks

hadoop fs -put stocks /user/bigdata/

## 创建外部表
CREATE EXTERNAL TABLE IF NOT EXISTS stocks\_external (
          ymd             DATE,
          price\_open      FLOAT, 
          price\_high      FLOAT,
          price\_low       FLOAT,
          price\_close     FLOAT,
          volume            INT,
          price\_adj\_close FLOAT
          )
          PARTITIONED BY (exchanger STRING, symbol STRING)
          ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
          LOCATION '/user/bigdata/stocks';


select * from stocks_external;

## 载入数据
alter table stocks\_external add partition(exchanger="NASDAQ", symbol="AAPL") location '/user/bigdata/stocks/NASDAQ/AAPL/'

show partitions stocks\_external;

select * from stocks\_external limit 10;


alter table stocks\_external add partition(exchanger="NASDAQ", symbol="INTC") location '/user/bigdata/stocks/NASDAQ/INTC/';

alter table stocks\_external add partition(exchanger="NYSE", symbol="IBM") location '/user/bigdata/stocks/NYSE/IBM/';

alter table stocks\_external add partition(exchanger="NYSE", symbol="GE") location '/user/bigdata/stocks/NYSE/GE/';

show partitions stocks\_external;

## 查询
SELECT * FROM stocks\_external WHERE exchanger = 'NASDAQ' AND symbol = 'AAPL' LIMIT 10;

SELECT ymd, price\_close FROM stocks\_external WHERE exchanger = 'NASDAQ' AND symbol = 'AAPL' LIMIT 10;

select exchanger, symbol,count(*) from stocks\_external group by exchanger, symbol;

select exchanger, symbol, max(price\_high) from stocks\_external group by exchanger, symbol;

## 删除表
* 删除内部表stocks  
drop table stocks;

* 查看HDFS上文件目录
hadoop fs -ls /warehouse/

* 删除外部表stocks\_external  
drop table stocks\_external；

* 查看HDFS上文件目录  
hadoop fs -ls /user/bigdata  
hadoop fs -ls /user/stocks


