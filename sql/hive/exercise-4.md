# Hive练习四

## 数据
stocks
 
## 建表
CREATE TABLE IF NOT EXISTS stocks (  
     ymd             DATE,  
     price\_open      FLOAT,   
     price\_high      FLOAT,  
     price\_low       FLOAT,  
     price\_close     FLOAT,  
     volume            INT,  
     price\_adj\_close FLOAT  
     )  
     PARTITIONED BY (exchanger STRING, symbol STRING)  
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';  

## 载入数据
load data local inpath '/home/bigdata/software/apache-hive-2.1.1-bin/hivedata/stocks/NASDAQ/AAPL/stocks.csv' overwrite into table stocks partition(exchanger="NASDAQ", symbol="AAPL");

show partitions stocks;

load data local inpath '/home/bigdata/software/apache-hive-2.1.1-bin/hivedata/stocks/NASDAQ/INTC/stocks.csv' overwrite into table stocks partition(exchanger="NASDAQ", symbol="INTC");

load data local inpath '/home/bigdata/software/apache-hive-2.1.1-bin/hivedata/stocks/NYSE/GE/stocks.csv' overwrite into table stocks partition(exchanger="NYSE", symbol="GE");

load data local inpath '/home/bigdata/software/apache-hive-2.1.1-bin/hivedata/stocks/NYSE/IBM/stocks.csv' overwrite into table stocks partition(exchanger="NYSE", symbol="IBM");

show partitions stocks;

## 查询
SELECT * FROM stocks WHERE exchanger = 'NASDAQ' AND symbol = 'AAPL' LIMIT 10;

SELECT ymd, price\_close FROM stocks WHERE exchanger = 'NASDAQ' AND symbol = 'AAPL' LIMIT 10;

select exchanger, symbol,count(*) from stocks group by exchanger, symbol;

select exchanger, symbol, max(price\_high) from stocks group by exchanger, symbol;

## 查看HDFS文件目录
hadoop fs -ls /warehouse/

hadoop fs -ls /warehouse/stocks/

hadoop fs -ls /warehouse/stocks/exchanger=NASDAQ

hadoop fs -ls /warehouse/stocks/exchanger=NASDAQ/symbol=AAPL

hadoop fs -ls /warehouse/stocks/exchanger=NYSE

hadoop fs -ls /warehouse/stocks/exchanger=NYSE/symbol=IBM


