CREATE DATABASE IF NOT EXISTS `alice`;
USE `alice`;

DROP TABLE IF EXISTS lineitem;
CREATE TABLE lineitem (
  l_orderkey integer NOT NULL,
  l_partkey integer NOT NULL,
  l_quantity decimal(15, 2) NOT NULL,
  l_extendedprice decimal(15, 2) NOT NULL,
  l_discount decimal(15, 2) NOT NULL,
  l_tax decimal(15, 2) NOT NULL,
  l_returnflag char(1) NOT NULL,
  l_linestatus char(1) NOT NULL,
  l_shipdate date NOT NULL,
  l_commitdate date NOT NULL,
  l_receiptdate date NOT NULL,
  l_shipmode char(10) NOT NULL,
  l_shipmode1 char(10) NOT NULL
);

LOAD DATA INFILE '/var/lib/mysql-files/ant_lineitem.csv' INTO TABLE lineitem COLUMNS TERMINATED BY ',' LINES TERMINATED BY '\r\n';