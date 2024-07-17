CREATE DATABASE IF NOT EXISTS `bob`;
USE `bob`;

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

DROP TABLE IF EXISTS `part`;
CREATE TABLE part (
  p_partkey integer NOT NULL,
  p_type varchar(25) NOT NULL
);

DROP TABLE IF EXISTS `orders`;
CREATE TABLE orders (
  o_orderkey integer NOT NULL,
  o_orderpriority char(15) NOT NULL
);


LOAD DATA INFILE '/var/lib/mysql-files/isv_part.csv' INTO TABLE part COLUMNS TERMINATED BY ',' LINES TERMINATED BY '\r\n';

LOAD DATA INFILE '/var/lib/mysql-files/isv_orders.csv' INTO TABLE orders COLUMNS TERMINATED BY ',' LINES TERMINATED BY '\r\n';

LOAD DATA INFILE '/var/lib/mysql-files/isv_lineitem.csv' INTO TABLE lineitem COLUMNS TERMINATED BY ',' LINES TERMINATED BY '\r\n';