CREATE DATABASE IF NOT EXISTS `alice`;
USE `alice`;

DROP TABLE IF EXISTS `user_credit`;
CREATE TABLE `user_credit` (
	`ID` varchar(64) NOT NULL,
	`credit_rank` int NOT NULL,
	`income` int NOT NULL,
	`age` int NOT NULL,
	PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

LOCK TABLES `user_credit` WRITE;

LOAD DATA INFILE "/var/lib/mysql-files/test_alice_10000000.csv" INTO TABLE user_credit COLUMNS TERMINATED BY ',';

UNLOCK TABLES;
