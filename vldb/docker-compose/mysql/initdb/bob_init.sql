CREATE DATABASE IF NOT EXISTS `bob`;
USE `bob`;

DROP TABLE IF EXISTS `user_stats`;
CREATE TABLE `user_stats` (
	`ID` varchar(64) not NULL,
	`order_amount` float not null,
	`is_active` tinyint(1) not null,
	PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

LOCK TABLES `user_stats` WRITE;

LOAD DATA INFILE "/var/lib/mysql-files/test_bob_10000000.csv" INTO TABLE user_stats COLUMNS TERMINATED BY ',';

UNLOCK TABLES;
