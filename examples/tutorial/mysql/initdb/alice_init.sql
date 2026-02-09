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
INSERT INTO `user_credit` VALUES ("id0001", 6, 100000, 20);
INSERT INTO `user_credit` VALUES ("id0002", 5, 90000, 19);
INSERT INTO `user_credit` VALUES ("id0003", 6, 89700, 32);
INSERT INTO `user_credit` VALUES ("id0005", 6, 607000, 30);
INSERT INTO `user_credit` VALUES ("id0006", 5, 30070, 25);
INSERT INTO `user_credit` VALUES ("id0007", 6, 12070, 28);
INSERT INTO `user_credit` VALUES ("id0008", 6, 200800, 50);
INSERT INTO `user_credit` VALUES ("id0009", 6, 607000, 30);
INSERT INTO `user_credit` VALUES ("id0010", 5, 30070, 25);
INSERT INTO `user_credit` VALUES ("id0011", 5, 12070, 28);
INSERT INTO `user_credit` VALUES ("id0012", 6, 200800, 50);
INSERT INTO `user_credit` VALUES ("id0013", 5, 30070, 25);
INSERT INTO `user_credit` VALUES ("id0014", 5, 12070, 28);
INSERT INTO `user_credit` VALUES ("id0015", 6, 200800, 18);
INSERT INTO `user_credit` VALUES ("id0016", 5, 30070, 26);
INSERT INTO `user_credit` VALUES ("id0017", 5, 12070, 27);
INSERT INTO `user_credit` VALUES ("id0018", 6, 200800, 16);
INSERT INTO `user_credit` VALUES ("id0019", 6, 30070, 25);
INSERT INTO `user_credit` VALUES ("id0020", 5, 12070, 28);
UNLOCK TABLES;



