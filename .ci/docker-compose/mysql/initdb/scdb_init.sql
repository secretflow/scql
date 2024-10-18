-- MySQL dump 10.13  Distrib 5.7.11, for Linux (x86_64)
--
-- Host: localhost    Database: scdb
-- ------------------------------------------------------
-- Server version	5.7.11

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Current Database: `scdb`
--

CREATE DATABASE /*!32312 IF NOT EXISTS*/ `scdb` /*!40100 DEFAULT CHARACTER SET latin1 */;

USE `scdb`;

--
-- Table structure for table `column_privs`
--

DROP TABLE IF EXISTS `column_privs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `column_privs` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '''unique id''',
  `host` varchar(128) DEFAULT NULL COMMENT '''host name''',
  `db` varchar(64) DEFAULT NULL COMMENT '''database name''',
  `user` varchar(64) DEFAULT NULL COMMENT '''user name''',
  `table_name` varchar(64) DEFAULT NULL COMMENT '''table name''',
  `column_name` varchar(64) DEFAULT NULL COMMENT '''column name''',
  `visibility_priv` bigint(20) unsigned DEFAULT NULL COMMENT '''visibility privilege''',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_host_user_db_table_col` (`host`,`db`,`user`,`table_name`,`column_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `columns`
--

DROP TABLE IF EXISTS `columns`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `columns` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '''unique id''',
  `db` varchar(64) DEFAULT NULL COMMENT '''database name''',
  `table_name` varchar(64) DEFAULT NULL COMMENT '''table name''',
  `column_name` varchar(64) DEFAULT NULL COMMENT '''column name''',
  `ordinal_position` bigint(20) unsigned DEFAULT NULL COMMENT '''ordinal position''',
  `type` varchar(32) DEFAULT NULL COMMENT '''column type''',
  `description` varchar(1024) DEFAULT NULL COMMENT '''column description''',
  `created_at` timestamp DEFAULT NULL,
  `updated_at` timestamp DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_db_table_col` (`db`,`table_name`,`column_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `database_privs`
--

DROP TABLE IF EXISTS `database_privs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `database_privs` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '''unique id''',
  `host` varchar(128) DEFAULT NULL COMMENT '''host name''',
  `db` varchar(64) DEFAULT NULL COMMENT '''database name''',
  `user` varchar(64) DEFAULT NULL COMMENT '''user name''',
  `create_priv` tinyint(1) DEFAULT NULL COMMENT '''create privilege''',
  `drop_priv` tinyint(1) DEFAULT NULL COMMENT '''drop privilege''',
  `grant_priv` tinyint(1) DEFAULT NULL COMMENT '''grant privilege''',
  `describe_priv` tinyint(1) DEFAULT NULL COMMENT '''describe privilege''',
  `show_priv` tinyint(1) DEFAULT NULL COMMENT '''show privilege''',
  `create_view_priv` tinyint(1) DEFAULT NULL COMMENT '''create view privilege''',
  `created_at` timestamp DEFAULT NULL,
  `updated_at` timestamp DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_host_user_db` (`host`,`db`,`user`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `databases`
--

DROP TABLE IF EXISTS `databases`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `databases` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '''unique id''',
  `db` varchar(64) DEFAULT NULL COMMENT '''database name''',
  `created_at` timestamp DEFAULT NULL,
  `updated_at` timestamp DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_db` (`db`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `table_privs`
--

DROP TABLE IF EXISTS `table_privs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `table_privs` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '''unique id''',
  `host` varchar(128) DEFAULT NULL COMMENT '''host name''',
  `db` varchar(64) DEFAULT NULL COMMENT '''database name''',
  `user` varchar(64) DEFAULT NULL COMMENT '''user name''',
  `table_name` varchar(64) DEFAULT NULL COMMENT '''table name''',
  `create_priv` tinyint(1) DEFAULT NULL COMMENT '''create privilege''',
  `drop_priv` tinyint(1) DEFAULT NULL COMMENT '''drop privilege''',
  `grant_priv` tinyint(1) DEFAULT NULL COMMENT '''grant privilege''',
  `visibility_priv` bigint(20) unsigned DEFAULT NULL COMMENT '''visibility privilege''',
  `created_at` timestamp DEFAULT NULL,
  `updated_at` timestamp DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_host_user_db_table` (`host`,`db`,`user`,`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tables`
--

DROP TABLE IF EXISTS `tables`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tables` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '''unique id''',
  `db` varchar(64) DEFAULT NULL COMMENT '''database name''',
  `table_name` varchar(64) DEFAULT NULL COMMENT '''table name''',
  `table_schema` longtext COMMENT '''table schema''',
  `owner` varchar(64) DEFAULT NULL COMMENT '''owner user name''',
  `host` varchar(64) DEFAULT NULL COMMENT '''owner user host''',
  `ref_db` varchar(64) DEFAULT NULL COMMENT '''reference database name''',
  `ref_table` varchar(64) DEFAULT NULL COMMENT '''reference table name''',
  `is_view` tinyint(1) DEFAULT NULL COMMENT '''this table is a view''',
  `select_string` longtext COMMENT '''the internal select query in string format''',
  `db_type` bigint(20) DEFAULT 0 COMMENT '''database type where table data stored''',
  `created_at` timestamp DEFAULT NULL,
  `updated_at` timestamp DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_db_table` (`db`,`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `users` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '''unique id''',
  `host` varchar(128) DEFAULT NULL COMMENT '''host name''',
  `user` varchar(64) DEFAULT NULL COMMENT '''user name''',
  `password` varchar(256) DEFAULT NULL COMMENT '''password''',
  `party_code` varchar(64) DEFAULT NULL COMMENT '''The party code the user belongs to''',
  `create_priv` tinyint(1) DEFAULT NULL COMMENT '''create privilege''',
  `create_user_priv` tinyint(1) DEFAULT NULL COMMENT '''create user privilege''',
  `drop_priv` tinyint(1) DEFAULT NULL COMMENT '''drop privilege''',
  `grant_priv` tinyint(1) DEFAULT NULL COMMENT '''grant privilege''',
  `describe_priv` tinyint(1) DEFAULT NULL COMMENT '''describe privilege''',
  `show_priv` tinyint(1) DEFAULT NULL COMMENT '''show privilege''',
  `create_view_priv` tinyint(1) DEFAULT NULL COMMENT '''create view privilege''',
  `eng_auth_method` smallint DEFAULT NULL,
  `eng_token` varchar(256) DEFAULT NULL,
  `eng_pubkey` varchar(256) DEFAULT NULL,
  `eng_endpoints` varchar(256) DEFAULT NULL,
  `created_at` timestamp DEFAULT NULL,
  `updated_at` timestamp DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_host_user` (`host`,`user`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2022-12-06 16:10:37
--
-- Dumping data for table `users`
--

LOCK TABLES `users` WRITE;
/*!40000 ALTER TABLE `users` DISABLE KEYS */;
INSERT INTO `users` VALUES (1,'%','root','*EF722C9187A4F1ABBD33861281A782F22D2DD9882045F4EB2E65294D3D825298','',1,1,1,1,1,1,1,0,'','','','2022-12-05 19:22:43.657','2022-12-05 19:22:43.657');
/*!40000 ALTER TABLE `users` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2022-12-06 16:14:57
