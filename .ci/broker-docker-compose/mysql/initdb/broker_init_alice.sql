
DROP DATABASE IF EXISTS `brokera`;
CREATE DATABASE `brokera`;
USE `brokera`;

--
-- Table structure for table `projects`
--

DROP TABLE IF EXISTS `projects`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `projects` (
  `id` varchar(64) NOT NULL COMMENT '''unique id''',
  `name` varchar(64) NOT NULL COMMENT '''project name''',
  `desc` varchar(64) DEFAULT NULL COMMENT '''description''',
  `creator` varchar(64) DEFAULT NULL COMMENT '''creator of the project''',
  `member` varchar(64) DEFAULT NULL COMMENT '''members, flattened string, like: alice',
  `archived` tinyint(1) DEFAULT NULL COMMENT '''if archived is true, whole project can''t be modified''',
  `spu_conf` longtext COMMENT '''description''',
  `created_at` datetime(3) DEFAULT NULL,
  `updated_at` datetime(3) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `projects`
--

LOCK TABLES `projects` WRITE;
/*!40000 ALTER TABLE `projects` DISABLE KEYS */;
/*!40000 ALTER TABLE `projects` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tables`
--

DROP TABLE IF EXISTS `tables`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tables` (
  `project_id` varchar(64) NOT NULL,
  `table_name` varchar(64) NOT NULL,
  `ref_table` varchar(128) DEFAULT NULL COMMENT '''ref table''',
  `db_type` varchar(64) DEFAULT NULL COMMENT '''database type like MYSQL''',
  `owner` longtext COMMENT '''ref table''',
  `created_at` datetime(3) DEFAULT NULL,
  `updated_at` datetime(3) DEFAULT NULL,
  PRIMARY KEY (`project_id`,`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `columns`
--

DROP TABLE IF EXISTS `columns`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `columns` (
  `project_id` varchar(64) NOT NULL,
  `table_name` varchar(64) NOT NULL,
  `column_name` varchar(64) NOT NULL,
  `data_type` varchar(64) DEFAULT NULL COMMENT '''data type like float''',
  `created_at` datetime(3) DEFAULT NULL,
  `updated_at` datetime(3) DEFAULT NULL,
  PRIMARY KEY (`project_id`,`table_name`,`column_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `columns`
--

LOCK TABLES `columns` WRITE;
/*!40000 ALTER TABLE `columns` DISABLE KEYS */;
/*!40000 ALTER TABLE `columns` ENABLE KEYS */;
UNLOCK TABLES;


--
-- Table structure for table `column_privs`
--

DROP TABLE IF EXISTS `column_privs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `column_privs` (
  `project_id` varchar(64) NOT NULL,
  `table_name` varchar(64) NOT NULL,
  `column_name` varchar(64) NOT NULL,
  `dest_party` varchar(64) NOT NULL,
  `priv` varchar(256) DEFAULT NULL COMMENT '''priv of column''',
  `created_at` datetime(3) DEFAULT NULL,
  `updated_at` datetime(3) DEFAULT NULL,
  PRIMARY KEY (`project_id`,`table_name`,`column_name`,`dest_party`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `column_privs`
--

LOCK TABLES `column_privs` WRITE;
/*!40000 ALTER TABLE `column_privs` DISABLE KEYS */;
/*!40000 ALTER TABLE `column_privs` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `invitations`
--

DROP TABLE IF EXISTS `invitations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `invitations` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '''auto generated increment id''',
  `project_id` varchar(64) NOT NULL COMMENT '''project id''',
  `name` varchar(64) DEFAULT NULL COMMENT '''name''',
  `desc` varchar(64) DEFAULT NULL COMMENT '''description''',
  `creator` varchar(64) DEFAULT NULL COMMENT '''creator of the project''',
  `member` varchar(64) NOT NULL COMMENT '''members, flattened string, like: alice',
  `spu_conf` longtext COMMENT '''description''',
  `inviter` varchar(256) DEFAULT NULL COMMENT '''inviter''',
  `invitee` varchar(256) DEFAULT NULL COMMENT '''invitee''',
  `accepted` tinyint(4) DEFAULT '0' COMMENT '''accepted''',
  `invite_time` datetime(3) DEFAULT NULL,
  `created_at` datetime(3) DEFAULT NULL,
  `updated_at` datetime(3) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `invitations`
--

LOCK TABLES `invitations` WRITE;
/*!40000 ALTER TABLE `invitations` DISABLE KEYS */;
/*!40000 ALTER TABLE `invitations` ENABLE KEYS */;
UNLOCK TABLES;