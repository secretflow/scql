
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
  `description` varchar(64) DEFAULT NULL COMMENT '''description''',
  `creator` varchar(64) DEFAULT NULL COMMENT '''creator of the project''',
  `archived` tinyint(1) DEFAULT NULL COMMENT '''if archived is true, whole project can''t be modified''',
  `created_at` timestamp DEFAULT NULL,
  `updated_at` timestamp DEFAULT NULL,
  `project_conf` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `members`
--

DROP TABLE IF EXISTS `members`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `members` (
  `project_id` varchar(64) NOT NULL,
  `member` varchar(64) NOT NULL COMMENT '''member in the project''',
  PRIMARY KEY (`project_id`, `member`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `owner` varchar(64) DEFAULT NULL COMMENT '''ref table''',
  `is_view` tinyint(1) DEFAULT NULL COMMENT '''this table is a view''',
  `select_string` longtext DEFAULT NULL COMMENT '''the internal select query in string format, the field is valid only when IsView is true''',
  `created_at` timestamp DEFAULT NULL,
  `updated_at` timestamp DEFAULT NULL,
  PRIMARY KEY (`project_id`,`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
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
  `created_at` timestamp DEFAULT NULL,
  `updated_at` timestamp DEFAULT NULL,
  PRIMARY KEY (`project_id`,`table_name`,`column_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;


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
  `created_at` timestamp DEFAULT NULL,
  `updated_at` timestamp DEFAULT NULL,
  PRIMARY KEY (`project_id`,`table_name`,`column_name`,`dest_party`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `description` varchar(64) DEFAULT NULL COMMENT '''description''',
  `creator` varchar(64) DEFAULT NULL COMMENT '''creator of the project''',
  `proj_created_at` timestamp DEFAULT NULL COMMENT '''the create time of the project''',
  `member` varchar(64) NOT NULL COMMENT '''members, flattened string, like: alice',
  `inviter` varchar(256) DEFAULT NULL COMMENT '''inviter''',
  `invitee` varchar(256) DEFAULT NULL COMMENT '''invitee''',
  `status` tinyint(4) DEFAULT '0' COMMENT '''0: default, not decided to accept invitation or not; 1: accepted; 2: rejected; 3: invalid''',
  `invite_time` timestamp DEFAULT NULL,
  `created_at` timestamp DEFAULT NULL,
  `updated_at` timestamp DEFAULT NULL,
  `project_conf` text,
  PRIMARY KEY (`id`),
  INDEX `idx_project_id_inviter_invitee_identifier` (`project_id`,`inviter`, `invitee`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `session_infos`
--

DROP TABLE IF EXISTS `session_infos`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `session_infos` (
  `session_id` varchar(64) NOT NULL COMMENT '''unique session id''',
  `status` tinyint DEFAULT '0' COMMENT '''session status''',
  `table_checksum` varbinary(256) DEFAULT NULL COMMENT '''table checksum for self party''',
  `ccl_checksum` varbinary(256) DEFAULT NULL COMMENT '''ccl checksum for self party''',
  `engine_url` varchar(256) DEFAULT NULL COMMENT '''url for engine to communicate with peer engine''',
  `engine_url_for_self` varchar(256) DEFAULT NULL COMMENT '''engine url used for self broker''',
  `job_info` longblob COMMENT '''serialized job info to specify task in engine''',
  `work_parties` longtext NOT NULL COMMENT '''parties involved, flattened string, like: alice',
  `output_names` longtext COMMENT '''output column names, flattened string, like: col1,col2''',
  `warning` longblob COMMENT '''warning infos, serialized from pb.Warning''',
  `created_at` timestamp DEFAULT NULL,
  `updated_at` timestamp DEFAULT NULL,
  `expired_at` timestamp DEFAULT NULL,
  PRIMARY KEY (`session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `session_results`
--

DROP TABLE IF EXISTS `session_results`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `session_results` (
  `session_id` varchar(64) NOT NULL COMMENT '''unique session id''',
  `result` longblob COMMENT '''query result, serialized from protobuf message''',
  `created_at` timestamp DEFAULT NULL,
  `updated_at` timestamp DEFAULT NULL,
  `expired_at` timestamp DEFAULT NULL,
  PRIMARY KEY (`session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `locks`
--

DROP TABLE IF EXISTS `locks`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `locks` (
  `id` tinyint NOT NULL AUTO_INCREMENT COMMENT '''lock id''',
  `owner` varchar(64) DEFAULT NULL COMMENT '''lock owner''',
  `updated_at` timestamp DEFAULT NULL,
  `expired_at` timestamp DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;
