-- Create schema brokera and set search path
CREATE SCHEMA IF NOT EXISTS brokera;
SET search_path TO brokera;

-- Table structure for table `projects`
DROP TABLE IF EXISTS projects;
CREATE TABLE projects (
  id VARCHAR(64) NOT NULL,
  name VARCHAR(64) NOT NULL,
  "desc" VARCHAR(64),
  creator VARCHAR(64),
  archived BOOLEAN,
  created_at TIMESTAMP DEFAULT NULL,
  updated_at TIMESTAMP DEFAULT NULL,
  project_conf TEXT,
  PRIMARY KEY (id)
);

-- Table structure for table `members`
DROP TABLE IF EXISTS members;
CREATE TABLE members (
  project_id VARCHAR(64) NOT NULL,
  member VARCHAR(64) NOT NULL,
  PRIMARY KEY (project_id, member)
);

-- Table structure for table `tables`
DROP TABLE IF EXISTS tables;
CREATE TABLE tables (
  project_id VARCHAR(64) NOT NULL,
  table_name VARCHAR(64) NOT NULL,
  ref_table VARCHAR(128),
  db_type VARCHAR(64),
  "owner" VARCHAR(64),
  is_view BOOLEAN,
  select_string TEXT,
  created_at TIMESTAMP DEFAULT NULL,
  updated_at TIMESTAMP DEFAULT NULL,
  PRIMARY KEY (project_id, table_name)
);

-- Table structure for table `columns`
DROP TABLE IF EXISTS columns;
CREATE TABLE columns (
  project_id VARCHAR(64) NOT NULL,
  table_name VARCHAR(64) NOT NULL,
  column_name VARCHAR(64) NOT NULL,
  data_type VARCHAR(64),
  created_at TIMESTAMP DEFAULT NULL,
  updated_at TIMESTAMP DEFAULT NULL,
  PRIMARY KEY (project_id, table_name, column_name)
);

-- Table structure for table `column_privs`
DROP TABLE IF EXISTS column_privs;
CREATE TABLE column_privs (
  project_id VARCHAR(64) NOT NULL,
  table_name VARCHAR(64) NOT NULL,
  column_name VARCHAR(64) NOT NULL,
  dest_party VARCHAR(64) NOT NULL,
  priv VARCHAR(256),
  created_at TIMESTAMP DEFAULT NULL,
  updated_at TIMESTAMP DEFAULT NULL,
  PRIMARY KEY (project_id, table_name, column_name, dest_party)
);

-- Table structure for table `invitations`
DROP TABLE IF EXISTS invitations;
CREATE TABLE invitations (
  id BIGSERIAL NOT NULL,
  project_id VARCHAR(64) NOT NULL,
  name VARCHAR(64),
  "desc" VARCHAR(64),
  creator VARCHAR(64),
  proj_created_at TIMESTAMP DEFAULT NULL,
  member VARCHAR(64) NOT NULL,
  inviter VARCHAR(256),
  invitee VARCHAR(256),
  status SMALLINT DEFAULT 0,
  invite_time TIMESTAMP DEFAULT NULL,
  created_at TIMESTAMP DEFAULT NULL,
  updated_at TIMESTAMP DEFAULT NULL,
  project_conf TEXT,
  PRIMARY KEY (id)
);

-- Create index for `invitations`
CREATE INDEX idx_project_id_inviter_invitee ON invitations (project_id, inviter, invitee);

-- Table structure for table `session_infos`
DROP TABLE IF EXISTS session_infos;
CREATE TABLE session_infos (
  session_id VARCHAR(64) NOT NULL,
  status SMALLINT DEFAULT 0,
  table_checksum BYTEA,
  ccl_checksum BYTEA,
  engine_url VARCHAR(256),
  engine_url_for_self VARCHAR(256),
  job_info BYTEA,
  work_parties TEXT NOT NULL,
  output_names TEXT,
  warning BYTEA,
  created_at TIMESTAMP DEFAULT NULL,
  updated_at TIMESTAMP DEFAULT NULL,
  expired_at TIMESTAMP DEFAULT NULL,
  PRIMARY KEY (session_id)
);

-- Table structure for table `session_results`
DROP TABLE IF EXISTS session_results;
CREATE TABLE session_results (
  session_id VARCHAR(64) NOT NULL,
  result BYTEA,
  created_at TIMESTAMP DEFAULT NULL,
  updated_at TIMESTAMP DEFAULT NULL,
  expired_at TIMESTAMP DEFAULT NULL,
  PRIMARY KEY (session_id)
);

-- Table structure for table `locks`
DROP TABLE IF EXISTS locks;
CREATE TABLE locks (
  id SERIAL NOT NULL,
  "owner" VARCHAR(64),
  updated_at TIMESTAMP DEFAULT NULL,
  expired_at TIMESTAMP DEFAULT NULL,
  PRIMARY KEY (id)
);