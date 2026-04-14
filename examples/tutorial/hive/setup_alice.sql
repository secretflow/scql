-- Alice 创建自己的表 (使用 alice 用户运行)
-- REF_TABLE 需要使用 db.table 格式
CREATE DATABASE IF NOT EXISTS hive_test;
CREATE TABLE hive_test.user_credit (ID STRING, credit_rank INT, income INT, age INT) REF_TABLE=default.user_credit DB_TYPE='hive';
DESCRIBE hive_test.user_credit;
