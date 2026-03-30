-- Bob 创建自己的表 (使用 bob 用户运行)
-- REF_TABLE 需要使用 db.table 格式
CREATE DATABASE IF NOT EXISTS hive_test;
CREATE TABLE hive_test.user_stats (ID STRING, order_amount INT, is_active INT) REF_TABLE=default.user_stats DB_TYPE='hive';
DESCRIBE hive_test.user_stats;
