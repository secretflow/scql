-- SCQL Hive 测试设置脚本 (使用 root 用户运行)

-- 1. 创建用户 (需要指定 PARTY_CODE)
CREATE USER alice PARTY_CODE "alice" IDENTIFIED BY 'alice123';
CREATE USER bob PARTY_CODE "bob" IDENTIFIED BY 'bob123';

-- 2. 创建数据库
CREATE DATABASE IF NOT EXISTS hive_test;

-- 3. 创建 Alice 的表 (user_credit)
CREATE TABLE hive_test.user_credit (ID STRING, credit_rank INT, income INT, age INT) REF_TABLE=user_credit DB_TYPE='hive' OWNER=alice;

-- 4. 创建 Bob 的表 (user_stats)
CREATE TABLE hive_test.user_stats (ID STRING, order_amount INT, is_active INT) REF_TABLE=user_stats DB_TYPE='hive' OWNER=bob;

-- 5. 显示表
SHOW TABLES FROM hive_test;

-- 6. 描述表
DESCRIBE hive_test.user_credit;
DESCRIBE hive_test.user_stats;
