-- Hive initialization script for Bob
-- Run this in Hive/beeline: beeline -u jdbc:hive2://localhost:10000 -f bob_init.hql

CREATE DATABASE IF NOT EXISTS bob;
USE bob;

DROP TABLE IF EXISTS user_stats;
CREATE TABLE user_stats (
    ID STRING,
    order_amount INT,
    is_active INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

INSERT INTO user_stats VALUES
    ('id0001', 5000, 1),
    ('id0002', 3000, 1),
    ('id0003', 8000, 0),
    ('id0005', 12000, 1),
    ('id0006', 1500, 1),
    ('id0007', 2500, 0),
    ('id0008', 9500, 1),
    ('id0009', 7000, 1),
    ('id0010', 500, 0),
    ('id0011', 3500, 1),
    ('id0012', 15000, 1),
    ('id0013', 2000, 0),
    ('id0014', 4500, 1),
    ('id0015', 6500, 1),
    ('id0016', 1000, 0),
    ('id0017', 8500, 1),
    ('id0018', 11000, 1),
    ('id0019', 3200, 1),
    ('id0020', 7500, 0);
