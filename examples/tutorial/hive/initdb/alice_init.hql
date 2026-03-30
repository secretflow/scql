-- Hive initialization script for Alice
-- Run this in Hive/beeline: beeline -u jdbc:hive2://localhost:10000 -f alice_init.hql

CREATE DATABASE IF NOT EXISTS alice;
USE alice;

DROP TABLE IF EXISTS user_credit;
CREATE TABLE user_credit (
    ID STRING,
    credit_rank INT,
    income INT,
    age INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

INSERT INTO user_credit VALUES
    ('id0001', 6, 100000, 20),
    ('id0002', 5, 90000, 19),
    ('id0003', 6, 89700, 32),
    ('id0005', 6, 607000, 30),
    ('id0006', 5, 30070, 25),
    ('id0007', 6, 12070, 28),
    ('id0008', 6, 200800, 50),
    ('id0009', 6, 607000, 30),
    ('id0010', 5, 30070, 25),
    ('id0011', 5, 12070, 28),
    ('id0012', 6, 200800, 50),
    ('id0013', 5, 30070, 25),
    ('id0014', 5, 12070, 28),
    ('id0015', 6, 200800, 18),
    ('id0016', 5, 30070, 26),
    ('id0017', 5, 12070, 27),
    ('id0018', 6, 200800, 16),
    ('id0019', 6, 30070, 25),
    ('id0020', 5, 12070, 28);

