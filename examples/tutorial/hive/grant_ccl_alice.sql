-- Alice 授权 CCL (alice 用户运行)
-- 允许 Alice 自己查看 user_credit 表的所有列
GRANT SELECT PLAINTEXT(ID, credit_rank, income, age) ON hive_test.user_credit TO alice;
-- 允许 Bob 查看 user_credit 表的所有列
GRANT SELECT PLAINTEXT(ID, credit_rank, income, age) ON hive_test.user_credit TO bob;
