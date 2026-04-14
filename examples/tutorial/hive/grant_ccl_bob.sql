-- Bob 授权 CCL (bob 用户运行)
-- 允许 Bob 自己查看 user_stats 表的所有列
GRANT SELECT PLAINTEXT(ID, order_amount, is_active) ON hive_test.user_stats TO bob;
-- 允许 Alice 查看 user_stats 表的所有列
GRANT SELECT PLAINTEXT(ID, order_amount, is_active) ON hive_test.user_stats TO alice;
