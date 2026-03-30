-- 联合查询测试 (可以使用 alice 或 bob 用户运行)
-- 查询: 查找年龄 >= 20 且活跃用户的信用和消费信息
SELECT a.ID, a.credit_rank, a.income, b.order_amount, b.is_active FROM hive_test.user_credit a JOIN hive_test.user_stats b ON a.ID = b.ID WHERE a.age >= 20 AND b.is_active = 1;

