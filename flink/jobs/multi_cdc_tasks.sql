-- ============================================
-- 多 CDC 任务示例
-- 可以在一个 SQL 文件中定义多个 Source/Sink 和同步任务
-- ============================================

-- ========== 任务1：users 表同步 ==========

CREATE TABLE mysql_users (
  id INT NOT NULL,
  name STRING,
  age INT,
  city STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = 'localhost',
  'port' = '3306',
  'username' = 'root',
  'password' = '123456',
  'database-name' = 'test_db',
  'table-name' = 'users',
  'server-time-zone' = 'UTC'
);

CREATE TABLE print_users (
  id INT NOT NULL,
  name STRING,
  age INT,
  city STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'print',
  'print-identifier' = 'users'
);

INSERT INTO print_users SELECT * FROM mysql_users;

-- ========== 任务2：users 表按城市聚合统计 ==========

CREATE TABLE print_city_stats (
  city STRING,
  user_count BIGINT NOT NULL,
  avg_age INT,
  PRIMARY KEY (city) NOT ENFORCED
) WITH (
  'connector' = 'print',
  'print-identifier' = 'city_stats'
);

INSERT INTO print_city_stats
SELECT city, COUNT(*) AS user_count, CAST(AVG(age) AS INT) AS avg_age
FROM mysql_users
GROUP BY city;
