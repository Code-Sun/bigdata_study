-- ============================================
-- Flink CDC 示例：MySQL users 表 -> 控制台打印
-- ============================================

-- MySQL CDC Source
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

-- Print Sink
CREATE TABLE print_users (
  id INT NOT NULL,
  name STRING,
  age INT,
  city STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'print'
);

-- 同步任务
INSERT INTO print_users SELECT * FROM mysql_users;
