# Flink Demo 项目

## 项目简介

包含两个 Demo：
1. WordCount — Flink 经典入门，从内存数据源分词统计
2. MySQLCDCDemo — Flink CDC 实时捕获 MySQL 变更并打印

## 项目结构

```
flink/
├── pom.xml
├── README.md
└── src/main/java/com/example/
    ├── WordCount.java        # 词频统计
    ├── MySQLCDCDemo.java     # MySQL CDC -> 控制台打印
    └── MySQLToDoris.java     # MySQL CDC -> Doris（模板，需要 Doris 环境）
```

## 环境要求

- JDK 11+（本项目使用 JDK 17）
- Maven 3.9+（安装在 /opt/apache-maven-3.9.14）
- Docker（运行 MySQL 容器）

## Maven 配置

### 本地仓库

```
/Users/a123/sunyifeng/maven_repo
```

### 镜像仓库（阿里云）

配置在 `/opt/apache-maven-3.9.14/conf/settings.xml` 中：

```xml
<mirror>
    <id>aliyun</id>
    <name>Aliyun Maven Mirror</name>
    <url>https://maven.aliyun.com/repository/public</url>
    <mirrorOf>central</mirrorOf>
</mirror>
```

---

## 一、WordCount Demo

### 打包

```bash
# 瘦 jar（只有项目代码，约 5KB）
mvn package

# fat jar（包含所有依赖，通过 -Pfat 激活 shade 插件）
mvn package -Pfat

# fat jar 静默模式（-q 是 --quiet 的缩写，只输出错误信息，不打印下载和编译日志）
mvn package -Pfat -q

# 下载依赖到 target/lib 目录
mvn dependency:copy-dependencies -DoutputDirectory=target/lib

# 下载依赖到默认的 target/dependency 目录
mvn dependency:copy-dependencies
```

在项目根目录执行需加 `-f flink/pom.xml`：

```bash
mvn -f flink/pom.xml package -Pfat
mvn -f flink/pom.xml dependency:copy-dependencies
```

### 运行

```bash
# 运行 fat jar（本地开发推荐）
java -jar target/flink-wordcount-1.0-SNAPSHOT.jar

# 运行瘦 jar（需要先下载依赖）
java -cp "target/original-flink-wordcount-1.0-SNAPSHOT.jar:target/lib/*" com.example.WordCount
java -cp "target/original-flink-wordcount-1.0-SNAPSHOT.jar:target/dependency/*" com.example.WordCount
```

### 预期输出

```
(hello,3)
(flink,3)
(world,2)
(is,1)
(great,1)
```

### target 产物说明

| 类型 | 文件名 | 用途 |
|------|--------|------|
| fat jar | flink-wordcount-1.0-SNAPSHOT.jar | 本地直接 `java -jar` 运行 |
| 瘦 jar | original-flink-wordcount-1.0-SNAPSHOT.jar | 提交到 Flink 集群执行 |
| 精简 pom | dependency-reduced-pom.xml | shade 插件副产品，已在 .gitignore 中忽略 |

---

## 二、Flink CDC Demo（MySQL -> 控制台）

### 1. 启动 MySQL（Docker）

```bash
# 查看正在运行的容器
docker ps

# 查看所有容器（包括已停止的）
docker ps -a


docker ps --filter name=mysql --format "{{.Names}} {{.Status}}"


# 停止容器（数据不会丢，下次可以 docker start 重新启动）
docker stop mysql

# 重新启动已停止的容器
docker start mysql

# 彻底删除容器（数据会丢失）
docker rm mysql
```

创建并启动 MySQL 容器：

```bash
docker run -d --name mysql \
  -e MYSQL_ROOT_PASSWORD=123456 \
  -e MYSQL_DATABASE=test_db \
  -p 3306:3306 \
  mysql:8.0 --server-id=1 --log-bin=mysql-bin --binlog-format=ROW
```

关键参数说明：
- `--server-id=1` — binlog 复制需要的唯一 ID
- `--log-bin=mysql-bin` — 开启 binlog
- `--binlog-format=ROW` — CDC 要求 ROW 格式

### 2. 修复 MySQL 字符集

MySQL 8.0 Docker 镜像默认客户端字符集是 latin1，会导致中文乱码，需要修复：

```bash
docker exec mysql mysql -uroot -p123456 -e "
SET GLOBAL character_set_client = utf8mb4;
SET GLOBAL character_set_results = utf8mb4;
SET GLOBAL character_set_connection = utf8mb4;
"
```

### 3. 创建表并插入测试数据

注意：必须加 `--default-character-set=utf8mb4` 确保中文正确写入。

```bash
docker exec mysql mysql -uroot -p123456 --default-character-set=utf8mb4 test_db -e "
CREATE TABLE IF NOT EXISTS users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(50),
  age INT,
  city VARCHAR(50)
);

INSERT INTO users (name, age, city) VALUES
('张三', 25, '北京'),
('李四', 30, '上海'),
('王五', 28, '广州'),
('赵六', 35, '深圳'),
('孙七', 22, '杭州'),
('周八', 27, '成都'),
('吴九', 33, '武汉'),
('郑十', 29, '南京'),
('钱一', 31, '西安'),
('陈二', 26, '重庆');
"
```

### 4. 连接 MySQL

```bash
# 命令行方式
docker exec -it mysql mysql -uroot -p123456 --default-character-set=utf8mb4 test_db

# 客户端工具（Navicat / IDEA Database 等）
# Host: localhost, Port: 3306, User: root, Password: 123456, Database: test_db
```

### 5. 打包并运行 CDC Demo

```bash
# 打 fat jar
mvn -f flink/pom.xml clean package -Pfat -q

# 运行（在 macOS Terminal.app 中执行，Kiro 终端中文显示有问题）
java -cp flink/target/flink-wordcount-1.0-SNAPSHOT.jar com.example.MySQLCDCDemo
```

### 6. 验证实时同步

CDC 启动后会先全量同步已有数据（`+I` 表示 INSERT），然后进入 binlog 监听模式。

在另一个终端操作 MySQL，CDC 会实时捕获：

```bash
# 插入
docker exec mysql mysql -uroot -p123456 --default-character-set=utf8mb4 test_db \
  -e "INSERT INTO users (name, age, city) VALUES ('林峰', 38, '福州');"

# 更新
docker exec mysql mysql -uroot -p123456 --default-character-set=utf8mb4 test_db \
  -e "UPDATE users SET age = 99 WHERE id = 1;"

# 删除
docker exec mysql mysql -uroot -p123456 --default-character-set=utf8mb4 test_db \
  -e "DELETE FROM users WHERE id = 10;"
```

CDC 输出格式：
- `+I[...]` — INSERT，新增数据
- `-U[...]` — UPDATE BEFORE，更新前的旧值
- `+U[...]` — UPDATE AFTER，更新后的新值
- `-D[...]` — DELETE，删除的数据

### 踩坑记录

1. **时区不匹配**：MySQL 容器默认 UTC，需要在 CDC Source 配置 `'server-time-zone' = 'UTC'`
2. **中文乱码**：MySQL 8.0 Docker 默认 `character_set_client=latin1`，需要手动设置为 utf8mb4，并且插入数据时加 `--default-character-set=utf8mb4`
3. **SPI 服务发现失败**：shade 打 fat jar 时 `META-INF/services` 文件会被覆盖，需要在 shade 配置中加 `ServicesResourceTransformer`

   报错信息：`Could not find any factories that implement 'org.apache.flink.table.delegation.ExecutorFactory' in the classpath.`

   原因：Java 的 SPI 机制通过 `META-INF/services/` 目录下的文件注册服务实现。Flink 的多个 jar 包里都有同名的 SPI 文件（如 `org.apache.flink.table.factories.Factory`），每个 jar 注册各自的实现类。shade 插件合并 jar 时遇到同名文件只保留最后一个，前面的全被覆盖，导致 Flink Table Planner 注册的 ExecutorFactory 丢失。

   解决：在 shade 配置中加 `ServicesResourceTransformer`，它会把所有同名 SPI 文件的内容合并而不是覆盖，确保所有服务实现都被保留：

   ```xml
   <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
   ```
4. **flink-table-planner-loader 不兼容 fat jar**：改用 `flink-table-planner_2.12` 依赖
5. **缺少 flink-connector-base**：CDC connector 依赖此模块，需要显式添加

---

## 三、Flink SQL Runner（多 CDC 任务执行器）

通过 SQL 文件定义多个 CDC 任务，FlinkSQLRunner 读取并依次执行。

### 项目结构

```
flink/
├── src/main/java/com/example/
│   └── FlinkSQLRunner.java    # SQL 文件执行器
└── jobs/
    ├── mysql_to_print.sql     # 单任务示例
    └── multi_cdc_tasks.sql    # 多任务示例
```

### 运行方式

```bash
# 打包
mvn -f flink/pom.xml clean package -Pfat -q

# 运行单任务
java -cp flink/target/flink-wordcount-1.0-SNAPSHOT.jar com.example.FlinkSQLRunner flink/jobs/mysql_to_print.sql

# 运行多任务
java -cp flink/target/flink-wordcount-1.0-SNAPSHOT.jar com.example.FlinkSQLRunner flink/jobs/multi_cdc_tasks.sql
```

### SQL 文件格式

- 每条 SQL 以分号 `;` 结尾
- 支持 `--` 单行注释和 `/* */` 多行注释
- 字符串内的分号不会被误分割
- 一个文件中可定义多个 Source/Sink 和同步任务

### 多任务示例验证

`multi_cdc_tasks.sql` 中定义了两个任务：
1. 任务1：users 表全量 + 增量同步到控制台
2. 任务2：users 表按城市聚合统计（人数、平均年龄）

插入一条测试数据：

```bash
docker exec mysql mysql -uroot -p123456 --default-character-set=utf8mb4 test_db \
  -e "INSERT INTO users (name, age, city) VALUES ('赵磊', 28, '北京');"
```

实时输出：

```
users> +I[12, 赵磊, 28, 北京]           -- 任务1：捕获新增记录
city_stats> -U[北京, 1, 25]              -- 任务2：撤回旧统计（1人，平均25岁）
city_stats> +U[北京, 2, 26]              -- 任务2：更新新统计（2人，平均26岁）
```

输出前缀说明：
- `users>` / `city_stats>` — 由 print connector 的 `print-identifier` 配置决定，用于区分不同任务的输出
- `+I` — INSERT，新增
- `-U` — UPDATE BEFORE，更新前旧值
- `+U` — UPDATE AFTER，更新后新值
- `-D` — DELETE，删除

---

## pom.xml 中的 Profile 机制

默认打瘦 jar，通过 `-Pfat` 激活 shade 插件打 fat jar：

```xml
<profiles>
    <profile>
        <id>fat</id>  <!-- mvn package -Pfat 激活 -->
        <build>
            <plugins>
                <!-- maven-shade-plugin + ServicesResourceTransformer -->
            </plugins>
        </build>
    </profile>
</profiles>
```

---

## Git 相关

### 远程仓库

```
https://github.com/Code-Sun/bigdata_study.git
```

### 常用命令

```bash
git add -A
git commit -m "提交信息"
git push

# 首次关联远程仓库
git remote add origin <仓库地址>
git push -u origin main

# 远程已有内容时合并
git pull origin main --allow-unrelated-histories --no-rebase
```

### .gitignore 忽略的文件

- `target/` — 编译产物
- `dependency-reduced-pom.xml` — shade 插件副产品
- `.idea/`、`*.iml` — IDE 配置
- `.DS_Store` — macOS 系统文件
- `*.log` — 日志文件
- `.env` — 环境配置
