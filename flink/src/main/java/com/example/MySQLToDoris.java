package com.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink CDC：MySQL 实时同步到 Doris
 *
 * 使用 Flink SQL 方式，通过 MySQL CDC Source 捕获变更，写入 Doris Sink
 * 支持全量 + 增量同步
 */
public class MySQLToDoris {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. MySQL CDC Source（修改为你的实际配置）
        tEnv.executeSql(
            "CREATE TABLE mysql_source (" +
            "  id BIGINT NOT NULL," +
            "  name STRING," +
            "  age INT," +
            "  create_time TIMESTAMP(3)," +
            "  PRIMARY KEY (id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'mysql-cdc'," +
            "  'hostname' = 'localhost'," +
            "  'port' = '3306'," +
            "  'username' = 'root'," +
            "  'password' = 'your_password'," +
            "  'database-name' = 'your_database'," +
            "  'table-name' = 'your_table'" +
            ")"
        );

        // 2. Doris Sink（修改为你的实际配置）
        tEnv.executeSql(
            "CREATE TABLE doris_sink (" +
            "  id BIGINT NOT NULL," +
            "  name STRING," +
            "  age INT," +
            "  create_time TIMESTAMP(3)," +
            "  PRIMARY KEY (id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'doris'," +
            "  'fenodes' = 'localhost:8030'," +
            "  'table.identifier' = 'your_database.your_table'," +
            "  'username' = 'root'," +
            "  'password' = 'your_password'," +
            "  'sink.properties.format' = 'json'," +
            "  'sink.properties.read_json_by_line' = 'true'," +
            "  'sink.enable-2pc' = 'true'," +
            "  'sink.label-prefix' = 'flink_cdc_doris'" +
            ")"
        );

        // 3. 实时同步
        tEnv.executeSql("INSERT INTO doris_sink SELECT * FROM mysql_source");
    }
}
