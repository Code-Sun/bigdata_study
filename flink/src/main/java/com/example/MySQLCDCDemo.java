package com.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Flink CDC Demo：监听 MySQL 变更并打印到控制台
 */
public class MySQLCDCDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. MySQL CDC Source
        tEnv.executeSql(
            "CREATE TABLE mysql_users (" +
            "  id INT NOT NULL," +
            "  name STRING," +
            "  age INT," +
            "  city STRING," +
            "  PRIMARY KEY (id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'mysql-cdc'," +
            "  'hostname' = 'localhost'," +
            "  'port' = '3306'," +
            "  'username' = 'root'," +
            "  'password' = '123456'," +
            "  'database-name' = 'test_db'," +
            "  'table-name' = 'users'," +
            "  'server-time-zone' = 'UTC'," +
            "  'jdbc.properties.useUnicode' = 'true'," +
            "  'jdbc.properties.characterEncoding' = 'UTF-8'" +
            ")"
        );

        // 2. 转成 DataStream 用 System.out 打印，避免编码问题
        Table table = tEnv.sqlQuery("SELECT * FROM mysql_users");
        tEnv.toChangelogStream(table)
            .addSink(new SinkFunction<Row>() {
                @Override
                public void invoke(Row value, Context context) {
                    System.out.println(value);
                }
            });

        env.execute("MySQL CDC Demo");
    }
}
