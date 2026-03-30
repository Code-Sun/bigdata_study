package com.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Flink SQL 文件执行器
 *
 * 读取 SQL 文件，按分号分割，依次执行每条 Flink SQL 语句。
 * 支持 CREATE TABLE、INSERT INTO 等 Flink SQL 语法。
 * SQL 文件中可以用 -- 做单行注释，/* ... * / 做多行注释。
 *
 * 用法：
 *   java -cp flink-wordcount-1.0-SNAPSHOT.jar com.example.FlinkSQLRunner <sql文件路径>
 *
 * 示例：
 *   java -cp target/flink-wordcount-1.0-SNAPSHOT.jar com.example.FlinkSQLRunner jobs/mysql_to_print.sql
 */
public class FlinkSQLRunner {

    public static void main(String[] args) throws Exception {
        System.setOut(new PrintStream(System.out, true, StandardCharsets.UTF_8));

        if (args.length < 1) {
            System.err.println("用法: FlinkSQLRunner <sql文件路径>");
            System.err.println("示例: FlinkSQLRunner jobs/mysql_to_print.sql");
            System.exit(1);
        }

        String sqlFilePath = args[0];
        System.out.println("========================================");
        System.out.println("Flink SQL Runner");
        System.out.println("SQL 文件: " + sqlFilePath);
        System.out.println("========================================");

        // 读取 SQL 文件
        String content = new String(Files.readAllBytes(Paths.get(sqlFilePath)), StandardCharsets.UTF_8);

        // 解析 SQL 语句
        List<String> statements = parseSQLStatements(content);
        System.out.println("解析到 " + statements.size() + " 条 SQL 语句\n");

        // 创建 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 依次执行每条 SQL
        int index = 0;
        for (String sql : statements) {
            index++;
            String preview = sql.length() > 80 ? sql.substring(0, 80) + "..." : sql;
            System.out.println("[" + index + "/" + statements.size() + "] 执行: " + preview);

            try {
                tEnv.executeSql(sql);
                System.out.println("  -> 成功\n");
            } catch (Exception e) {
                System.err.println("  -> 失败: " + e.getMessage() + "\n");
                throw e;
            }
        }

        System.out.println("========================================");
        System.out.println("所有 SQL 执行完毕，CDC 任务运行中...");
        System.out.println("========================================");
    }

    /**
     * 解析 SQL 文件内容，按分号分割，去除注释和空语句
     */
    static List<String> parseSQLStatements(String content) {
        // 去除多行注释
        content = content.replaceAll("/\\*.*?\\*/", " ");

        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;

        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);

            // 处理单行注释
            if (!inSingleQuote && c == '-' && i + 1 < content.length() && content.charAt(i + 1) == '-') {
                int lineEnd = content.indexOf('\n', i);
                if (lineEnd == -1) break;
                i = lineEnd;
                current.append(' ');
                continue;
            }

            // 跟踪单引号（字符串内的分号不分割）
            if (c == '\'') {
                inSingleQuote = !inSingleQuote;
            }

            // 分号分割语句
            if (c == ';' && !inSingleQuote) {
                String sql = current.toString().trim();
                if (!sql.isEmpty()) {
                    statements.add(sql);
                }
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        // 最后一条可能没有分号
        String last = current.toString().trim();
        if (!last.isEmpty()) {
            statements.add(last);
        }

        return statements;
    }
}
