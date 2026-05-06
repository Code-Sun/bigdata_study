package com.bigdata.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MysqlJdbcDemo {

    // ── 连接配置（按需修改） ─────────────────────────────────────────────
    private static final String URL      = "jdbc:mysql://localhost:3306/test_db"
                                         + "?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8";
    private static final String USER     = "root";
    private static final String PASSWORD = "your_password";

    // ── 获取连接 ────────────────────────────────────────────────────────
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }

    // ── 查询：返回结果集列表 ─────────────────────────────────────────────
    public static List<Map<String, Object>> query(String sql, Object... params) {
        List<Map<String, Object>> result = new ArrayList<>();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }

            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= colCount; i++) {
                        row.put(meta.getColumnLabel(i), rs.getObject(i));
                    }
                    result.add(row);
                }
            }
        } catch (SQLException e) {
            System.err.println("查询失败: " + e.getMessage());
        }
        return result;
    }

    // ── 执行 DML（INSERT / UPDATE / DELETE） ────────────────────────────
    public static int update(String sql, Object... params) {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            return ps.executeUpdate();

        } catch (SQLException e) {
            System.err.println("执行失败: " + e.getMessage());
            return -1;
        }
    }

    // ── 分批插入（每批 batchSize 条，失败自动回滚当批） ──────────────────
    public static void batchInsert(String sql, List<Object[]> allData, int batchSize) {
        int total = allData.size();
        int batchCount = (int) Math.ceil((double) total / batchSize);
        int successRows = 0;

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);

            for (int batch = 0; batch < batchCount; batch++) {
                int from = batch * batchSize;
                int to   = Math.min(from + batchSize, total);
                List<Object[]> segment = allData.subList(from, to);

                try {
                    for (Object[] params : segment) {
                        for (int i = 0; i < params.length; i++) {
                            ps.setObject(i + 1, params[i]);
                        }
                        ps.addBatch();
                    }
                    ps.executeBatch();
                    conn.commit();
                    ps.clearBatch();
                    successRows += segment.size();
                    System.out.printf("第 %d/%d 批完成，本批 %d 条，累计 %d/%d 条%n",
                            batch + 1, batchCount, segment.size(), successRows, total);

                } catch (SQLException e) {
                    conn.rollback();
                    ps.clearBatch();
                    System.err.printf("第 %d 批插入失败，已回滚，原因: %s%n", batch + 1, e.getMessage());
                }
            }

            System.out.printf("全部完成：成功 %d 条，失败 %d 条%n", successRows, total - successRows);

        } catch (SQLException e) {
            System.err.println("数据库连接异常: " + e.getMessage());
        }
    }

    // ── 打印结果集 ──────────────────────────────────────────────────────
    private static void printResult(List<Map<String, Object>> rows) {
        if (rows.isEmpty()) {
            System.out.println("(无数据)");
            return;
        }
        // 打印表头
        System.out.println(String.join(" | ", rows.get(0).keySet()));
        System.out.println("-".repeat(60));
        // 打印行数据
        for (Map<String, Object> row : rows) {
            System.out.println(String.join(" | ",
                    row.values().stream()
                       .map(v -> v == null ? "NULL" : v.toString())
                       .toArray(String[]::new)));
        }
        System.out.println("共 " + rows.size() + " 行");
    }

    // ── 示例入口 ────────────────────────────────────────────────────────
    public static void main(String[] args) {

        // 1. 简单查询
        System.out.println("=== 查询所有用户 ===");
        List<Map<String, Object>> users = query("SELECT * FROM users LIMIT 10");
        printResult(users);

        // 2. 带参数查询
        System.out.println("\n=== 条件查询 ===");
        List<Map<String, Object>> filtered = query(
                "SELECT id, name, email FROM users WHERE age > ? AND status = ?",
                18, "active");
        printResult(filtered);

        // 3. 插入单条数据
        System.out.println("\n=== 插入数据 ===");
        int affected = update(
                "INSERT INTO users (name, email, age, status) VALUES (?, ?, ?, ?)",
                "张三", "zhangsan@example.com", 25, "active");
        System.out.println("插入影响行数: " + affected);

        // 4. 分批插入 10万条数据，每批 10000 条
        System.out.println("\n=== 分批插入 100000 条 ===");
        List<Object[]> allData = new ArrayList<>(100000);
        for (int i = 1; i <= 100000; i++) {
            allData.add(new Object[]{"用户" + i, "user" + i + "@example.com", 20 + (i % 40), "active"});
        }
        batchInsert("INSERT INTO users (name, email, age, status) VALUES (?, ?, ?, ?)", allData, 10000);

        // 5. 更新数据
        System.out.println("\n=== 更新数据 ===");
        int updated = update("UPDATE users SET status = ? WHERE name = ?", "inactive", "张三");
        System.out.println("更新影响行数: " + updated);
    }
}
