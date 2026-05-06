"""
通过 JayDeBeApi 使用 JDBC 连接数据库并读取数据。

依赖安装:
    pip install JayDeBeApi JPype1

JDBC Driver 下载示例:
    MySQL:      https://dev.mysql.com/downloads/connector/j/
    PostgreSQL: https://jdbc.postgresql.org/download/
    Hive:       https://www.cloudera.com/downloads/connectors/hive/jdbc/
    Presto:     https://prestodb.io/docs/current/installation/jdbc.html
"""

import jaydebeapi
import jpype
import pandas as pd
from contextlib import contextmanager


# ── 连接配置（按需修改） ────────────────────────────────────────────────────

JDBC_CONFIGS = {
    "mysql": {
        "driver": "com.mysql.cj.jdbc.Driver",
        "url": "jdbc:mysql://localhost:3306/test_db?useSSL=false&serverTimezone=UTC",
        "jar": "/path/to/mysql-connector-java-8.x.x.jar",
    },
    "postgresql": {
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://localhost:5432/test_db",
        "jar": "/path/to/postgresql-42.x.x.jar",
    },
    "hive": {
        "driver": "org.apache.hive.jdbc.HiveDriver",
        "url": "jdbc:hive2://localhost:10000/default",
        "jar": "/path/to/hive-jdbc-standalone.jar",
    },
    "presto": {
        "driver": "com.facebook.presto.jdbc.PrestoDriver",
        "url": "jdbc:presto://localhost:8080/hive/default",
        "jar": "/path/to/presto-jdbc-xxx.jar",
    },
}


# ── 连接工具 ───────────────────────────────────────────────────────────────

@contextmanager
def jdbc_connection(db_type: str, user: str, password: str):
    """上下文管理器，自动关闭连接。"""
    cfg = JDBC_CONFIGS[db_type]
    conn = jaydebeapi.connect(
        cfg["driver"],
        cfg["url"],
        [user, password],
        cfg["jar"],
    )
    try:
        yield conn
    finally:
        conn.close()


# ── 查询函数 ───────────────────────────────────────────────────────────────

def query_to_dataframe(conn, sql: str, params=None) -> pd.DataFrame:
    """执行 SQL 并返回 DataFrame。"""
    with conn.cursor() as cursor:
        cursor.execute(sql, params or [])
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns)


def execute_sql(conn, sql: str, params=None) -> None:
    """执行 DDL / DML（无返回值）。"""
    with conn.cursor() as cursor:
        cursor.execute(sql, params or [])
    conn.commit()


def batch_insert(conn, table: str, df: pd.DataFrame, batch_size: int = 1000) -> None:
    """将 DataFrame 分批写入目标表。"""
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    with conn.cursor() as cursor:
        for start in range(0, len(df), batch_size):
            batch = df.iloc[start: start + batch_size]
            cursor.executemany(sql, batch.values.tolist())
    conn.commit()
    print(f"已写入 {len(df)} 行到 {table}")


# ── 示例入口 ───────────────────────────────────────────────────────────────

def demo_mysql():
    with jdbc_connection("mysql", user="root", password="your_password") as conn:
        # 查询
        df = query_to_dataframe(conn, "SELECT * FROM orders LIMIT 100")
        print(df.head())
        print(f"共 {len(df)} 行，{len(df.columns)} 列")

        # 带参数查询
        df2 = query_to_dataframe(
            conn,
            "SELECT * FROM orders WHERE status = ? AND amount > ?",
            params=["PAID", 100],
        )
        print(df2)


def demo_hive():
    with jdbc_connection("hive", user="hive", password="") as conn:
        df = query_to_dataframe(conn, "SELECT * FROM default.user_events LIMIT 50")
        print(df)


def demo_presto():
    with jdbc_connection("presto", user="presto", password="") as conn:
        df = query_to_dataframe(
            conn,
            "SELECT date, SUM(amount) AS total FROM hive.sales.orders GROUP BY date ORDER BY date DESC LIMIT 30",
        )
        print(df)


if __name__ == "__main__":
    # 取消注释需要运行的示例
    demo_mysql()
    # demo_hive()
    # demo_presto()
