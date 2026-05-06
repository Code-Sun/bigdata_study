import pandas as pd
import sys


def read_excel(file_path: str, sheet_name=0):
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        print(f"文件: {file_path}")
        print(f"Sheet: {sheet_name}")
        print(f"行数: {len(df)}, 列数: {len(df.columns)}")
        print(f"列名: {list(df.columns)}")
        print("\n前5行数据:")
        print(df.head())
        return df
    except FileNotFoundError:
        print(f"错误: 文件 '{file_path}' 不存在")
        sys.exit(1)
    except Exception as e:
        print(f"错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python read_excel.py <excel文件路径> [sheet名称或索引]")
        print("示例: python read_excel.py data.xlsx")
        print("      python read_excel.py data.xlsx Sheet1")
        sys.exit(1)

    file_path = sys.argv[1]
    sheet = sys.argv[2] if len(sys.argv) > 2 else 0

    df = read_excel(file_path, sheet_name=sheet)
