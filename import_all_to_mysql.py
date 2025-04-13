import pandas as pd
import mysql.connector
import sys
import os
from datetime import datetime
import glob
import csv
import io

# MySQL连接信息
db_config = {
    'host': '192.168.1.22',
    'user': 'habits',
    'password': 'Qwe123.005',
    'database': 'habits',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}

# 日期时间转换函数
def convert_date(date_str):
    if pd.isna(date_str) or date_str == '':
        return None
    try:
        return datetime.strptime(date_str, '%Y/%m/%d %H:%M:%S')
    except ValueError:
        return None

# 为特定CSV文件添加列名
def add_headers_to_csv(csv_file_path):
    # 检查文件名是否为SERVICES或DRIVERS
    file_name = os.path.basename(csv_file_path)
    if not ('SERVICES' in file_name or 'DRIVERS' in file_name):
        return csv_file_path  # 不需要处理的文件直接返回原路径
    
    print(f"为文件 {file_name} 添加列名...")
    
    # 定义列名
    headers = [
        'Name',
        'Display Name',
        'Status',
        'Startup Type',
        'ErrorControl',
        'Group',
        'Dependencies',
        'File Description',
        'File Version',
        'Company',
        'Product Name',
        'Description',
        'Filename',
        'Last Error',
        'Last Write Time',
        'Command-Line',
        'Process ID'
    ]
    
    # 读取原始CSV内容
    # 尝试多种编码方式读取文件
    encodings = ['gbk', 'gb2312', 'gb18030', 'utf-8', 'latin1']
    content = None
    used_encoding = None
    
    for encoding in encodings:
        try:
            with open(csv_file_path, 'r', encoding=encoding) as f:
                content = f.read()
                used_encoding = encoding
                print(f"使用 {encoding} 编码成功读取文件")
                break
        except UnicodeDecodeError:
            print(f"尝试使用 {encoding} 编码失败")
            if encoding == encodings[-1]:
                # 如果所有编码都失败，使用latin1作为最后的尝试
                with open(csv_file_path, 'r', encoding='latin1') as f:
                    content = f.read()
                    used_encoding = 'latin1'
                    print("所有编码尝试失败，使用latin1编码作为最后尝试")
    
    # 创建临时文件名
    temp_file_path = csv_file_path + '.temp'
    
    # 写入带有列名的新文件，使用相同的编码
    with open(temp_file_path, 'w', encoding=used_encoding) as f:
        f.write(','.join(headers) + '\n' + content)
    
    print(f"已成功为 {file_name} 添加列名")
    return temp_file_path

# 根据CSV文件名创建表名
def get_table_name(csv_file_path):
    # 从文件路径中提取文件名
    file_name = os.path.basename(csv_file_path)
    # 提取前缀作为表名 (例如: USB_DESKTOP-QTCL99K.csv -> USB)
    table_name = file_name.split('_')[0]
    return table_name

# 为SERVICES和DRIVERS表创建特定的表结构
def create_special_table(cursor, table_name):
    # 这两个表有相同的列结构
    if table_name not in ['SERVICES', 'DRIVERS']:
        return False
    
    # 确保数据库使用utf8mb4字符集
    cursor.execute("SET NAMES utf8mb4")
    cursor.execute("SET CHARACTER SET utf8mb4")
    cursor.execute("SET character_set_connection=utf8mb4")
        
    create_table_sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Name VARCHAR(255),
        Display_Name VARCHAR(255),
        Status VARCHAR(255),
        Startup_Type VARCHAR(255),
        ErrorControl VARCHAR(255),
        Group_Name VARCHAR(255),
        Dependencies TEXT,
        File_Description TEXT,
        File_Version VARCHAR(255),
        Company TEXT,
        Product_Name TEXT,
        Description TEXT,
        Filename VARCHAR(255),
        Last_Error TEXT,
        Last_Write_Time DATETIME,
        Command_Line TEXT,
        Process_ID VARCHAR(255)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """
    
    # 执行创建表的SQL
    cursor.execute(create_table_sql)
    print(f"{table_name}表创建成功")
    return True

# 动态创建表结构
def create_table(cursor, table_name, df):
    # 创建表的SQL语句开头
    create_table_sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
    """
    
    # 为每一列添加对应的字段定义
    column_defs = []
    for col in df.columns:
        # 清理列名
        clean_col = col.strip().replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '')
        
        # 根据数据类型确定字段类型
        if 'time' in clean_col.lower() or 'date' in clean_col.lower():
            column_defs.append(f"`{clean_col}` DATETIME")
        elif df[col].dtype == 'float64':
            column_defs.append(f"`{clean_col}` FLOAT")
        elif df[col].dtype == 'int64':
            column_defs.append(f"`{clean_col}` INT")
        else:
            # 对于文本字段，统一使用TEXT类型以确保中文字符能够正确存储
            # 即使是短文本也使用VARCHAR(255)以确保足够空间存储中文字符
            max_len = df[col].astype(str).str.len().max()
            if max_len < 100:
                column_defs.append(f"`{clean_col}` VARCHAR(255)")
            else:
                column_defs.append(f"`{clean_col}` TEXT")
    
    # 组合SQL语句
    create_table_sql += ",\n        ".join(column_defs)
    create_table_sql += "\n    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    
    # 执行创建表的SQL
    cursor.execute(create_table_sql)
    print(f"{table_name}表创建成功")

# 导入CSV文件到MySQL
def import_csv_to_mysql(csv_file_path):
    try:
        # 连接到MySQL数据库
        print(f"正在尝试连接到MySQL服务器: {db_config['host']}...")
        conn = mysql.connector.connect(**db_config)
        print(f"成功连接到MySQL服务器: {db_config['host']}")
        cursor = conn.cursor()
        
        # 设置连接的字符集为utf8mb4
        cursor.execute("SET NAMES utf8mb4")
        cursor.execute("SET CHARACTER SET utf8mb4")
        cursor.execute("SET character_set_connection=utf8mb4")
        
        # 检查是否需要添加列名
        file_name = os.path.basename(csv_file_path)
        if 'SERVICES' in file_name or 'DRIVERS' in file_name:
            # 为SERVICES和DRIVERS文件添加列名
            processed_file_path = add_headers_to_csv(csv_file_path)
        else:
            processed_file_path = csv_file_path
        
        # 读取CSV文件
        print(f"正在读取CSV文件: {processed_file_path}")
        # 尝试多种编码方式读取文件
        encodings = ['gbk', 'gb2312', 'gb18030', 'utf-8', 'latin1']
        for encoding in encodings:
            try:
                # 对于所有编码，先尝试不使用errors参数直接读取
                df = pd.read_csv(processed_file_path, encoding=encoding)
                print(f"使用 {encoding} 编码成功读取文件")
                break
            except UnicodeDecodeError:
                # 如果出现解码错误，尝试下一个编码
                print(f"尝试使用 {encoding} 编码失败: 解码错误")
                continue
            except Exception as e:
                # 其他错误，记录并尝试下一个编码
                print(f"尝试使用 {encoding} 编码失败: {e}")
                continue
        else:
            # 如果所有编码都失败，使用带有errors='replace'参数的方式再试一次
            for encoding in ['gbk', 'gb2312', 'gb18030']:
                try:
                    df = pd.read_csv(processed_file_path, encoding=encoding, errors='replace')
                    print(f"使用 {encoding} 编码(替换模式)成功读取文件")
                    break
                except Exception as e:
                    print(f"尝试使用 {encoding} 编码(替换模式)失败: {e}")
            else:
                # 如果仍然失败，使用latin1作为最后的尝试
                print("所有编码尝试失败，使用latin1编码作为最后尝试")
                df = pd.read_csv(processed_file_path, encoding='latin1')
                print("使用latin1编码成功读取文件")
        
        # 获取表名
        table_name = get_table_name(csv_file_path)
        
        # 清理列名（替换空格为下划线，移除特殊字符）
        df.columns = [col.strip().replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '') for col in df.columns]
        
        # 修复特殊列名问题
        column_mapping = {
            'Hub___Port': 'Hub_Port',  # 将三个下划线的列名映射到单个下划线
            'Group': 'Group_Name'      # 避免与MySQL关键字冲突
        }
        df.columns = [column_mapping.get(col, col) for col in df.columns]
        
        # 打印列名以便调试
        print(f"表 {table_name} 清理后的列名:", df.columns.tolist())
        
        # 对SERVICES和DRIVERS表进行特殊处理
        if table_name in ['SERVICES', 'DRIVERS']:
            # 使用预定义的表结构
            created = create_special_table(cursor, table_name)
            if not created:
                print(f"创建特殊表 {table_name} 失败，尝试使用动态创建")
                create_table(cursor, table_name, df)
        else:
            # 其他表使用动态创建表结构
            create_table(cursor, table_name, df)
        
        # 对SERVICES和DRIVERS表进行特殊列名映射
        if table_name in ['SERVICES', 'DRIVERS']:
            # 确保列名与预定义的表结构匹配
            special_columns_mapping = {
                'Command-Line': 'Command_Line',
                'Process_ID': 'Process_ID',
                'Last_Write_Time': 'Last_Write_Time'
            }
            
            # 应用特殊列名映射
            df.columns = [special_columns_mapping.get(col, col) for col in df.columns]
            
            # 打印映射后的列名以便调试
            print(f"表 {table_name} 映射后的列名:", df.columns.tolist())
        
        # 转换日期时间列
        date_columns = [col for col in df.columns if 'time' in col.lower() or 'date' in col.lower()]
        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].apply(convert_date)
        
        # 准备插入数据
        print(f"正在准备插入数据到 {table_name} 表...")
        for _, row in df.iterrows():
            # 构建插入语句
            columns = ', '.join([f"`{col}`" for col in df.columns])
            placeholders = ', '.join(['%s'] * len(df.columns))
            
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # 准备数据
            data = tuple(None if pd.isna(val) else val for val in row)
            
            # 执行插入
            cursor.execute(insert_query, data)
        
        # 提交事务
        conn.commit()
        print(f"成功导入 {len(df)} 条记录到 {table_name} 表")
        return True
        
    except mysql.connector.Error as err:
        print(f"MySQL连接错误: {err}")
        print(f"尝试连接的服务器: {db_config['host']}")
        print(f"尝试连接的数据库: {db_config['database']}")
        print(f"尝试连接的用户: {db_config['user']}")
        if 'conn' in locals() and conn.is_connected():
            conn.rollback()
        return False
    except Exception as e:
        print(f"导入过程中出错: {e}")
        if 'conn' in locals() and conn.is_connected():
            conn.rollback()
        return False
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("数据库连接已关闭")
        
        # 如果创建了临时文件，删除它
        if 'processed_file_path' in locals() and processed_file_path != csv_file_path:
            try:
                os.remove(processed_file_path)
                print(f"已删除临时文件: {processed_file_path}")
            except Exception as e:
                print(f"删除临时文件时出错: {e}")

def main():
    # 设置CSV文件目录
    csv_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'extracted', '2298_DESKTOP-QTCL99K_rp')
    
    if not os.path.exists(csv_dir):
        print(f"错误: 找不到目录 {csv_dir}")
        return
    
    # 获取目录中所有CSV文件
    csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))
    
    if not csv_files:
        print(f"错误: 在 {csv_dir} 中没有找到CSV文件")
        return
    
    # 导入所有CSV文件
    success_count = 0
    for csv_file in csv_files:
        print(f"\n开始导入数据从 {csv_file} 到 MySQL...")
        if import_csv_to_mysql(csv_file):
            success_count += 1
    
    print(f"\n导入过程完成，成功导入 {success_count}/{len(csv_files)} 个文件")

if __name__ == "__main__":
    main()