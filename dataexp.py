###############################################################################
## File       : dataexp.py
## Purpose    : 在数据开发前获取数据字典和数据概况
## Title      : 数据探查
## Category   : 数据开发
## Version    : v0.5.0
## Company    : duanzhihui.com
## Author     : 段智慧
## Description: 数据探查
##                  数据字典 根据"表清单"获取表的数据字典。
##                  示例数据 获取5行数据作为示例数据。
##                  统计数据 根据 统计标示 获取统计数据。
## History    : 2024-03-21  v0.1.1    段智慧 重构代码支持多数据库    
##              2024-04-11  v0.2.0    段智慧 增加字段值TOP10频率统计
##              2025-04-11  v0.3.0    段智慧 减少excel公式
##              2025-04-11  v0.4.0    段智慧 不拆分sheet
##              2025-04-21  v0.5.0    段智慧 支持数据库连接采用SSH隧道
###############################################################################

import datetime
import numpy as np
import xlwings as xw
import pandas as pd
import re
import copy as cp
import yaml
from abc import ABC, abstractmethod
import os
from sqlalchemy import create_engine
import sshtunnel
from urllib.parse import urlparse


class DataExplorer(ABC):
    """数据探查基类，定义接口规范"""
    
    # 添加默认字段统计标示
    dft_ind_list = [
        [1,1,1,0,1,0,0,0,0,0,1,1,0], # 默认
        [1,1,1,1,1,1,1,1,1,1,1,1,0], # 字符串
        [1,1,1,0,1,0,0,0,0,0,1,1,1], # 数值
        [1,1,1,0,1,0,0,0,0,0,1,1,0], # 日期
        [2,0,0,0,0,0,0,0,0,0,0,0,0]  # CLOB
    ]
    
    def __init__(self, config):
        """初始化连接和配置"""
        self.config = config
        self.tunnel = None
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 初始化连接: {self.config['database']['conn_str']}")
        # 检查是否需要SSH隧道
        if 'ssh_id' in self.config['database'] and self.config['database']['ssh_id']:
            ssh_id = self.config['database']['ssh_id']
            if ssh_id in self.config['sshs']:
                ssh_config = self.config['sshs'][ssh_id]
                
                # 解析数据库连接字符串
                conn_str = self.config['database']['conn_str']
                db_url = urlparse(conn_str)
                db_host = None
                db_port = None
                
                # 从连接字符串中提取主机和端口
                if db_url.netloc:
                    # 处理形如 user:pass@host:port 的连接字符串
                    netloc_parts = db_url.netloc.split('@')
                    if len(netloc_parts) > 1:
                        host_port = netloc_parts[1].split(':')
                        db_host = host_port[0]
                        if len(host_port) > 1:
                            db_port = int(host_port[1])
                elif 'HOST' in conn_str.upper():
                    # 处理 Oracle 风格的连接字符串
                    host_match = re.search(r'HOST=([^)]+)', conn_str, re.IGNORECASE)
                    port_match = re.search(r'PORT=(\d+)', conn_str, re.IGNORECASE)
                    if host_match:
                        db_host = host_match.group(1)
                    if port_match:
                        db_port = int(port_match.group(1))
                
                if db_host and db_port:
                    print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 正在创建SSH隧道连接到 {ssh_config['host']}:{ssh_config['port']}")
                    print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 远程数据库地址: {db_host}:{db_port}")
                    
                    # 测试 SSH 连接是否可用
                    print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 测试 SSH 连接...")
                    try:
                        import paramiko
                        ssh = paramiko.SSHClient()
                        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                        ssh.connect(
                            hostname=ssh_config['host'],
                            port=int(ssh_config['port']),
                            username=ssh_config['username'],
                            password=ssh_config['password'],
                            timeout=10
                        )
                        ssh.close()
                        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} SSH 连接测试成功")
                    except Exception as e:
                        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} SSH 连接测试失败: {str(e)}")
                        # 如果基本的 SSH 连接都失败，就不要尝试创建隧道了
                        raise ValueError(f"无法连接到 SSH 服务器: {str(e)}")
                    
                    # 设置SSH连接选项
                    ssh_pkey = None
                    ssh_private_key_password = None
                    
                    # 检查是否有SSH私钥配置
                    if 'private_key' in ssh_config:
                        from paramiko import RSAKey
                        from io import StringIO
                        
                        ssh_pkey = RSAKey.from_private_key(StringIO(ssh_config['private_key']))
                        if 'private_key_password' in ssh_config:
                            ssh_private_key_password = ssh_config['private_key_password']
                    
                    # 设置SSH连接参数
                    ssh_kwargs = {
                        'ssh_username': ssh_config['username'],
                        'ssh_password': ssh_config['password'],
                        'remote_bind_address': (db_host, db_port),
                        'local_bind_address': ('127.0.0.1', 0),  # 使用随机本地端口
                    }
                    
                    # 如果有SSH私钥，添加到参数中
                    if ssh_pkey:
                        ssh_kwargs['ssh_pkey'] = ssh_pkey
                        if ssh_private_key_password:
                            ssh_kwargs['ssh_private_key_password'] = ssh_private_key_password
                    
                    # 尝试连接，最多重试3次
                    max_retries = 3
                    retry_count = 0
                    last_error = None
                    
                    while retry_count < max_retries:
                        try:
                            # 设置日志级别
                            import logging
                            logging.basicConfig(level=logging.INFO)
                            sshtunnel.DEFAULT_LOGLEVEL = logging.INFO
                            
                            # 创建SSH隧道
                            self.tunnel = sshtunnel.SSHTunnelForwarder(
                                (ssh_config['host'], int(ssh_config['port'])),
                                **ssh_kwargs
                            )
                            self.tunnel.start()
                            
                            # 修改连接字符串，使用本地隧道端口
                            local_port = self.tunnel.local_bind_port
                            
                            # 根据不同类型的连接字符串进行替换
                            if db_url.netloc:
                                # 替换标准连接字符串中的主机和端口
                                old_host_port = f"{db_host}:{db_port}"
                                new_host_port = f"127.0.0.1:{local_port}"
                                conn_str = conn_str.replace(old_host_port, new_host_port)
                            elif 'HOST' in conn_str.upper():
                                # 替换 Oracle 风格连接字符串中的主机和端口
                                conn_str = re.sub(r'HOST=[^)]+', f'HOST=127.0.0.1', conn_str, flags=re.IGNORECASE)
                                conn_str = re.sub(r'PORT=\d+', f'PORT={local_port}', conn_str, flags=re.IGNORECASE)
                            
                            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} SSH隧道已创建，本地端口: {local_port}")
                            self.config['database']['conn_str'] = conn_str
                            break  # 成功创建隧道，跳出循环
                            
                        except Exception as e:
                            last_error = e
                            retry_count += 1
                            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} SSH隧道创建失败 (尝试 {retry_count}/{max_retries}): {str(e)}")
                            
                            # 如果隧道已创建但出错，确保关闭
                            if hasattr(self, 'tunnel') and self.tunnel:
                                try:
                                    self.tunnel.stop()
                                except:
                                    pass
                                self.tunnel = None
                            
                            # 等待一段时间再重试
                            import time
                            time.sleep(2)
                    
                    # 如果所有重试都失败
                    if retry_count >= max_retries:
                        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 在 {max_retries} 次尝试后无法创建SSH隧道")
                        raise ValueError(f"无法创建SSH隧道: {str(last_error)}")
                else:
                    print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 无法从连接字符串中提取主机和端口信息")
                    print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 连接字符串: {conn_str}")
                    raise ValueError("无法从连接字符串中提取主机和端口信息")
        
        # 创建数据库连接
        try:
            self.engine = create_engine(self.config['database']['conn_str'])
            self.conn = self.engine.connect()
        except Exception as e:
            # 如果数据库连接失败，确保关闭SSH隧道
            if hasattr(self, 'tunnel') and self.tunnel:
                self.tunnel.stop()
                print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} SSH隧道已关闭（因数据库连接失败）")
            raise ValueError(f"无法连接到数据库: {str(e)}")
        
    def close(self):
        """关闭连接"""
        if hasattr(self, 'conn'):
            self.conn.close()
        if hasattr(self, 'engine'):
            self.engine.dispose()
        if hasattr(self, 'tunnel') and self.tunnel:
            self.tunnel.stop()
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} SSH隧道已关闭")

    @abstractmethod
    def get_dict_script(self, schema, table):
        """获取数据字典SQL"""
        pass
        
    @abstractmethod
    def get_tables_script(self, schema):
        """获取指定库下所有表的SQL"""
        pass
        
    @abstractmethod
    def get_sample_script(self, schema, table, condition):
        """获取样例数据SQL"""
        pass

    @abstractmethod
    def _get_limit_clause(self):
        """获取limit子句"""
        pass
        
    def get_stats_script(self, schema, table, column, indicators, condition):
        """获取统计数据SQL"""
        script = ''
        tbl_name = f"{schema}.{table}"
        
        for i in range(1, len(indicators)):
            if indicators[i] == 1:
                script += self.script_list[i][2] 
            else:
                script += self.script_list[i][1]
                
        script = script.replace('TBL_NAME', tbl_name)
        script = script.replace('COL_NAME', column)
        
        if condition:
            script += condition
            
        return script
        
    def get_freq_script(self, schema, table, column, condition):
        """获取字段值TOP10频率统计SQL"""
        tbl_name = f"{schema}.{table}"
        script = f"SELECT {column} AS col_name, COUNT(1) AS cnt FROM {tbl_name}"
        
        if condition:
            script += f" {condition}"
            
        script += f" GROUP BY {column} ORDER BY COUNT(1) DESC {self._get_limit_clause(10)}"
        return script

    def _get_limit_clause(self, limit=5):
        """获取limit子句，默认为5"""
        return f"LIMIT {limit}"

    def execute_script(self, script):
        """执行SQL并返回DataFrame"""
        try:
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 执行脚本: {script}")
            data = pd.read_sql(script, self.conn)
            return pd.DataFrame(data, dtype=np.str_)
        except Exception as e:
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 执行异常: {repr(e)}")
            return pd.DataFrame()

    def pre_col_ind(self, col_list, ind_list):
        """准备字段统计标示：根据字段数据类型自动生成字段级数据统计标示"""
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 准备字段统计标示")
        for i in range(len(col_list)):
            data_type = col_list[i][1].lower()
            if re.findall(r'str|char', data_type, re.I):
                ind_list[i] = cp.copy(self.dft_ind_list[1])
            elif re.findall(r'clob|text', data_type, re.I):
                ind_list[i] = cp.copy(self.dft_ind_list[4])    
            elif re.findall(r'int|dec|long|float|num', data_type, re.I):
                ind_list[i] = cp.copy(self.dft_ind_list[2])
            elif re.findall(r'time|date', data_type, re.I):
                ind_list[i] = cp.copy(self.dft_ind_list[3])
            else:
                ind_list[i] = cp.copy(self.dft_ind_list[0])
        return ind_list

# 具体数据库实现类
class HiveExplorer(DataExplorer):
    """Hive数据探查实现"""
    
    script_list = [
        ['指标名称', '留空脚本', '取数脚本'],
        ['行数',      'SELECT \'-\' as COL01, ', 'SELECT COUNT(*) as COL01, '],
        ['空值数量',   ' \'-\' as COL02, ', ' COUNT(CASE WHEN COL_NAME IS NULL THEN 1 END) AS COL02, '],
        ['空白数量',   ' \'-\' as COL03, ', ' COUNT(CASE WHEN TRIM(COL_NAME) = \'\' THEN 1 END) AS COL03, '],
        ['不同值数量', ' \'-\' as COL04, ', ' COUNT(DISTINCT COL_NAME) AS COL04, '],
        ['唯一值数量', ' \'-\' as COL05, ', ' \'-\' AS COL05, '],
        ['重复值数量', ' \'-\' as COL06, ', ' \'-\' AS COL06, '],
        ['最小长度',   ' \'-\' as COL07, ', ' MIN(LENGTH(COL_NAME)) AS COL07, '],
        ['最大长度',   ' \'-\' as COL08, ', ' MAX(LENGTH(COL_NAME)) AS COL08, '],
        ['平均长度',   ' \'-\' as COL09, ', ' AVG(LENGTH(COL_NAME)) AS COL09, '],
        ['最小值',    ' \'-\' as COL10, ', ' MIN(COL_NAME) AS COL10, '],
        ['最大值',    ' \'-\' as COL11, ', ' MAX(COL_NAME) AS COL11, '],
        ['平均值',    ' \'-\' as COL12  FROM TBL_NAME ', ' AVG(COL_NAME) AS COL12 FROM TBL_NAME ']
    ]
    
    def get_dict_script(self, schema, table):
        return f"""
        DESCRIBE {schema}.{table}
        """
        
    def get_tables_script(self, schema):
        return f"""
        SHOW TABLES IN {schema}
        """
        
    def _get_limit_clause(self, limit=5):
        return f"limit {limit}"

    def get_sample_script(self, schema, table, condition):
        """获取样例数据SQL"""
        tbl_name = f"{schema}.{table}"
        if condition is None or condition.strip() == '':
            condition = ''
        return f"SELECT * FROM {tbl_name} {condition} {self._get_limit_clause()}"

class MySQLExplorer(DataExplorer):
    """MySQL数据探查实现"""
    
    script_list = [
        ['指标名称', '留空脚本', '取数脚本'],
        ['行数',      'SELECT \'-\' as COL01, ', 'SELECT COUNT(*) as COL01, '],
        ['空值数量',   ' \'-\' as COL02, ', ' SUM(CASE WHEN COL_NAME IS NULL THEN 1 ELSE 0 END) AS COL02, '],
        ['空白数量',   ' \'-\' as COL03, ', ' SUM(CASE WHEN TRIM(COL_NAME) = \'\' THEN 1 ELSE 0 END) AS COL03, '],
        ['不同值数量', ' \'-\' as COL04, ', ' COUNT(DISTINCT COL_NAME) AS COL04, '],
        ['唯一值数量', ' \'-\' as COL05, ', ' \'-\' AS COL05, '],
        ['重复值数量', ' \'-\' as COL06, ', ' \'-\' AS COL06, '],
        ['最小长度',   ' \'-\' as COL07, ', ' MIN(CHAR_LENGTH(COL_NAME)) AS COL07, '],
        ['最大长度',   ' \'-\' as COL08, ', ' MAX(CHAR_LENGTH(COL_NAME)) AS COL08, '],
        ['平均长度',   ' \'-\' as COL09, ', ' AVG(CHAR_LENGTH(COL_NAME)) AS COL09, '],
        ['最小值',    ' \'-\' as COL10, ', ' MIN(COL_NAME) AS COL10, '],
        ['最大值',    ' \'-\' as COL11, ', ' MAX(COL_NAME) AS COL11, '],
        ['平均值',    ' \'-\' as COL12  FROM TBL_NAME ', ' AVG(COL_NAME) AS COL12 FROM TBL_NAME ']
    ]
    
    def get_dict_script(self, schema, table):
        return f"""
        select COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_COMMENT,
        (CASE WHEN COLUMN_KEY = 'PRI' THEN 'YES' ELSE 'NO' END) as IS_PRIMARY_KEY,
        ORDINAL_POSITION as FIELD_ORDER
        from information_schema.columns 
        where table_schema = '{schema}'
        and TABLE_NAME = '{table}'
        order by ORDINAL_POSITION asc
        """
        
    def get_tables_script(self, schema):
        return f"""
        SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.tables 
        WHERE TABLE_SCHEMA = '{schema}'
        """
        
    def _get_limit_clause(self, limit=5):
        return f"limit {limit}"

    def get_sample_script(self, schema, table, condition):
        """获取样例数据SQL"""
        tbl_name = f"{schema}.{table}"
        if condition is None or condition.strip() == '':
            condition = ''
        return f"SELECT * FROM {tbl_name} {condition} {self._get_limit_clause()}"

class OracleExplorer(DataExplorer):
    """Oracle数据探查实现"""
    
    script_list = [
        ['指标名称', '留空脚本', '取数脚本'],
        ['行数',      'SELECT \'-\' as COL01, ', 'SELECT COUNT(*) as COL01, '],
        ['空值数量',   ' \'-\' as COL02, ', ' SUM(CASE WHEN COL_NAME IS NULL THEN 1 ELSE 0 END) AS COL02, '],
        ['空白数量',   ' \'-\' as COL03, ', ' SUM(CASE WHEN TRIM(COL_NAME) = \'\' THEN 1 ELSE 0 END) AS COL03, '],
        ['不同值数量', ' \'-\' as COL04, ', ' COUNT(DISTINCT COL_NAME) AS COL04, '],
        ['唯一值数量', ' \'-\' as COL05, ', ' \'-\' AS COL05, '],
        ['重复值数量', ' \'-\' as COL06, ', ' \'-\' AS COL06, '],
        ['最小长度',   ' \'-\' as COL07, ', ' MIN(LENGTH(COL_NAME)) AS COL07, '],
        ['最大长度',   ' \'-\' as COL08, ', ' MAX(LENGTH(COL_NAME)) AS COL08, '],
        ['平均长度',   ' \'-\' as COL09, ', ' AVG(LENGTH(COL_NAME)) AS COL09, '],
        ['最小值',    ' \'-\' as COL10, ', ' MIN(COL_NAME) AS COL10, '],
        ['最大值',    ' \'-\' as COL11, ', ' MAX(COL_NAME) AS COL11, '],
        ['平均值',    ' \'-\' as COL12  FROM TBL_NAME ', ' AVG(COL_NAME) AS COL12 FROM TBL_NAME ']
    ]
    
    def get_dict_script(self, schema, table):
        return f"""
        SELECT t.column_name, t.data_type, 
               t.nullable, B.comments,
               CASE WHEN p.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END AS is_primary_key,
               t.column_id as field_order
        FROM all_tab_columns t
        LEFT JOIN (
            SELECT c.column_name
            FROM all_cons_columns c
            JOIN all_constraints cons ON c.constraint_name = cons.constraint_name
            WHERE cons.table_name = '{table.upper()}'
            AND cons.owner = '{schema.upper()}'
            AND cons.constraint_type = 'P'
        ) p ON t.column_name = p.column_name
        LEFT JOIN ALL_COL_COMMENTS B 
          ON T.OWNER = B.OWNER 
         AND T.TABLE_NAME = B.TABLE_NAME 
         AND T.COLUMN_NAME = B.COLUMN_NAME         
        WHERE t.table_name = '{table.upper()}'
        AND t.owner = '{schema.upper()}'
        ORDER BY t.column_id
        """        
    def get_tables_script(self, schema):
        return f"""
        SELECT TABLE_NAME, COMMENTS FROM all_tab_comments 
        WHERE OWNER = '{schema.upper()}'
        AND TABLE_TYPE = 'TABLE'
        """
        
    def _get_limit_clause(self, limit=5):
        return f"WHERE ROWNUM <= {limit}"
    
    def get_freq_script(self, schema, table, column, condition):
        """获取字段值TOP10频率统计SQL - Oracle特定实现"""
        tbl_name = f"{schema}.{table}"
        script = f"""
        SELECT * FROM (
            SELECT {column} AS col_name, COUNT(1) AS cnt 
            FROM {tbl_name}
            {condition if condition else ''}
            GROUP BY {column} 
            ORDER BY COUNT(1) DESC
        ) WHERE ROWNUM <= 10
        """
        return script

    def get_sample_script(self, schema, table, condition):
        """获取样例数据SQL"""
        tbl_name = f"{schema}.{table}"
        if condition is None or condition.strip() == '':
            condition = ''
        return f"SELECT * FROM {tbl_name} {condition} {self._get_limit_clause()}"

class SQLServerExplorer(DataExplorer):
    """SQLServer数据探查实现"""
    
    script_list=[
        ['指标名称', '留空脚本', '取数脚本']
        ,['行数', 		'SELECT \'-\' as COL01, ', 'SELECT SUM(1) as COL01, ']
        ,['空值数量', 	' \'-\' as COL02, ', ' SUM(CASE WHEN COL_NAME IS NULL THEN 1 ELSE 0 END) AS COL02, ']
        ,['空白数量', 	' \'-\' as COL03, ', ' SUM(CASE WHEN COL_NAME = \'\' THEN 1 ELSE 0 END) AS COL03, ']
        ,['不同值数量', ' \'-\' as COL04, ', ' COUNT(DISTINCT COL_NAME) AS COL04, ']
        ,['唯一值数量', ' \'-\' as COL05, ', ' \'-\' as COL05, ']
        ,['重复值数量', ' \'-\' as COL06, ', ' \'-\' as COL06, ']
        ,['最小长度', 	' \'-\' as COL07, ', ' MIN(LEN(COL_NAME)) AS COL07, ']
        ,['最大长度', 	' \'-\' as COL08, ', ' MAX(LEN(COL_NAME)) AS COL08, ']
        ,['平均长度', 	' \'-\' as COL09, ', ' AVG(LEN(COL_NAME)) AS COL09, ']
        ,['最小值', 	' \'-\' as COL10, ', ' MIN(COL_NAME) AS COL10, ']
        ,['最大值', 	' \'-\' as COL11, ', ' MAX(COL_NAME) AS COL11, ']
        ,['平均值', 	' \'-\' as COL12  FROM TBL_NAME ', ' AVG(COL_NAME) AS COL12 FROM TBL_NAME ']]
        
    def get_dict_script(self, schema, table):
        return f"""
        SELECT 
            c.name AS column_name,
            t.name AS data_type,
            CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS nullable,
            ep.value AS description,
            CASE WHEN pk.column_id IS NOT NULL THEN 'YES' ELSE 'NO' END AS is_primary_key,
            c.column_id as field_order
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        LEFT JOIN sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description'
        LEFT JOIN (
            SELECT ic.column_id, ic.object_id
            FROM sys.index_columns ic
            JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.key_constraints k ON i.object_id = k.parent_object_id AND i.index_id = k.unique_index_id
            WHERE k.type = 'PK'
        ) pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id
        WHERE OBJECT_NAME(c.object_id) = '{table}'
        AND SCHEMA_NAME(SCHEMA_ID()) = '{schema}'
        ORDER BY c.column_id
        """
        
    def get_tables_script(self, schema):
        return f"""
        SELECT name as TABLE_NAME, 
               ISNULL((SELECT value FROM sys.extended_properties 
                      WHERE class = 1 AND major_id = OBJECT_ID('{schema}.' + name) AND minor_id = 0), '') as TABLE_COMMENT 
        FROM sys.tables 
        WHERE SCHEMA_NAME(schema_id) = '{schema}'
        """
        
    def _get_limit_clause(self, limit=5):
        return f"TOP {limit}"
    
    def get_freq_script(self, schema, table, column, condition):
        """获取字段值TOP10频率统计SQL - SQLServer特定实现"""
        tbl_name = f"{schema}.{table}"
        script = f"SELECT {self._get_limit_clause(10)} {column} AS col_name, COUNT(1) AS cnt FROM {tbl_name}"
        
        if condition:
            script += f" {condition}"
            
        script += f" GROUP BY {column} ORDER BY COUNT(1) DESC"
        return script

    def get_sample_script(self, schema, table, condition):
        """获取样例数据SQL"""
        tbl_name = f"{schema}.{table}"
        if condition is None or condition.strip() == '':
            condition = ''
        return f"SELECT {self._get_limit_clause()} * FROM {tbl_name} {condition}"

class HiveExplorer(DataExplorer):
    """Hive数据探查实现"""
    
    script_list = [
        ['指标名称', '留空脚本', '取数脚本'],
        ['行数',      'SELECT \'-\' as COL01, ', 'SELECT COUNT(*) as COL01, '],
        ['空值数量',   ' \'-\' as COL02, ', ' COUNT(CASE WHEN COL_NAME IS NULL THEN 1 END) AS COL02, '],
        ['空白数量',   ' \'-\' as COL03, ', ' COUNT(CASE WHEN TRIM(COL_NAME) = \'\' THEN 1 END) AS COL03, '],
        ['不同值数量', ' \'-\' as COL04, ', ' COUNT(DISTINCT COL_NAME) AS COL04, '],
        ['唯一值数量', ' \'-\' as COL05, ', ' \'-\' AS COL05, '],
        ['重复值数量', ' \'-\' as COL06, ', ' \'-\' AS COL06, '],
        ['最小长度',   ' \'-\' as COL07, ', ' MIN(LENGTH(COL_NAME)) AS COL07, '],
        ['最大长度',   ' \'-\' as COL08, ', ' MAX(LENGTH(COL_NAME)) AS COL08, '],
        ['平均长度',   ' \'-\' as COL09, ', ' AVG(LENGTH(COL_NAME)) AS COL09, '],
        ['最小值',    ' \'-\' as COL10, ', ' MIN(COL_NAME) AS COL10, '],
        ['最大值',    ' \'-\' as COL11, ', ' MAX(COL_NAME) AS COL11, '],
        ['平均值',    ' \'-\' as COL12  FROM TBL_NAME ', ' AVG(COL_NAME) AS COL12 FROM TBL_NAME ']
    ]
    
    def get_dict_script(self, schema, table):
        # 尝试使用 DESCRIBE 命令获取表结构，这是 Hive 特有的方式
        return f"""
        DESCRIBE {schema}.{table}
        """
        
    def _parse_describe_formatted(self, df):
        """解析 DESCRIBE 命令的输出结果
        Args:
            df (DataFrame): DESCRIBE 命令的输出结果
        Returns:
            DataFrame: 标准化后的字段信息
        """
        if df.empty:
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} DESCRIBE 命令返回空结果")
            return pd.DataFrame()
            
        # 打印原始输出以便调试
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} DESCRIBE 命令原始输出：")
        print(df.head())
        
        # 初始化结果列表
        result = []
        field_order = 1
        
        # 检查列名，适应不同的输出格式
        if len(df.columns) >= 2:
            # 标准的 DESCRIBE 输出通常有三列：col_name, data_type, comment
            for _, row in df.iterrows():
                if len(row) >= 2:
                    # 获取基本字段信息
                    field_name = str(row.iloc[0]).strip() if not pd.isna(row.iloc[0]) else ""
                    field_type = str(row.iloc[1]).strip() if not pd.isna(row.iloc[1]) else ""
                    
                    # 获取注释（如果有）
                    field_comment = ""
                    if len(row) > 2 and not pd.isna(row.iloc[2]):
                        field_comment = str(row.iloc[2]).strip()
                    
                    # 跳过空行或特殊行
                    if field_name == "" or field_name.startswith("#"):
                        continue
                        
                    # 判断是否为主键（Hive不支持主键约束，默认为NO）
                    is_primary_key = "NO"
                    # 判断是否可为空（Hive默认所有字段可为空）
                    is_nullable = "YES"
                    
                    # 添加到结果列表
                    result.append([field_name, field_type, is_nullable, field_comment, is_primary_key, str(field_order)])
                    field_order += 1
        
        # 打印解析后的结果
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 解析到 {len(result)} 个字段")
        
        # 转换为DataFrame
        if result:
            return pd.DataFrame(result, columns=["COLUMN_NAME", "TYPE_NAME", "IS_NULLABLE", "REMARKS", "IS_PRIMARY_KEY", "ORDINAL_POSITION"])
        else:
            return pd.DataFrame()
        
    def get_tables_script(self, schema):
        return f"""
        SHOW TABLES IN {schema}
        """
        
    def _get_limit_clause(self, limit=5):
        return f"limit {limit}"

    def get_sample_script(self, schema, table, condition):
        """获取样例数据SQL"""
        tbl_name = f"{schema}.{table}"
        if condition is None or condition.strip() == '':
            condition = ''
        return f"SELECT * FROM {tbl_name} {condition} {self._get_limit_clause()}"

# ... 其他数据库实现类 ...

class DataExpApp:
    """数据探查应用类"""
    
    # 统计列名映射
    STAT_COLUMN_NAMES = {
        0: "统计标示",
        1: "行数标示",
        2: "空值数量标示",
        3: "空白数量标示",
        4: "不同值数量标示",
        5: "唯一值数量标示",
        6: "重复值数量标示",
        7: "最小长度标示",
        8: "最大长度标示",
        9: "平均长度标示",
        10: "最小值标示",
        11: "最大值标示",
        12: "平均值标示"
    }
    
    def __init__(self, config_file, conn_id=None, output_file=None):
        """初始化应用
        Args:
            config_file (str): 配置文件路径
            conn_id (str, optional): 连接ID. 默认None，使用配置文件中的默认连接
            output_file (str, optional): 输出Excel文件路径. 默认None，使用配置文件中的默认路径
        """
        # 修改配置文件路径处理
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, config_file)
        self._load_config(config_path, conn_id, output_file)
        self._init_excel()
        # 存储已连接的数据库探查器
        self.explorers = {}
        # 存储字段清单数据
        self.col_df = None
        # 存储表清单数据
        self.tbl_df = None
        
    def _load_config(self, config_file, conn_id=None, output_file=None):
        """加载配置
        Args:
            config_file (str): 配置文件路径
            conn_id (str, optional): 连接ID
            output_file (str, optional): 输出文件路径或输出ID
        """
        try:
            with open(config_file, encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            
            # 处理输出文件路径
            if output_file:
                if output_file in self.config['outputs']:
                    # 如果传入的是输出ID
                    self.output_path = self.config['outputs'][output_file]['path']
                else:
                    # 如果传入的是直接的文件路径
                    self.output_path = output_file
            else:
                # 使用默认输出ID
                default_output_id = self.config.get('default_output_id')
                if not default_output_id:
                    raise ValueError("未指定输出文件且配置文件中未找到默认输出ID")
                
                if default_output_id not in self.config['outputs']:
                    raise ValueError(f"在配置文件中未找到输出ID '{default_output_id}' 的配置")
                
                self.output_path = self.config['outputs'][default_output_id]['path']
            
            # 处理连接ID
            self.conn_id = conn_id or self.config.get('default_conn_id')
            if not self.conn_id:
                raise ValueError("未指定连接ID且配置文件中未找到默认连接ID")
            
            if 'connections' not in self.config or self.conn_id not in self.config['connections']:
                raise ValueError(f"在配置文件中未找到连接ID '{self.conn_id}' 的配置")
            
            # 确保 sshs 配置存在
            if 'sshs' not in self.config:
                self.config['sshs'] = {}
                
        except FileNotFoundError:
            raise ValueError(f"配置文件未找到: {config_file}")
        except yaml.YAMLError as e:
            raise ValueError(f"配置文件格式错误: {str(e)}")
        except Exception as e:
            raise ValueError(f"加载配置文件时出错: {str(e)}")

    def _init_excel(self):
        """初始化Excel"""
        self.wb = self._open_workbook()
        self.tbl_sht = self.wb.sheets["表清单"]
        self.col_sht = self.wb.sheets["字段清单"]
        
    def _load_data_from_excel(self):
        """从Excel加载数据到DataFrame"""
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 从Excel加载数据")
        
        # 加载表清单数据
        tbl_row = self.tbl_sht.range('A1').end('down').row
        tbl_data = self.tbl_sht.range((2, 1), (tbl_row, 10)).value
        tbl_headers = self.tbl_sht.range((1, 1), (1, 10)).value
        
        # 检查数据结构，确保数据形状与列标题匹配
        if isinstance(tbl_data, list):
            # 如果只有一行数据，xlwings可能返回一维列表而不是二维列表
            if not isinstance(tbl_data[0], list) and not isinstance(tbl_data[0], tuple):
                tbl_data = [tbl_data]  # 转换为二维列表
                
        # 检查数据是否为嵌套列表，如果不是，则进行转换
        if tbl_data and not isinstance(tbl_data[0], (list, tuple)):
            # 单列数据，需要转换为二维列表
            tbl_data = [[item] for item in tbl_data]
            
        # 确保每行数据的列数与表头列数匹配
        header_count = len(tbl_headers) if isinstance(tbl_headers, (list, tuple)) else 1
        normalized_data = []
        for row in tbl_data:
            if isinstance(row, (list, tuple)):
                # 如果行数据列数少于表头列数，用None填充
                if len(row) < header_count:
                    normalized_data.append(list(row) + [None] * (header_count - len(row)))
                else:
                    normalized_data.append(row)
            else:
                # 单个值，扩展为与表头同样长度的列表
                normalized_data.append([row] + [None] * (header_count - 1))
                
        self.tbl_df = pd.DataFrame(normalized_data, columns=tbl_headers)
        
        # 加载字段清单数据
        col_row = self.col_sht.used_range.last_cell.row
        
        # 先获取表头信息
        col_headers = []
        for i in range(1, 66):  # 根据表头列数调整
            header = self.col_sht.range((2, i)).value
            if header:
                col_headers.append(header)
            else:
                break

        if col_row < 3:  # 如果没有数据，只有表头
            # 创建空的DataFrame，使用表头
            self.col_df = pd.DataFrame(columns=col_headers)
        else:
            # 确保读取的数据列数与表头列数一致
            col_data = self.col_sht.range((3, 1), (col_row, len(col_headers))).value
            
            # 如果只有一行数据，需要特殊处理
            if isinstance(col_data[0], list):
                self.col_df = pd.DataFrame(col_data, columns=col_headers)
            else:
                # 单行数据转换为二维列表
                self.col_df = pd.DataFrame([col_data], columns=col_headers)
        
        # 创建表ID和字段ID
        self._create_ids()
        
    def _create_ids(self):
        """创建表ID和字段ID"""
        # 为表清单创建唯一表ID
        if not self.tbl_df.empty:
            self.tbl_df['表ID'] = self.tbl_df.apply(
                lambda row: f"{row['连接id']}_{row['库名']}_{row['表名']}", axis=1
            )
        
        # 为字段清单创建唯一表ID和字段ID
        if not self.col_df.empty and '连接id' in self.col_df.columns and '库名' in self.col_df.columns and '表名' in self.col_df.columns and '字段名' in self.col_df.columns:
            self.col_df['表ID'] = self.col_df.apply(
                lambda row: f"{row['连接id']}_{row['库名']}_{row['表名']}", axis=1
            )
            self.col_df['字段ID'] = self.col_df.apply(
                lambda row: f"{row['表ID']}_{row['字段名']}", axis=1
            )
        
    def _save_data_to_excel(self):
        """将DataFrame数据保存回Excel"""
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 保存数据到Excel")
        
        # 保存表清单数据
        if not self.tbl_df.empty:
            save_df = self.tbl_df.copy()
            # 表ID转化为小写
            save_df['表ID'] = save_df['表ID'].str.lower()
            # 删除'表ID'重复的记录
            save_df = save_df.drop_duplicates(subset=['表ID'], keep='first')
            
            # 移除辅助列
            if '表ID' in save_df.columns:
                save_df = save_df.drop(columns=['表ID'])
                
            # 先保存数据
            self.tbl_sht.range('A2').value = save_df.values
            
            # 然后单独设置链接公式
            if '链接' in self.tbl_sht.range('A1').expand('right').value:
                link_col_index = self.tbl_sht.range('A1').expand('right').value.index('链接') + 1
                last_row = self.tbl_sht.range('A1').end('down').row
                
                # 获取公式范围
                formula_range = self.tbl_sht.range((2, link_col_index), (last_row, link_col_index))
                
                # 设置公式
                formula = '=IFNA(HYPERLINK(CONCATENATE("#字段清单!C",MATCH([@表名],字段清单!C:C,0)),"查看"),"待获取")'
                formula_range.formula = formula
        
        # 保存字段清单数据
        if not self.col_df.empty:
            # 移除辅助列
            save_df = self.col_df.copy()

            # 删除表名为空的记录
            if '表名' in save_df.columns:
                save_df = save_df[save_df['表名'].notna() & (save_df['表名'] != '')]
            
            if '表ID' in save_df.columns:
                save_df = save_df.drop(columns=['表ID'])
            if '字段ID' in save_df.columns:
                save_df = save_df.drop(columns=['字段ID'])
            
            # 保存数据
            self.col_sht.range('A3').value = save_df.values
        
        # 保存工作簿
        self.wb.save()
        
    def _get_explorer(self, conn_id):
        """获取数据探查器，如果已存在则复用，否则创建新的
        Args:
            conn_id (str): 连接ID
        Returns:
            DataExplorer: 数据探查器实例
        """
        # 如果已经创建过该连接ID的探查器，则直接返回
        if conn_id in self.explorers:
            return self.explorers[conn_id]
            
        # 检查连接ID是否存在于配置中
        if 'connections' not in self.config or conn_id not in self.config['connections']:
            raise ValueError(f"在配置文件中未找到连接ID '{conn_id}' 的配置")
            
        # 获取连接配置
        conn_config = self.config['connections'][conn_id]
        db_type = conn_config['type'].lower()  # 转换为小写
        
        # 创建数据库配置
        db_config = {'database': conn_config.copy()}  # 使用复制，避免修改原始配置
        
        # 添加 SSH 隧道配置
        if 'ssh_id' in conn_config and conn_config['ssh_id'] and 'sshs' in self.config:
            ssh_id = conn_config['ssh_id']
            if ssh_id in self.config['sshs']:
                db_config['sshs'] = self.config['sshs']
                print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 使用SSH隧道连接: {ssh_id}")
            else:
                print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 警告: SSH隧道ID '{ssh_id}' 在配置中不存在")
        
        explorers = {
            'hive': HiveExplorer,
            'mysql': MySQLExplorer,
            'oracle': OracleExplorer,
            'sqlserver': SQLServerExplorer,
            'impala': HiveExplorer  # 添加 impala 类型，使用 HiveExplorer
        }
        
        if db_type not in explorers:
            raise ValueError(f"不支持的数据库类型: {db_type}")
            
        # 创建并保存探查器实例
        explorer = explorers[db_type](db_config)
        self.explorers[conn_id] = explorer
        return explorer

    def _open_workbook(self):
        """打开Excel工作簿"""
        Apps = xw.apps
        if Apps.count:
            app = Apps.active
        else:
            app = xw.App(visible=True, add_book=False)
        return app.books.open(self.output_path)

    def run(self):
        """运行数据探查"""
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 开始数据探查")
        
        try:
            # 从Excel加载数据
            self._load_data_from_excel()
            
            # 处理表清单
            self._process_tables()
            
            # 保存数据回Excel
            self._save_data_to_excel()
        finally:
            # 关闭所有已连接的探查器
            for explorer in self.explorers.values():
                explorer.close()
            self.wb.save()
            
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 结束数据探查")

    def _process_tables(self):
        """处理表清单"""
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 开始处理表清单")
        
        if self.tbl_df.empty:
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 表清单为空，跳过处理")
            return
        
        # 创建一个新的DataFrame来存储需要添加的表
        new_tables = []
        
        # 遍历表清单
        for idx, row in self.tbl_df.iterrows():
            conn_id = row['连接id']
            schema = row['库名']
            table = row['表名']
            condition = row['筛选条件']
            table_id = row['表ID']
            
            # 获取数据库探查器
            try:
                # 检查连接ID是否存在于配置中
                if 'connections' not in self.config or conn_id not in self.config['connections']:
                    print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 跳过处理：在配置文件中未找到连接ID '{conn_id}' 的配置")
                    continue
                
                explorer = self._get_explorer(conn_id)
                
                # 处理表名为 * 的情况，获取该库下所有表
                if table == '*' and self.tbl_df.loc[idx, '数据字典'] == 1:
                    print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 发现通配符表名 '*'，获取 {conn_id}.{schema} 下的所有表")
                    try:
                        # 获取所有表
                        script = explorer.get_tables_script(schema)
                        tables_df = explorer.execute_script(script)
                        
                        if not tables_df.empty:
                            # 遍历所有表，添加到新表列表
                            for _, table_row in tables_df.iterrows():
                                table_name = table_row.iloc[0]  # 表名
                                table_comment = table_row.iloc[1] if len(table_row) > 1 else ''  # 表描述
                                
                                # 创建新表ID
                                new_table_id = f"{conn_id}_{schema}_{table_name}"
                                
                                # 复制当前行的其他信息
                                new_table = row.copy()
                                new_table['表名'] = table_name
                                new_table['表描述'] = table_comment
                                new_table['表ID'] = new_table_id
                                
                                # 添加到新表列表
                                new_tables.append(new_table)
                            
                            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 成功获取 {conn_id}.{schema} 下的 {len(tables_df)} 个表")
                            self.tbl_df.loc[idx, ['数据字典','示例数据','统计数据']] = 2
                        else:
                            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 未找到 {conn_id}.{schema} 下的表")
                            self.tbl_df.loc[idx, ['数据字典','示例数据','统计数据']] = 0
                    except Exception as e:
                        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 获取表列表异常: {str(e)}")
                        # 更新表清单处理状态为异常
                        self.tbl_df.loc[idx, ['数据字典','示例数据','统计数据']] = 4
                else:
                    # 正常处理单个表
                    # 获取数据字典
                    if self.tbl_df.loc[idx, '数据字典'] == 1:
                        self._process_dict(conn_id, schema, table, table_id, condition, explorer)
                        
                    # 获取示例数据    
                    if self.tbl_df.loc[idx, '示例数据'] == 1:
                        self._process_sample(conn_id, schema, table, table_id, condition, explorer)
                        
                    # 获取统计数据
                    if self.tbl_df.loc[idx, '统计数据'] in [1, 9]:
                        self._process_stats(conn_id, schema, table, table_id, condition, explorer, auto_generate=True)
                
            except Exception as e:
                print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 获取数据库探查器异常: {str(e)}")
                continue
        
        # 将新表添加到表清单
        if new_tables:
            new_tables_df = pd.DataFrame(new_tables)
            self.tbl_df = pd.concat([self.tbl_df, new_tables_df], ignore_index=True)
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 已添加 {len(new_tables)} 个新表到表清单")

    def _process_dict(self, conn_id, schema, table, table_id, condition, explorer):
        """处理数据字典
        Args:
            conn_id (str): 连接ID
            schema (str): 库名
            table (str): 表名
            table_id (str): 表ID
            condition (str): 筛选条件
            explorer (DataExplorer): 数据探查器
        """
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 处理数据字典: {table_id}")
        try:
            # 获取数据字典
            script = explorer.get_dict_script(schema, table)
            df_dict = explorer.execute_script(script)
            
            # 检查是否是Hive探查器，如果是，需要特殊处理DESCRIBE FORMATTED的输出
            if isinstance(explorer, HiveExplorer) and hasattr(explorer, '_parse_describe_formatted'):
                df_dict = explorer._parse_describe_formatted(df_dict)
            
            if not df_dict.empty:
                # 从现有DataFrame中删除该表的所有记录，确保数据字典始终是最新的
                if self.col_df is not None and not self.col_df.empty:
                    self.col_df = self.col_df[self.col_df['表ID'] != table_id]
                
                # 获取表描述，供后续使用
                table_desc = ''
                if not self.tbl_df.empty:
                    matching_rows = self.tbl_df[self.tbl_df['表ID'] == table_id]
                    if not matching_rows.empty and '表描述' in matching_rows.columns:
                        table_desc = matching_rows['表描述'].iloc[0]
                        if pd.isna(table_desc):  # 检查是否为NaN
                            table_desc = ''
                
                # 准备新的字段记录列表
                new_rows = []
                
                # 准备数据字典数据
                for i, row in df_dict.iterrows():
                    field_name = row.iloc[0]  # 字段名
                    field_type = row.iloc[1]  # 类型
                    nullable = row.iloc[2]    # 可空
                    description = row.iloc[3] # 字段描述
                    is_primary_key = row.iloc[4] if len(row) > 4 else 'NO'  # 是否主键
                    field_order = row.iloc[5] if len(row) > 5 else str(i+1)  # 字段顺序
                    
                    # 创建字段ID
                    field_id = f"{table_id}_{field_name}"
                    
                    # 添加新字段
                    new_row = {
                        '连接id': conn_id,
                        '库名': schema,
                        '表名': table,
                        '表描述': table_desc,
                        '字段顺序': field_order,
                        '字段名': field_name,
                        '类型': field_type,
                        '主键': is_primary_key,
                        '可空': nullable,
                        '字段描述': description,
                        '表ID': table_id,
                        '字段ID': field_id
                    }
                   # 添加到新行列表
                    new_rows.append(new_row)
                
                # 将新记录添加到DataFrame
                new_df = pd.DataFrame(new_rows)
                if self.col_df is None or self.col_df.empty:
                    self.col_df = new_df
                else:
                    self.col_df = pd.concat([self.col_df, new_df], ignore_index=True)
                # 更新表清单处理状态
                self.tbl_df.loc[self.tbl_df['表ID'] == table_id, '数据字典'] = 2
            else:
                print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 未找到表 {table_id} 的字段信息")
                self.tbl_df.loc[self.tbl_df['表ID'] == table_id, ['数据字典', '示例数据', '统计数据']] = 0
                    
        except Exception as e:
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 处理数据字典异常: {str(e)}")
            # 更新表清单处理状态为异常
            self.tbl_df.loc[self.tbl_df['表ID'] == table_id, '数据字典'] = 4
    
    def _process_sample(self, conn_id, schema, table, table_id, condition, explorer):
        """处理示例数据
        Args:
            conn_id (str): 连接ID
            schema (str): 库名
            table (str): 表名
            table_id (str): 表ID
            condition (str): 筛选条件
            explorer (DataExplorer): 数据探查器
        """
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 处理示例数据: {table_id}")
        try:
            # 获取示例数据
            script = explorer.get_sample_script(schema, table, condition)
            df_sample = explorer.execute_script(script)
            if not df_sample.empty:
                # 处理超长文本，将长度超过200的文本替换为空字符串
                for col in df_sample.columns:
                    df_sample[col] = df_sample[col].apply(lambda x: '忽略超长文本' if isinstance(x, str) and len(x) > 200 else x)
                
                # 获取该表的所有字段
                if not self.col_df.empty:
                    table_fields = self.col_df[self.col_df['表ID'] == table_id]
                    
                    # 处理示例数据
                    sample_data = df_sample.values.tolist()
                    sample_data_transposed = list(map(list, zip(*sample_data)))
                    # 更新示例数据到字段清单
                    for i, col_name in enumerate(df_sample.columns):
                        # 查找对应的字段（忽略大小写）
                        # 创建不区分大小写的字段ID
                        field_id_lower = f"{table_id}_{col_name.lower()}"
                        
                        # 使用字段名的小写形式进行比较
                        field_exists = False
                        matching_ids = []
                        
                        for idx, row in self.col_df[self.col_df['表ID'] == table_id].iterrows():
                            if row['字段名'].lower() == col_name.lower():
                                field_exists = True
                                matching_ids.append(row['字段ID'])
                        
                        if field_exists and i < len(sample_data_transposed):
                            # 获取示例数据行
                            sample_rows = sample_data_transposed[i]
                            # 更新示例数据列
                            for j in range(min(5, len(sample_rows))):
                                col_name_row = f"Row{j+1}"
                                # 更新所有匹配的字段ID
                                for match_id in matching_ids:
                                    self.col_df.loc[self.col_df['字段ID'] == match_id, col_name_row] = sample_rows[j]
                # 更新表清单处理状态
                self.tbl_df.loc[self.tbl_df['表ID'] == table_id, '示例数据'] = 2
            else:
                print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 未找到表 {table_id} 的示例数据")
                self.tbl_df.loc[self.tbl_df['表ID'] == table_id, ['示例数据', '统计数据']] = 0

                
        except Exception as e:
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 处理示例数据异常: {str(e)}")
            # 更新表清单处理状态为异常
            self.tbl_df.loc[self.tbl_df['表ID'] == table_id, '示例数据'] = 4
    
    def _process_stats(self, conn_id, schema, table, table_id, condition, explorer, auto_generate=True):
        """处理统计数据
        Args:
            conn_id (str): 连接ID
            schema (str): 库名
            table (str): 表名
            table_id (str): 表ID
            condition (str): 筛选条件
            explorer (DataExplorer): 数据探查器
            auto_generate (bool): 是否自动生成统计标示
        """
        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 处理统计数据: {table_id}")
        try:
            # 获取该表的所有字段
            if self.col_df.empty or not (self.col_df['表ID'] == table_id).any():
                print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 未找到表 {table_id} 的字段信息")
                return
                
            table_fields = self.col_df[self.col_df['表ID'] == table_id].copy()
            
            # 如果需要自动生成统计标示
            if auto_generate:
                # 确保所有必要的列都存在
                required_cols = ['类型', '字段名']
                for col in required_cols:
                    if col not in table_fields.columns:
                        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 字段清单缺少必要的列: {col}")
                        return
                
                # 提取字段类型和统计标示
                field_types = []
                for i, row in table_fields.iterrows():
                    field_type = row['类型'] if pd.notna(row['类型']) else ''
                    field_types.append(field_type)
                
                field_indices = []
                for i in range(len(field_types)):
                    ind = [0] * 13
                    for j in range(13):
                        col_name = self.STAT_COLUMN_NAMES[j]
                        if col_name in table_fields.columns:
                            value = table_fields.iloc[i][col_name]
                            ind[j] = value if pd.notna(value) else 0
                        else:
                            ind[j] = 0
                    field_indices.append(ind)
                
                # 自动生成统计标示
                field_names = table_fields['字段名'].tolist()
                field_info = list(zip(field_names, field_types))
                field_indices = explorer.pre_col_ind(field_info, field_indices)
                
                # 更新统计标示
                for i, (idx, field_row) in enumerate(table_fields.iterrows()):
                    for j in range(13):
                        col_name = self.STAT_COLUMN_NAMES[j]
                        if col_name in self.col_df.columns:
                            self.col_df.at[idx, col_name] = field_indices[i][j]
                            table_fields.at[idx, col_name] = field_indices[i][j]

            # 处理每个字段的统计
            all_completed = True
            
            for idx, field_row in table_fields.iterrows():
                # 确保所有必要的列都存在
                if '统计标示' not in field_row or pd.isna(field_row['统计标示']):
                    continue
                    
                if field_row['统计标示'] == 1:  # 检查统计标示
                    try:
                        # 获取统计数据
                        indicators = [1]  # 默认值
                        for j in range(1, 13):
                            col_name = self.STAT_COLUMN_NAMES[j]
                            if col_name in field_row and pd.notna(field_row[col_name]):
                                indicators.append(field_row[col_name])
                            else:
                                indicators.append(0)
                        
                        # 确保字段名存在
                        if '字段名' not in field_row or pd.isna(field_row['字段名']):
                            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 跳过处理：字段名不存在")
                            continue
                            
                        script = explorer.get_stats_script(
                            schema,            # schema
                            table,             # table
                            field_row['字段名'], # column
                            indicators,        # indicators
                            condition          # condition
                        )
                        df_stats = explorer.execute_script(script)
                        
                        if not df_stats.empty:
                            # 更新统计标示状态
                            if '统计标示' in self.col_df.columns:
                                self.col_df.at[idx, '统计标示'] = 2
                            
                            # 更新统计数据
                            stats_cols = ['行数', '空值数量', '空白数量', '不同值数量', '唯一值数量', '重复值数量', 
                                         '最小长度', '最大长度', '平均长度', '最小值', '最大值', '平均值']
                            
                            for i, col in enumerate(stats_cols):
                                if col in self.col_df.columns:
                                    self.col_df.at[idx, col] = df_stats.values[0][i]
                            
                            # 计算比率
                            if df_stats.values[0][0] != '-' and float(df_stats.values[0][0]) > 0:
                                if '空值率' in self.col_df.columns and df_stats.values[0][1] != '-':
                                    self.col_df.at[idx, '空值率'] = float(df_stats.values[0][1])/float(df_stats.values[0][0])
                                if '空白率' in self.col_df.columns and df_stats.values[0][2] != '-':
                                    self.col_df.at[idx, '空白率'] = float(df_stats.values[0][2])/float(df_stats.values[0][0])
                                if '不同值率' in self.col_df.columns and df_stats.values[0][3] != '-':
                                    self.col_df.at[idx, '不同值率'] = float(df_stats.values[0][3])/float(df_stats.values[0][0])
                                if '唯一值率' in self.col_df.columns and df_stats.values[0][4] != '-':
                                    self.col_df.at[idx, '唯一值率'] = float(df_stats.values[0][4])/float(df_stats.values[0][0])
                                if '重复值率' in self.col_df.columns and df_stats.values[0][5] != '-':
                                    self.col_df.at[idx, '重复值率'] = float(df_stats.values[0][5])/float(df_stats.values[0][0])
                            
                            # 获取TOP10频率统计
                            freq_script = explorer.get_freq_script(
                                schema,            # schema
                                table,             # table
                                field_row['字段名'], # column
                                condition          # condition
                            )
                            df_freq = explorer.execute_script(freq_script)
                            
                            if not df_freq.empty:
                                # 将频率统计结果添加到统计结果中
                                freq_data = df_freq.values.flatten()
                                row_count = float(df_stats.values[0][0])
                                
                                for k in range(min(10, len(df_freq))):
                                    val_col = f'TOP{k+1:02d}_VAL'
                                    rate_col = f'TOP{k+1:02d}_RATE'
                                    if 2*k < len(freq_data) and val_col in self.col_df.columns:
                                        self.col_df.at[idx, val_col] = freq_data[2*k]
                                    if 2*k+1 < len(freq_data) and row_count > 0 and rate_col in self.col_df.columns:
                                        self.col_df.at[idx, rate_col] = float(freq_data[2*k+1])/row_count
                            
                        else:
                            all_completed = False
                            
                    except Exception as e:
                        print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 处理字段统计异常: {str(e)}")
                        all_completed = False
                        continue
                elif field_row['统计标示'] == 0:
                    all_completed = False
            
            # 更新表清单处理状态
            if all_completed:
                self.tbl_df.loc[self.tbl_df['表ID'] == table_id, '统计数据'] = 2
            else:
                self.tbl_df.loc[self.tbl_df['表ID'] == table_id, '统计数据'] = 1
            
        except Exception as e:
            print(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} 处理统计数据异常: {str(e)}")
            # 更新表清单处理状态为异常
            self.tbl_df.loc[self.tbl_df['表ID'] == table_id, '统计数据'] = 4

# 修改运行入口，支持命令行参数
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='数据探查工具')
    parser.add_argument('--config', default='config.yml', help='配置文件路径')
    parser.add_argument('--conn_id', help='数据库连接ID')
    parser.add_argument('--output', help='输出Excel文件路径')
    
    args = parser.parse_args()
    
    app = DataExpApp(
        config_file=args.config,
        conn_id=args.conn_id,
        output_file=args.output
    );
    
    app.run() 