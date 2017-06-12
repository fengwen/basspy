# BassFuncClass功能描述
- 封装hive、mysql等数据操作方法。后续增加sqoop、ftp等数据交换方法
- 配置文件独立，数据库用户等写入到配置文件migubass.conf中
- 支持hive SQL以";"分割符多sql编写，默认执行错误退出。加入--{errorcontinue}标签，当执行错误时可以继续执行
- 默认传入sql连接hive default库，支持数据库设置，支持队列设置
- 在不增加SQL语句情况支持断点SQL运行
- 记录程序开始结束时间。记录每个sql返回日志包括。执行状态、时长、操作表、记录数、开始结束时间、错误原因等。

# 使用方法
## python lin
要求安装pymysql组件
## 脚本样例
```
# -*- coding: utf-8 -*-
import sys
from migubase import BassFuncClass
reload(sys)
sys.setdefaultencoding('utf8')
sql = """
-- 统计整体用户信息
-- -- 创建表
create table if not exists game_user 
  (
    msisdn string,
    tpa string,
    imei string,
    idfa string,
    cookie string
  )  
partitioned by (monthid string)
row format delimited
fields terminated by '\t'
stored as parquet
location '/dw/dm/game_user';
"""
bass =BassFuncClass("aa")
def main():
	bass.curMonth="201705"
	rc = bass.execSql(sql,dbname='default')
	sys.exit(rc)	

if __name__ == '__main__':
	main()
```
## 执行方法
python t_xx_d.py -d yyyymmdd

python t_xx_m_py -m yyyymm

断点执行：python t_xx_m.py -d yyyymmdd -s 步骤号

python t_dm_game_broad_user_m.py -m 201705 -s 13

## 配置文件
```
[jfmysql]
host = 127.0.0.1
port = 3306
user = root
password = root
database = bass

...more
```
# migubase.BassFuncClass函数说明
## execHiveSql
```
execHiveSql(sql,dbname='default',queue="root.hadoop")
```
- 支持hive SQL以";"分割符多SQL传入执行
- 默认传入sql连接hive default库
- 支持数据库设置
- 支持队列设置
- 支持返回日志包括。执行状态、时长、操作表、记录数、开始结束时间、错误原因等
- 返回状态，非0为错误
## sql编写
### Tab键转义
如是tab键作为分隔符号，写成\\\t
### {errorcontinue}标签

错误继续执行。
```
INSERT overwrite TABLE game_user_trans2 partition(monthid = '{curMonth}')
SELECT user_id
FROM game_user_imei
WHERE monthid = '{curMonth}'
UNION
SELECT user_id
FROM game_user_idfa
WHERE monthid = '{curMonth}'
UNION
SELECT user_id
FROM game_user_cookie
WHERE monthid = '{curMonth}';
--{errorcontinue}
```




## 日志表信息
```
CREATE TABLE t_program_log(
    seqno LONG,
    proc_name VARCHAR(64),
    data_time VARCHAR(16),
    proc_path VARCHAR(256),
    proc_author VARCHAR(32),
    modify_time VARCHAR(32),
    begin_time VARCHAR(32),
    end_time VARCHAR(32),
    status_code INT,
    run_dura  INT,
    source_table VARCHAR(512),
    target_table VARCHAR(512),
    stepno INT,
    error_msg  VARCHAR(256)
);

create index idx_1 on t_program_log(proc_name,data_time);
```

```
drop table t_program_step_log;
CREATE TABLE t_program_step_log(
    seqno LONG,
    proc_name VARCHAR(64),
    data_time VARCHAR(16),
    begin_time VARCHAR(32),
    end_time VARCHAR(32),
    step_no INT,
    status_code INT,
    run_dura  DECIMAL(10,3),
    dbname varchar(16),
    source_table VARCHAR(512),
    target_table VARCHAR(512),
    table_data_time VARCHAR(32),
    effect_rows int,
    error_msg  VARCHAR(256),
    step_sql text
);
create index idx_1 on t_program_step_log(proc_name,data_time);
```