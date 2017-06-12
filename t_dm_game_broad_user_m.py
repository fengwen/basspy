# -*- coding: utf-8 -*-
import sys
from migubase import BassFuncClass
reload(sys)
sys.setdefaultencoding('utf8')

#***************************************************************************************************
# **  filename：   t_dm_cmread_xtactiveindex_d2.sh
# **  功能描述：   咪咕阅读活跃类指标
# **              
# **  author：     shuanwei
# **  创建日期：   20151012
# **  修改日志：
# **  修改日期          修改人          修改内容
# ** -----------------------------------------------------------------------------------------------
# **
# **  China Mobile(Hangzhou) Information Technology Co., Ltd.
# **  All Rights Reserved.
#***************************************************************************************************

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

create table if not exists game_user_tmp
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
location '/dw/tmp/game_user_tmp';

-- -- 写入数据

INSERT overwrite TABLE game_user partition(monthid='{curMonth}')
select msisdn,tpa,imei,idfa,cookie
from 
(SELECT msisdn,
       tpa,
       imei,
       idfa,
       cookie
FROM t_di_n_game_pay_d t
WHERE substr(dayid,1,6)='{curMonth}'
GROUP BY msisdn,tpa,imei,idfa,cookie
union all
SELECT msisdn,
       tpa,
       imei,
       idfa,
       cookie
FROM t_di_n_game_use_d t
WHERE substr(dayid,1,6)='{curMonth}'
GROUP BY msisdn,tpa,imei,idfa,cookie) t;


-- 统计使用手机号/第三方的用户
create table  if not exists game_user_trans1 
  (
    user_id string,
    imei string,
    idfa string,
    cookie string
  )  
partitioned by (monthid string)
row format delimited
fields terminated by '\t'
stored as parquet
location '/dw/tmp/game_user_trans1';



INSERT overwrite TABLE game_user_trans1 partition(monthid='{curMonth}')
SELECT if(msisdn <>'',msisdn,tpa) AS user_id,
       imei,
       idfa,
       cookie
FROM game_user
WHERE monthid = '{curMonth}'
  AND msisdn <>''
  OR tpa <>'';


-- 统计使用imei的用户
create table if not exists game_user_imei 
(
  user_id string 
) 
partitioned by (monthid string)
row format delimited
fields terminated by '\t'
stored as parquet
location '/dw/tmp/game_user_imei';

INSERT overwrite TABLE game_user_imei partition(monthid = '{curMonth}')
SELECT a.imei AS user_id
FROM
  (SELECT imei
   FROM game_user
   WHERE monthid = '{curMonth}'
     AND msisdn =''
     AND tpa =''
     AND imei <>''
   GROUP BY imei) a
LEFT OUTER JOIN
  (SELECT imei
   FROM game_user_trans1
   WHERE monthid = '{curMonth}'
   GROUP BY imei) b ON b.imei = a.imei
WHERE b.imei ='';

-- 统计使用idfa的用户
create table if not exists game_user_idfa 
(
  user_id string 
) 
partitioned by (monthid string)
row format delimited
fields terminated by '\t'
stored as parquet
location '/dw/tmp/game_user_idfa';

INSERT overwrite TABLE game_user_idfa partition(monthid = '{curMonth}')
SELECT a.idfa AS user_id
FROM
  (SELECT idfa
   FROM game_user
   WHERE monthid = '{curMonth}'
     AND msisdn =''
     AND tpa =''
     AND imei =''
     AND idfa <>''
   GROUP BY idfa) a
LEFT OUTER JOIN
  (SELECT idfa
   FROM game_user_trans1
   WHERE monthid = '{curMonth}'
   GROUP BY idfa) b ON b.idfa = a.idfa
WHERE b.idfa ='';


-- 统计使用cookie的用户
create table if not exists game_user_cookie 
(
  user_id string 
) 
partitioned by (monthid string)
row format delimited
fields terminated by '\t'
stored as parquet
location '/dw/tmp/game_user_cookie';

INSERT overwrite TABLE game_user_cookie partition(monthid = '{curMonth}')
SELECT a.cookie AS user_id
FROM
  (SELECT cookie
   FROM game_user_tmp
   WHERE monthid = '{curMonth}'
     AND msisdn =''
     AND tpa =''
     AND imei =''
     AND idfa =''
     AND cookie <>''
   GROUP BY cookie) a
LEFT OUTER JOIN
  (SELECT cookie
   FROM game_user_trans1
   WHERE monthid = '{curMonth}'
   GROUP BY cookie) b ON b.cookie = a.cookie
WHERE b.cookie ='';

-- 统计使用imei、idfa、cookie的用户
create table if not exists game_user_trans2 
(
  user_id string 
) partitioned by (monthid string)
row format delimited
fields terminated by '\t'
stored as parquet
location '/dw/tmp/game_user_trans2';


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
--{errorbreak}

-- 分省统计用户数
create table if not exists game_broad_user
(
  province_id string,
  province_name string,
  total bigint
) partitioned by (monthid string)
row format delimited
fields terminated by '\t'
stored as parquet
location '/dw/dm/game_broad_user';

INSERT overwrite TABLE game_broad_user partition(monthid = '{curMonth}')
SELECT b.province_id,
       b.province_name,
       count(*) AS total
FROM
  (SELECT DISTINCT user_id
   FROM
     (SELECT user_id
      FROM game_user_trans1
      WHERE monthid = '{curMonth}'
      UNION SELECT user_id
      FROM game_user_trans2
      WHERE monthid = '{curMonth}') a) a
LEFT JOIN t_di_segment_dim_new b ON substr(a.user_id,1,7) = b.segment
GROUP BY province_id,
         b.province_name;
"""

bass =BassFuncClass("aa")
def main():
	bass.curMonth="201705"
	rc = bass.execHiveSql(sql,dbname='default')
	sys.exit(rc)	

if __name__ == '__main__':
	main()

