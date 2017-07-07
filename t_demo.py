#***************************************************************************************************
# **  文件名称：   t_demo.py
# **  功能描述：   demo
# **              
# **  创建者：     fw
# **  创建日期：   20170612
# **  修改日志：
# **  修改日期          修改人          修改内容
# ** -----------------------------------------------------------------------------------------------
# **
# **  migu
# **  All Rights Reserved.
#***************************************************************************************************

# -*- coding: utf-8 -*-
import sys
basspylib="../lib"
#sys.path.append(basspylib)
from migubase import BassFuncClass
reload(sys)
sys.setdefaultencoding('utf8')

bass =BassFuncClass(name="yjjf")

allsql=""
#初始化表名
bass.source_table1="mgwh_yjjf_ods.t_di_cmread_pagevisit_d"
bass.source_table2="mgwh_yjjf_ods.t_di_cmread_interface_d"
bass.source_table3="mgwh_yjjf_ods.t_di_cmread_uespagevisit_d"
bass.source_table4="mgwh_yjjf_ods.t_di_cmread_pagevisit_n_d"

bass.tmp_table1= "mgwh_yjjf_temp.t_lm_cmread_ydvisit_d2_tmp1"
#咪咕阅读整体剃重
bass.target_table="mgwh_yjjf_dm.t_lm_cmread_ydvisit_d2"
#咪咕阅读按platform剃重
bass.target_table2="mgwh_yjjf_dm.t_lm_cmread_ydvisit_d2_platform"



#判断接口是否已经加载
#v_count=`mysql -h$db_host -u$db_user -p$db_passwd -D$database << EOF | tail -n +2
#  select count(distinct interface_table) from t_load_to_hive_log
#   where interface_table in('$source_table1','$source_table2','$source_table3','$source_table4') and data_time = '{curDay}' and end_time is not null; 
#EOF`
#echo "v_count=$v_count"
#if [ 4 -le $v_count ]; then 
#  echo "the interface number is ok "
#else                             
#  echo "the interface number is less"      
##  exit 1
#fi

sql="""
	----新建临时表
	drop table if exists {tmp_table1};
	create table {tmp_table1}
	(
	   msisdn               string,
	   platform             string,
	   msisdn_type          string,
	   imei                 string,
	   idfa                 string,
	   cookie               string,
	   visit_times          bigint
	)
	stored as rcfile;

	----新建结果表
	----drop table if exists {target_table}
	create table if not exists {target_table}
	(
	   msisdn               string,
	   platform             string,
	   imei                 string,
	   idfa                 string,
	   cookie               string,
	   count_type           string,  --统计用户时的标签：msisdn、imei、idfa、cookie
	   visit_times          bigint
	)
	partitioned by (dayid string)
	stored as rcfile;

	create table if not exists {target_table2}
	(
	   msisdn               string,
	   platform             string,
	   imei                 string,
	   idfa                 string,
	   cookie               string,
	   count_type           string,  --统计用户时的标签：msisdn、imei、idfa、cookie
	   visit_times          bigint
	)
	partitioned by (dayid string)
	stored as rcfile;
"""
allsql=allsql+sql

#--20161001开始用新接口
if bass.curDay >= '20161001':
	sql="""
  ---汇总临时表
  insert overwrite table ${tmp_table1}
  select t1.msisdn,t1.platform,t1.msisdn_type,t1.imei,t1.idfa,t1.cookie,sum(1) as visit_times from
   (select t.msisdn,
          (case when t.portal_type = 1 then '1'  --wap
                when t.portal_type = 2 then '2'  --www
                when t.portal_type = 4 then '3'  --客户端
                when t.portal_type = 8 then '4'  --MM
                else '9' end) as platform,
          cast(t.msisdn_type as string) as msisdn_type,
          t.imei,
          t.idfa,
          t.cookie
     from ${source_table4} t  --用户页面访问话单（新）
    where t.dayid = '{curDay}' and t.portal_type in(1,2,4,8)) t1
   group by t1.msisdn,t1.platform,t1.msisdn_type,t1.imei,t1.idfa,t1.cookie;
  """ 
else:
	#汇总临时表
	sql="""
	insert overwrite table ${tmp_table1}
	select t1.msisdn,t1.platform,t1.msisdn_type,t1.imei,t1.idfa,t1.cookie,sum(1) as visit_times from
	 (select t.msisdn,
	         (case when t.portal_type = '1' then '1'  --wap
	               when t.portal_type = '2' then '2'  --www
	               when t.portal_type = '4' then '3'  --客户端
	               when t.portal_type = '8' then '4'  --MM
	               else '9' end) as platform,
	         t.msisdn_type,
	         t.imei,
	         t.idfa,
	         t.cookie
	    from ${source_table1} t  --页面访问话单
	   where t.dayid = '{curDay}'
	  union all
	  select t.msisdn,
	         (case when t.platform = '1' then '1'
	               when t.platform = '2' then '2'
	               when t.platform = '4' then '3'
	               when t.platform = '8' then '4'
	               else '9' end) as platform,
	         t.msisdn_type,
	         t.imei,
	         t.idfa,
	         t.cookie
	    from ${source_table2} t  --接口访问话单
	   where t.dayid = '{curDay}'
	  union all
	  select t.msisdn,
	         (case when t.platform = '1' then '1'
	               when t.platform = '2' then '2'
	               when t.platform in('4','16') then '3'
	               when t.platform = '8' then '4'
	               else '9' end) as platform,
	         t.msisdn_type,
	         t.imei,
	         t.idfa,
	         t.cookie
	    from ${source_table3} t  --UES页面访问话单
	   where t.dayid = '{curDay}') t1
	 group by t1.msisdn,t1.platform,t1.msisdn_type,t1.imei,t1.idfa,t1.cookie
	"""
allsql=allsql+sql

#--20160101~09没有游客数据，单独处理
if bass.curDay <= '20160109':
  #汇总到阅读访问用户中间层表
  #--msisdn--用户
	sql="""
  ---汇总到阅读访问用户中间层表
  ---msisdn--用户	
  insert overwrite table ${target_table} partition(dayid='{curDay}') 
  select t.msisdn,
         t.platform,
         '' as imei,
         '' as idfa,
         '' as cookie,
         'msisdn',
         sum(t.visit_times) as visit_times
    from ${tmp_table1} t
   group by t.msisdn,
            t.platform ;
	"""
elif bass.curDay>= '20161001':
  #汇总到阅读访问用户中间层表
	sql="""
	---汇总到阅读访问用户中间层表
  insert overwrite table ${target_table} partition(dayid='{curDay}') 
  select t3.msisdn
        ,t3.platform
        ,t3.imei       
        ,t3.idfa       
        ,t3.cookie     
        ,t3.count_type   --统计用户时的标签：msisdn、imei、idfa、cookie
        ,sum(t3.visit_times) as visit_times
    from (select msisdn,platform,imei,idfa,cookie,'msisdn' as count_type,visit_times
            from ${tmp_table1} where msisdn_type not in('7','8')
          union all
          select t1.msisdn,t1.platform,t1.imei,'' as idfa,'' as cookie,'imei' as count_type,t1.visit_times from
           (select * from ${tmp_table1} where msisdn_type in('7','8') and imei <> '') t1 left join
           (select distinct imei from ${tmp_table1} where msisdn_type not in('7','8') and imei <> '') t2 on t1.imei = t2.imei where t2.imei is null
          union all
          select t1.msisdn,t1.platform,'' as imei,t1.idfa,'' as cookie,'idfa' as count_type,t1.visit_times from
           (select * from ${tmp_table1} where msisdn_type in('7','8') and imei = '' and idfa <> '') t1 left join
           (select distinct idfa from ${tmp_table1} where msisdn_type not in('7','8') and idfa <> '') t2 on t1.idfa = t2.idfa where t2.idfa is null
          union all
          select t1.msisdn,t1.platform,'' as imei,'' as idfa,t1.cookie,'cookie' as count_type,t1.visit_times from
           (select * from ${tmp_table1} where msisdn_type in('7','8') and imei = '' and idfa = '' and cookie <> '') t1 left join
           (select distinct cookie from ${tmp_table1} where msisdn_type not in('7','8') and cookie <> '') t2 on t1.cookie = t2.cookie where t2.cookie is null) t3
   group by t3.msisdn
           ,t3.platform
           ,t3.imei       
           ,t3.idfa       
           ,t3.cookie     
           ,t3.count_type ;

  insert overwrite table ${target_table2} partition(dayid='{curDay}') 
  select t3.msisdn
        ,t3.platform
        ,t3.imei       
        ,t3.idfa       
        ,t3.cookie     
        ,t3.count_type   --统计用户时的标签：msisdn、imei、idfa、cookie
        ,sum(t3.visit_times) as visit_times
    from (select msisdn,platform,imei,idfa,cookie,'msisdn' as count_type,visit_times
            from ${tmp_table1} where msisdn_type not in('7','8')
          union all
          select t1.msisdn,t1.platform,t1.imei,'' as idfa,'' as cookie,'imei' as count_type,t1.visit_times from
           (select * from ${tmp_table1} where msisdn_type in('7','8') and imei <> '') t1 left join
           (select imei,platform from ${tmp_table1} where msisdn_type not in('7','8') and imei <> '' group by imei, platform) t2 on t1.imei = t2.imei and t1.platform = t2.platform where t2.imei is null
          union all
          select t1.msisdn,t1.platform,'' as imei,t1.idfa,'' as cookie,'idfa' as count_type,t1.visit_times from
           (select * from ${tmp_table1} where msisdn_type in('7','8') and imei = '' and idfa <> '') t1 left join
           (select idfa,platform from ${tmp_table1} where msisdn_type not in('7','8') and idfa <> '' group by idfa, platform) t2 on t1.idfa = t2.idfa and t1.platform = t2.platform where t2.idfa is null
          union all
          select t1.msisdn,t1.platform,'' as imei,'' as idfa,t1.cookie,'cookie' as count_type,t1.visit_times from
           (select * from ${tmp_table1} where msisdn_type in('7','8') and imei = '' and idfa = '' and cookie <> '') t1 left join
           (select cookie,platform from ${tmp_table1} where msisdn_type not in('7','8') and cookie <> '' group by cookie, platform) t2 on t1.cookie = t2.cookie and t1.platform = t2.platform where t2.cookie is null) t3
   group by t3.msisdn
           ,t3.platform
           ,t3.imei       
           ,t3.idfa       
           ,t3.cookie     
           ,t3.count_type ;

  --补全visit_times
  insert into table ${target_table} partition(dayid='{curDay}') 
  select null as msisdn,
         t1.platform,
         null as imei,
         null as idfa,
         null as cookie,
         'msisdn' as count_type,
         sum(t1.visit_times) as visit_times
    from (select platform,sum(visit_times) as visit_times from ${tmp_table1} group by platform
            union all
          select platform,(0 - sum(visit_times)) as visit_times from ${target_table} where dayid='{curDay}' group by platform) t1
  group by t1.platform ;

  insert into table ${target_table2} partition(dayid='{curDay}') 
  select null as msisdn,
         t1.platform,
         null as imei,
         null as idfa,
         null as cookie,
         'msisdn' as count_type,
         sum(t1.visit_times) as visit_times
    from (select platform,sum(visit_times) as visit_times from ${tmp_table1} group by platform
            union all
          select platform,(0 - sum(visit_times)) as visit_times from ${target_table2} where dayid='{curDay}' group by platform) t1
  group by t1.platform ;
"""
else:
	sql="""
  ---汇总到阅读访问用户中间层表
  insert overwrite table ${target_table} partition(dayid='{curDay}') 
  select t3.msisdn
        ,t3.platform
        ,t3.imei       
        ,t3.idfa       
        ,t3.cookie     
        ,t3.count_type   --统计用户时的标签：msisdn、imei、idfa、cookie
        ,sum(t3.visit_times) as visit_times
    from (select msisdn,platform,imei,idfa,cookie,'msisdn' as count_type,visit_times
            from ${tmp_table1} where msisdn_type not in('4','8')
          union all
          select t1.msisdn,t1.platform,t1.imei,'' as idfa,'' as cookie,'imei' as count_type,t1.visit_times from
           (select * from ${tmp_table1} where msisdn_type in('4','8') and imei <> '') t1 left join
           (select distinct imei from ${tmp_table1} where msisdn_type not in('4','8') and imei <> '') t2 on t1.imei = t2.imei where t2.imei is null
          union all
          select t1.msisdn,t1.platform,'' as imei,t1.idfa,'' as cookie,'idfa' as count_type,t1.visit_times from
           (select * from ${tmp_table1} where msisdn_type in('4','8') and imei = '' and idfa <> '') t1 left join
           (select distinct idfa from ${tmp_table1} where msisdn_type not in('4','8') and idfa <> '') t2 on t1.idfa = t2.idfa where t2.idfa is null
          union all
          select t1.msisdn,t1.platform,'' as imei,'' as idfa,t1.cookie,'cookie' as count_type,t1.visit_times from
           (select * from ${tmp_table1} where msisdn_type in('4','8') and imei = '' and idfa = '' and cookie <> '') t1 left join
           (select distinct cookie from ${tmp_table1} where msisdn_type not in('4','8') and cookie <> '') t2 on t1.cookie = t2.cookie where t2.cookie is null) t3
   group by t3.msisdn
           ,t3.platform
           ,t3.imei       
           ,t3.idfa       
           ,t3.cookie     
           ,t3.count_type ;

  #--补全visit_times
  insert into table ${target_table} partition(dayid='{curDay}') 
  select null as msisdn,
         t1.platform,
         null as imei,
         null as idfa,
         null as cookie,
         'msisdn' as count_type,
         sum(t1.visit_times) as visit_times
    from (select platform,sum(visit_times) as visit_times from ${tmp_table1} group by platform
            union all
          select platform,(0 - sum(visit_times)) as visit_times from ${target_table} where dayid='{curDay}' group by platform) t1
  group by t1.platform ;
  """
  
allsql=allsql+sql

def main():
	setAttr="set mapreduce.job.queuename=root.hadoop;"
	#print bass.replaceTimeVars(allsql)
	rc = bass.execHiveSql(allsql,dbname='mgwh_yjjf_temp',setAttr=setAttr)
	sys.exit(rc)

if __name__ == '__main__':
	main()