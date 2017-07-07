# -*- coding: utf-8 -*-
#***************************************************************************************************
# **  文件名称：   migubase.py
# **  功能描述：   python脚本库内处理公共函数库
# **  创建者：     fengwen
# **  创建日期：   20170601
# **  修改日志：
# **  修改日期          修改人          修改内容
#	**  		
# ** -----------------------------------------------------------------------------------------------
# **
# **  migu.
# **  All Rights Reserved.
#***************************************************************************************************
import os
import sys,re,getopt
import mysql.connector
import time,datetime
import exceptions
import subprocess
import traceback
import ConfigParser
reload(sys)
sys.setdefaultencoding('utf8')





class BassFuncClass(object):
	def __init__(self, name=None,lib=None):
		#默认参数列表
		self.name = name
		self.lib=lib
		self.stepNo=0
		self.sqlSeqNo=1
		self.sqlBreakStepNo=0
		self.datatime="";
		self.errorbreak="if errorcode<>0 break"
		self.errorcontinue="if errorcode<>0 continue"
		self.procAuthor=""
		self.modifyTime=""
		self.statusCode=1  #0 成功 1 开始运行 <0 失败
		self.cycleType=""
		self.procSourceTable=""
		self.procTargetTable=""
		self.params=""
		self.curDay=""
		self.curMonth=""
		self.startTime=datetime.datetime.now()
				
		self.errorMsg=""
		self.runFlag=1;
		self.failFlag=-1;
		self.succFlag=0;
		self.getProcName()
		self.jfmysqlcfg = self.readMysqlConf("jfmysql")
		self.bimysqlcfg = self.readMysqlConf("bimysql")
		self.yjjfdb = self.readMysqlConf("yjjfdb")
		#{'host':'10.146.26.58',port=3306,user=hadoop,'password'='hadoop123','database':'ftptohw',charset='utf8'		
		self.getParams()
		
		self.__logHead()


	def setProcStats(self,statusCode,errorMsg):
		self.statusCode=statusCode
		self.errorMsg=errorMsg
		

	#这里的 h 就表示该选项无参数，i:表示 i 选项后需要有参数
	def getParams(self):
		try:

			for p in sys.argv:
				self.params=self.params+p+' '

			opts,args = getopt.getopt(sys.argv[1:], "d:s:m:c:");
		except getopt.GetoptError:
			print '		'+self.procName+' -d yyyymmdd'
			print '		'+self.procName+' -d yyyymmdd -s 2'
			print '		or: '+self.procName+' -m yyyymm'
			sys.exit(2)
		

		for opt, val in opts:
			print opt,val
			if opt == "-h":
				print '		'+self.procName+' -d yyyymmdd'
				print '		or: '+self.procName+' -m yyyymm'
				sys.exit(2)
			if opt == "-d":
				self.datatime = val
				self.curDay = val
				self.checkDateFmt(self.curDay)
				self.preDay             = self.getDay(self.curDay,-1)
			 	self.preDay2            = self.getDay(self.curDay,-2)
			 	self.preDay3            = self.getDay(self.curDay,-3)
			 	self.curMonth						=	self.curDay[0:6]
			 	self.curMfirstDay				= self.curMonth+"01"
			 	self.cycleType					=	opt
			if opt == "-m":
				self.datatime = val
				self.curMonth = val
				self.checkMonthFmt(self.curMonth)
				self.cycleType					=	opt
			if opt == "-s":
				self.sqlBreakStepNo = int(val)
			if opt == "-c":
				self.flowCycle = val
				self.cycleType					=	opt
				
	def __del__(self):
		self.__logEnd()

	#校验传入的日期是否合法
	def checkDateFmt(self,checkDate):
		cm=re.compile('^2\d{7}$')
		result=cm.match(checkDate)
		if result==None:
			self.__writeLog(checkDate+" is a not date.")
			self.setProcStats(self.failFlag,checkDate+" is a not date.")
			self.sqlSeqNo=0
			sys.exit(-1)

	def checkMonthFmt(self,checkDate):
		cm=re.compile('^2\d{6}$')
		result=cm.match(checkDate)
		if result==None:
			self.__writeLog(checkDate+" is a not date.")
			self.setProcStats(self.failFlag,checkDate+" is a not date.")
			self.sqlSeqNo=0
			sys.exit(-1)
						
	def getDay(self,yyyymmdd,shiftDay):
		day = datetime.datetime.strptime(yyyymmdd, '%Y%m%d')
		delta=datetime.timedelta(days=shiftDay)
		LastDay=day+delta
		return LastDay.strftime('%Y%m%d')

	def wtlog(procName):
		client= dbutil.connection("127.0.0.1","root","root","migu")
		try:
			aa=client.cur
			sql = "insert into test1(name, age) values ('%s', %d)" % (procName, 36)
			aa.execute(sql)
		except Exception, e:
			print "write log error ",e

	def printf(self,str):
		print(str)

#	def replaceTimeVars(self,sql):
#		keys = dir(self)
#		for keyName in keys:
#		    if keyName.startswith(('cur','pre','next','errorbreak','errorcontinue','source','tmp','target','dim')):
#				#print keyName,getattr(self, keyName)
#					p=re.compile('(\${'+keyName+'})')
#					sql = p.sub( getattr(self, keyName), sql)
#					p=re.compile('({'+keyName+'})')
#					sql = p.sub( getattr(self, keyName), sql)
#		return sql

	def replaceTimeVars(self,sql):
		keys = dir(self)#获取参数所有列表信息
		mydict={}
		try:
			#查找${},
			g=re.findall(r'(\${(.+?)})',sql)
			for r in g:
				mydict[r[1]]=r[0]
			
			for (keyName,val) in  mydict.items():
				p=re.compile('(\${'+keyName+'})')
				sql = p.sub( getattr(self, keyName), sql)
				p=re.compile('({'+keyName+'})')
				sql = p.sub( getattr(self, keyName), sql)			
			#查找{}
			mydict={}
			g=re.findall(r'({(.+?)})',sql)
			for r in g:
				mydict[r[1]]=r[0]		

			for (keyName,val) in  mydict.items():
				p=re.compile('(\${'+keyName+'})')
				sql = p.sub( getattr(self, keyName), sql)
				p=re.compile('({'+keyName+'})')
				sql = p.sub( getattr(self, keyName), sql)
			
			#g=re.findall(r'({(.+?)})',sql)
			#if g is not None and len(g)>0:
			#	self.__writeLog("WARN:   变量未声名,替换失败,列表:"+str(g));
		except Exception,e:
			self.__writeLog("ERROR:   变量未声名,替换失败,"+str(e));
			self.setProcStats(self.failFlag,"ERROR:   变量未声名,替换失败,"+str(e))
			self.__writeLog("ERROR:   可以调用printParams()函数查看已经声明的变量\n")
			sys.exit(-1)
			
		return sql#返回替换表量的sql
	def checkSqlType(self,sql):
		sql=sql.lower()
		INSERTSQL="insert"
		CREATESQL="create"
		LOADSQL="load"
		DROPSQL="drop"
		sqlType=""

		pattern=re.compile(r'(.*)drop(.*)table(.*)',re.S)
		res=pattern.match(sql)
		if res:
			sqlType=LOADSQL
		
		pattern=re.compile(r'(.*)load(.*)data(.*)',re.S)
		res=pattern.match(sql)
		if res:
			sqlType=LOADSQL
		
		pattern=re.compile(r'(.*)create(.*)table|view(.*)',re.S)
		res=pattern.match(sql)
		if res:
			sqlType=CREATESQL

		pattern=re.compile(r'(.*)sqoop(.*)(.*)',re.S)
		res=pattern.match(sql)
		if res:
			sqlType="sqoop"
									
		pattern=re.compile(r'(.*)insert(.*)table(.*)select(.*)',re.S)
		res=pattern.match(sql)
		if res:
			sqlType=INSERTSQL			
		return sqlType
		
	def execHiveSql(self,sql,**dbset):
		flag=0
		startTime=datetime.datetime.now()
		sql=self.replaceTimeVars(sql)
		dbname=dbset.get('dbname','')
		setAttr=dbset.get('setAttr','')
		sqlList=sql.splitlines(False);
		onesql=""
		errorExit=""
		for i in range(len(sqlList)):
			linesqlstr=sqlList[i].strip()
			if linesqlstr.find(self.errorbreak) <> -1 or linesqlstr.find(self.errorcontinue)<> -1:
				continue
			if len(linesqlstr)>0:
				onesql=onesql+linesqlstr+"\n"
			if len(linesqlstr)>0 and linesqlstr[-1]==";":
				if i+1 < len(sqlList) and (sqlList[i+1].find(self.errorbreak) <> -1 or sqlList[i+1].find(self.errorcontinue) <> -1):
					errorExit=sqlList[i+1]
				self.__writeLog("执行第("+str(self.sqlSeqNo)+")个SQL语句:\n[SQL]\n"+onesql+"\n[/SQL]");

				if self.sqlSeqNo>=self.sqlBreakStepNo:
					r=self.executeOneSql(onesql,dbname=dbname,setAttr=setAttr)
					flag=int(r['returncode'])
					self.__printSqlFmtLog(r,dbname,onesql,startTime)
				else:
					self.__writeLog("忽略执行第("+str(self.sqlSeqNo)+")个SQL语句:\n[SQL]\n"+onesql+"\n[/SQL]");

				if flag<>0 and errorExit.find("continue") == -1:
					break
				if flag<>0 and errorExit.find("continue") <> -1:
					self.__writeLog("有错误错误继续标签，继续执行")

					#break
				self.sqlSeqNo=self.sqlSeqNo+1
			## 写step数据库日志

				onesql=""
				errorExit=""
		return flag




	def executeSql(self,sql,**dbset):
		sql=self.replaceTimeVars(sql)
		#self.printf(sql)
		sqls=sql.strip().split(";")
		dbname=dbset.get('dbname','')
		for sqlstr in sqls:
			if len(sqlstr.strip())>10:
				#print "self.sqlSeqNo:",str(self.sqlSeqNo),"self.sqlBreakStepNo:",str(self.sqlBreakStepNo)
				if self.sqlSeqNo>=self.sqlBreakStepNo:
					self.__writeLog("执行第("+str(self.sqlSeqNo)+")个SQL语句:\n[SQL]\n"+sqlstr+"\n[/SQL]");
					r=self.executeOneSql(sqlstr,dbname=dbname)
					self.__printSqlFmtLog(r)
				else:
					self.__writeLog("忽略执行第("+str(self.sqlSeqNo)+")个SQL语句:\n[SQL]\n"+sqlstr+"\n[/SQL]");
				self.sqlSeqNo=self.sqlSeqNo+1
		return 0

	def __printSqlFmtLog(self,r,dbname,sql,startTime):
		endTime=datetime.datetime.now()
		table=""
		partitiontable=""
		numRows=0
		timedur=0
		errorMsg=""
		if str(r['returncode'])=="0":
			self.statusCode=0
			self.errorMsg=""
		else:
			self.statusCode=-1

		self.__writeLog("返回值: "+str(r['returncode']));
		self.__writeLog("执行时长: "+r['timedur']+" 秒");
		timedur=r['timedur']
		if len(str(r['table']))>0:
			table=r['table']
			self.__writeLog("操作表: "+table+"");
		if len(str(r['partitiontable']))>0:
			table=r['partitiontable']
			self.__writeLog("操作分区表: "+table+"")
		if len(str(r['numRows']).strip())>0:
			numRows=int(r['numRows'])
			self.__writeLog("影响记录数: "+r['numRows']+" Rows");
		if len(str(r['errmsg']))>0:
			self.errorMsg=str(r['errmsg'])
			errorMsg=str(r['errmsg'])
			self.__writeLog("错误信息:"+self.errorMsg);
		
		sqlType=self.checkSqlType(sql)
		
		if len(str(table))>0:
			pos=table.find('.',1)
			if pos<>-1:
				dbname=table[0:pos]

		steplogsql=""" insert into t_program_step_log(proc_name,data_time,status_code,run_dura,target_table,effect_rows,error_msg,
		step_sql,begin_time,end_time,step_no,dbname,sql_type)
		VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
		#params=(1,'selenium2','boy','U***@qq.com')
		parseparams=(self.procName,self.datatime,self.statusCode,timedur,table.strip(),numRows,self.errorMsg,sql,endTime.strftime("%Y-%m-%d %H:%M:%S"),
		startTime.strftime("%Y-%m-%d %H:%M:%S"),self.sqlSeqNo,dbname.strip(),sqlType)
		self.execMetaMySql(steplogsql,parseparams,self.jfmysqlcfg)

#	def connect2mysql(self,hostname,port, username,password,database):
#		"""Login to database and open cursor"""
#		try:
			#self.con = conn = mysql.connector.connect(host=hostname,port=int(port),user=username,passwd=password,db=database)
			#self.con.autocommit(True)
			#self.dboper = self.con.cursor()
#		except Exception,e:
#			self.__writeLog("cat not connect to "+database +" host="+host+" port="+port+" username="+username+" password="+password+" errinfo=["+str(e.args[0])+"]")



	def getProcName(self):
		self.absProcName=os.path.abspath(__file__)
		fPath,fFileName=os.path.split(self.absProcName)
		self.absProcPath=fPath
		self.procName=sys.argv[0]

	def __logHead(self):
	 	self.__writeLog("",printFlag = 0,withTime=0,writeLog=0);
 		self.__writeLog("[procName]"+self.procName+"[/procName]",withTime=0);
 		self.__writeLog("[absProcName]"+self.absProcName+"[/absProcName]",withTime=0);
		self.__writeLog("[PARA]"+self.params+"[/PARA]",withTime=0)
		self.__writeLog("[BEGIN TIME]"+self.startTime.strftime("%Y-%m-%d %H:%M:%S")+"[/BEGIN TIME]",withTime=0)
		
		
		#self.setSourceTagetTables()
		if self.cycleType=="-d" or self.cycleType=="-m":
			sql="delete from t_program_log where proc_name=%s and data_time=%s"
			parseparams=(self.procName,self.datatime)
			self.execMetaMySql(sql,parseparams,self.jfmysqlcfg)

			sql="delete from t_program_step_log where proc_name=%s and data_time=%s"
			parseparams=(self.procName,self.datatime)
			self.execMetaMySql(sql,parseparams,self.jfmysqlcfg)

			sql=""" insert into t_program_log(proc_name,data_time,proc_path,proc_author,modify_time,begin_time,
			    		status_code,source_table,target_table) VALUES (%s,%s,%s,%s,%s,%s,
			    		%s,%s,%s) """
			#params=(1,'selenium2','boy','U***@qq.com')
			parseparams=(self.procName,self.datatime,self.absProcName,self.procAuthor,self.modifyTime,self.startTime.strftime("%Y-%m-%d %H:%M:%S")
			,1,self.procSourceTable,self.procTargetTable)
			self.execMetaMySql(sql,parseparams,self.jfmysqlcfg)
			os.system('klist')

	def setSourceTagetTables(self):
		keys = dir(self)
		source = []
		target = []
		for keyName in keys:
			if keyName.startswith(('source')):
				t=str(getattr(self, keyName))
				if len(t.strip())>0:
					source.append(t)
			if keyName.startswith(('target')):
				t=str(getattr(self, keyName))
				if len(t.strip())>0:
					target.append(t)
		self.procSourceTable=str(source)
		self.procTargetTable=str(target)
		
	def __logEnd(self):
		#time.sleep(10)
		self.endTime=datetime.datetime.now()

		dursec = (self.endTime-self.startTime).seconds
		#print "dursec=",dursec
		hour = dursec/(60*60)
		dursec = dursec % (60*60) #ȡСʱº󶅊£Ԡī˽
		min = dursec / 60
		sec = dursec % (60)
		
		
		if self.cycleType=="-d" or self.cycleType=="-m":
			self.setSourceTagetTables()
			sql="update t_program_log set  end_time=%s,status_code=%s,run_dura=%s,stepno=%s,error_msg=%s,source_table=%s,target_table=%s where proc_name=%s and data_time=%s"
			params=(self.endTime.strftime("%Y-%m-%d %H:%M:%S"),self.statusCode,dursec,self.sqlSeqNo,self.errorMsg,self.procSourceTable,self.procTargetTable,self.procName,self.datatime)
			self.execMetaMySql(sql,params,self.jfmysqlcfg)
  		# #closeConnection()
		self.__writeLog("程序运行完成("+self.endTime.strftime("%Y-%m-%d %H:%M:%S")+") 运行状态:"+str(self.statusCode) +" "+self.params+
			" 程序花费时间  "+str(hour)+" 时 "+str(min)+" 分 "+str(sec)+" 秒");
		self.__writeLog("[END TIME]"+time.strftime("%Y-%m-%d %H:%M:%S")+"[/END TIME]",withTime=0)



	def __writeLog(self,str,**flagDict):
		withTime=flagDict.get('withTime',1)
		if withTime==0:
			self.printf(str)
		else:
			self.printf(time.strftime("%Y-%m-%d %H:%M:%S")+" "+ str)

	def sqoopHiveToMysql(self,**args):
		#传入hivetable="xxx",partition="xxx",mysqldb="xxx",table="xxx".如果传入的hivetale是标准的xxx_xx_xx.table，会默认拼接路径，否则需要传入hdfspath
		allresult = {}
		try:
			startTime=datetime.datetime.now()
			dbconfig=self.readMysqlConf(args["mysqldb"])
			dbname=dbconfig["database"]
			url="jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf-8" % (dbconfig["host"],dbconfig["port"],dbconfig["database"])
			username=dbconfig["user"]
			password=dbconfig["password"]
			table=args["table"]
			hivetable=args["hivetable"]
			x= hivetable.split('.')
			hivetablename=x[len(x)-1]
			
			hdfspathFromDbname=""
			exportdir=""
			
			#通过表配置名拼接路径
			if len(x)==2:
				hivedbname=x[0];
				y=hivedbname.split('_')
				if len(y)==3:
					hdfspathFromDbname="/data/%s/%s/hive/%s" % (y[0],y[1],y[2])
					print "hdfspathFromDbname:"+hdfspathFromDbname
			
			if	len(hdfspathFromDbname)>1:
				exportdir=hdfspathFromDbname+"/"+hivetablename
			
			if	len(hdfspathFromDbname)==0 and len(args["hdfspath"])>1:
				exportdir=args["hdfspath"]+"/"+hivetablename
			
			if	"parition" in args.keys():
				exportdir=exportdir+"/"+args["parition"]
			sqoopCmd = 'sqoop-export -jt local --connect "%s" --username %s --password %s --table %s --export-dir %s --fields-terminated-by \'\\t\' --input-null-string \'\\\N\' --input-null-non-string \'\\\N\'' % (url,username,password,table,exportdir) 
			self.__writeLog(sqoopCmd)
			e=self.run_command_all(sqoopCmd)
			gstr = e['errorlog']
			allresult['returncode']=e['returncode']
			allresult['errorlog']=gstr
			if e['returncode']==0:
				g = re.findall(r"Exported (.+?) records",gstr)
				if g is not None and len(g)>0:
					allresult['numRows']=g[0]
				g = re.findall(r" in (.+?) seconds ",gstr)
				if g is not None and len(g)>0:
					i=len(g)-1
					allresult['timedur']=g[i]
				
				allresult['errmsg']=""
			else:
					allresult['errmsg']="export Error"
			
			allresult['partitiontable']=""
			allresult["table"]=dbname+"."+table
			self.__printSqlFmtLog(allresult,dbname,sqoopCmd,startTime)		
		except Exception,e:
			print "exec failed"
			traceback.print_exc()
		return allresult					
	def run_command_all(self,*popenargs, **kwargs):
		allresult = {}
		cmd = popenargs[0]
		if 'stdout' in kwargs or 'stderr' in kwargs :
			raise ValueError('xxxxxxx')
		process = subprocess.Popen(stdout=subprocess.PIPE,shell=True,stderr = subprocess.PIPE,*popenargs, **kwargs)
		output, unused_err = process.communicate()
		
		print 'stderr : ',unused_err
		print 'stdout : ',output
		#output, unused_err = process.communicate()
		retcode = process.poll()
		#if retcode:
		#	print retcode,cmd,unused_err,output
		#	raise CalledCommandError(cmd,retcode,errorlog=unused_err,output=output)
		allresult['cmd'] = cmd
		allresult['returncode'] = retcode
		allresult['errorlog'] = unused_err
		allresult['outdata'] = output
		return allresult

	def executeOneSql(self,sql,**dbset):
		allresult = {}
		allresult['errmsg']=""
		allresult['timedur']="0"
		allresult['table']=""
		allresult['partitiontable']=""
		allresult['numRows']=""

		dbname=dbset.get('dbname','')
		udbname=''
		if dbname<>"":
			udbname="use "+dbname+";\n"

		setAttr=dbset.get('setAttr','')
		if setAttr<>"":
			udbname=udbname+setAttr+"\n"
		
		udbname=udbname+"set hive.stats.autogather=true;\n"
		hivecmd = 'hive -e "'+udbname+sql+'"'

		#print hivecmd
		try:
			e=self.run_command_all(hivecmd)
			#print "errorlog:",e['errorlog']

			gstr = e['errorlog']
			allresult['returncode']=e['returncode']
			if e['returncode']==0:
				g = re.findall(r"Table(.+?)stats:",gstr)
				if g is not None and len(g)>0:
					allresult['table']=g[0]
				g = re.findall(r"Partition(.+?)stats:",gstr)
				if g is not None and len(g)>0:
					allresult['partitiontable']=g[0].strip()
				g = re.findall(r"numRows=(.+?),",gstr)
				if g is not None and len(g)>0:
					allresult['numRows']=g[0]
				g = re.findall(r"Time taken: (.+?) seconds",gstr)
				if g is not None and len(g)>0:
					i=len(g)-1
					allresult['timedur']=g[i]
			else:
				#print "returnCode:",e['returncode']
				#g = re.findall(r'(FAILED:([\s\S]*)+)',gstr,re.S)#2.6版本不支持此写法
				g=re.findall(r'(FAILED:.*)',gstr,re.S)
				if g is not None and len(g)>0:
					allresult['errmsg']=g[0]
		except Exception,e:
			#print (str(e))
			print "exec failed"
			traceback.print_exc()
		return allresult

	def sendMail():
		#发送邮件
		print "send mail"

	def readMysqlConf(self,dbname):
		dbconfig={}
		cp = ConfigParser.SafeConfigParser(allow_no_value = True)
		cfgfile=""
		if self.lib is None:
			cfgfile=self.absProcPath+"/migubass.conf.py"
		else:
			cfgfile=self.absProcPath+"/migubass.conf.py"
		cp.read(cfgfile)
		dbconfig['host'] = cp.get(dbname, 'host')
		dbconfig['port'] = cp.get(dbname, 'port')
		dbconfig['user'] = cp.get(dbname, 'user')
		dbconfig['password'] = cp.get(dbname, 'password')
		dbconfig['database'] = cp.get(dbname, 'database')
		return dbconfig

	def execMetaMySql(self,sql,params,dbcfg):
		#元数据库记录程序日志，不需要修改环境变量
		flag=0
		cnx = None
		cur =None
		errmsg=""
		try:
			cnx=mysql.connector.connect(host=dbcfg['host'],port=int(dbcfg['port']),user=dbcfg['user'],password=dbcfg['password'],database=dbcfg['database'],charset='utf8')
			cur = cnx.cursor()
			#sql="insert into user(id,name,sex,email) VALUES (%s,%s,%s,%s)"
			#params=(1,'selenium2','boy','U***@qq.com')
			cur.execute(sql,params)
			cnx.commit()
			cur.close()
			cnx.close()
		except Exception,e:
			self.__writeLog("exec sql error "+str(e))
			flag=-1
		return flag
		
	def execMySql(self,sql,params,dbcfg):
		flag=0
		cnx = None
		cur =None
		sql=self.replaceTimeVars(sql)
		#self.printf(sql)
		sqls=sql.strip().split(";")
		errmsg=""
		try:
			#cnx=self.connect2db(dbcfg['host'],dbcfg['port'],dbcfg['user'],dbcfg['password'],dbcfg['database'])
			#conn = mysql.connector.connect(host='172.19.204.156',user='hadoop',password='hadoop123',database='mgreport')
			cnx=mysql.connector.connect(host=dbcfg['host'],port=int(dbcfg['port']),user=dbcfg['user'],password=dbcfg['password'],database=dbcfg['database'],charset='utf8')
			#cnx.autocommit(True)
			cur = cnx.cursor()
			#sql="insert into user(id,name,sex,email) VALUES (%s,%s,%s,%s)"
			#params=(1,'selenium2','boy','U***@qq.com')
			for sqlstr in sqls:
				self.__writeLog(sqlstr)
				cur.execute(sqlstr,params)
				cnx.commit()
			cur.close()
			cnx.close()
		except Exception,e:
			self.__writeLog("exec sql error "+str(e))
			errmsg=str(e)
			flag=-1
			#cur.close()
			#conn.close()
		self.setProcStats(flag,errmsg)	
		return flag
	def connect2db(self,hostname,port, username,password,database):
		"""Login to database and open cursor"""
		try:
			conn = mysql.connector.connect(host=hostname,port=int(port),user=username,password=password,database=database,charset='utf8')
			conn.autocommit(True)
		except Exception,e:
			self.__writeLog("cat not connect to "+database +" host="+host+" port="+port+" username="+username+" password="+password+" errinfo=["+str(e.args[0])+"]")
		return conn

	def printParams(self):
		keys = dir(self)
		#if keyName.startswith(('cur','pre','next','errorbreak','errorcontinue','source','tmp','target','dim')):	
		self.__writeLog("时间变量列表:在公共lib中声明，只是显示'cur','pre','next'开头，公共的变量可以持续添加。如在程序中自己声明，使用bass.curMLastDay表示当月最后一天",withTime=0)
		for keyName in keys:
			if keyName.startswith(('cur','pre','next')):
				msg='%+32s = %-64s' % (keyName,str(getattr(self, keyName)))
				print (msg)
				#self.__writeLog("  "+keyName+" = "+str(getattr(self, keyName)))
		self.__writeLog("其他变量：只是显示以'source','tmp','target','dim'开头的变量",withTime=0)		
		for keyName in keys:
			if keyName.startswith(('source','tmp','target','dim')):
				msg='%+32s = %-64s' % (keyName,str(getattr(self, keyName)))
				print (msg)		
	def checkInterface(self,dataTime,checkNum,sourceTables):
	#连接mysql检查接口数量
		flag=0
		num=0
		try:
			dbcfg=self.jfmysqlcfg
			cnx=mysql.connector.connect(host=dbcfg['host'],port=int(dbcfg['port']),user=dbcfg['user'],password=dbcfg['password'],database=dbcfg['database'],charset='utf8')
			cur = cnx.cursor()
			
			tabstr=""
			for i in range(len(sourceTables)):
				x= sourceTables[i].split('.')
				v=x[len(x)-1]
				if i==0:
					tabstr="'"+v+"'"
				else:
					tabstr=tabstr+",'"+ v+"'"
			
			stmt_select = (" select count(distinct interface_table) from t_load_to_hive_log "
										 " where interface_table in("+tabstr+") and data_time = '"+dataTime+"' and end_time is not null"
										)
			print 		stmt_select					
			cur.execute(stmt_select)
			row=cur.fetchone()
			num=row[0]
			if num <> checkNum:
				flag=-1
				errMsg="ODS接口表校验个数不符合。当前值("+str(num)+") 比较值("+str(checkNum)+") "
				self.errorMsg=errMsg
				self.statusCode=-2
				self.__writeLog(errMsg+stmt_select)
			cur.close()
			cnx.close()
		except Exception,e:
			flag=-1
			self.__writeLog("exec Mysqlsqldb "+dbcfg['database']+" error:"+str(e))
		
		return flag
			
class CalledCommandError(Exception):
	def __init__(self, returncode, cmd, errorlog,output):
		self.returncode = returncode
		self.cmd = cmd
		self.output = output
		self.errorlog = errorlog
	def __str__(self):
		return "命令运行错误:'%s',返回值: %s,错误信息： %s" % (self.cmd, str(self.returncode) ,self.errorlog)



