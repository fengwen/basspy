# -*- coding: utf-8 -*-
import os
import sys,re,getopt
import pymysql
import time,datetime
import exceptions
import subprocess
import traceback
import ConfigParser
reload(sys)
sys.setdefaultencoding('utf8')





class BassFuncClass(object):
	def __init__(self, name):
		#默认参数列表
		self.stepNo=0
		self.sqlBreakStepNo=0
		self.datatime="";
		self.errorbreak="if errorcode<>0 break"
		self.errorcontinue="if errorcode<>0 continue"
		self.procAuthor=""
		self.modifyTime=""
		self.statusCode=1
		self.sourceTable=""
		self.targetTable=""
		self.params=""
		self.curDay=""
		self.curMonth=""
		self.startTime=datetime.datetime.now()
		self.getParams()
		self.getProcName()
		self.jfmysqlcfg = self.readMysqlConf("jfmysql")
		self.__logHead()

		self.sqlSeqNo=1
		self.errorMsg=""
		self.name = name

	 	
	 	

	#这里的 h 就表示该选项无参数，i:表示 i 选项后需要有参数
	def getParams(self):
		try:
			
			for p in sys.argv:
				self.params=self.params+p+' '
			
			opts,args = getopt.getopt(sys.argv[1:], "d:s:m:");
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
				self.preDay             = self.getDay(self.curDay,-1)
			 	self.preDay2            = self.getDay(self.curDay,-2)
			 	self.preDay3            = self.getDay(self.curDay,-3)				
			if opt == "-m":
				self.datatime = val
				self.curMonth = val
			if opt == "-s":
				self.sqlBreakStepNo = int(val)							
	
	def __del__(self):
		self.__logEnd()

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

	def replaceTimeVars(self,sql):
		keys = dir(self)
		for keyName in keys:
		    if keyName.startswith(('cur','pre','next','errorbreak','errorcontinue')):
				#print keyName,getattr(self, keyName)
				p=re.compile('({'+keyName+'})')
				sql = p.sub( getattr(self, keyName), sql)
		return sql			


	def execHiveSql(self,sql,**dbset):
		flag=0
		startTime=datetime.datetime.now()
		sql=self.replaceTimeVars(sql)
		dbname=dbset.get('dbname','')
		sqlList=sql.splitlines(False);
		onesql=""
		errorExit=""
		for i in range(len(sqlList)):
			if sqlList[i].find(self.errorbreak) <> -1:
				continue
			if len(sqlList[i].strip())>0:
				onesql=onesql+sqlList[i]+"\n"			
			if len(sqlList[i].strip())>0 and sqlList[i][-1]==";":
				if i+1 < len(sqlList) and (sqlList[i+1].find(self.errorbreak) <> -1 or sqlList[i+1].find(self.errorcontinue) <> -1):
					errorExit=sqlList[i+1]
				self.__writeLog("执行第("+str(self.sqlSeqNo)+")个SQL语句:\n[SQL]\n"+onesql+"\n[/SQL]");
				
				if self.sqlSeqNo>=self.sqlBreakStepNo:
					r=self.executeOneSql(onesql,dbname=dbname)
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
			self.__writeLog("操作分区表: "+partitiontable+"")
		if len(str(r['numRows']).strip())>0:
			numRows=int(r['numRows'])
			self.__writeLog("影响记录数: "+r['numRows']+" Rows");
		if len(str(r['errmsg']))>0:
			self.errorMsg=str(r['errmsg'])
			errorMsg=str(r['errmsg'])
			self.__writeLog("错误信息:"+self.errorMsg);		

		steplogsql=""" insert into t_program_step_log(proc_name,data_time,status_code,run_dura,target_table,table_data_time,effect_rows,error_msg,
		step_sql,begin_time,end_time,step_no,dbname)
		VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
		#params=(1,'selenium2','boy','U***@qq.com')
		parseparams=(self.procName,self.datatime,self.statusCode,timedur,table,self.datatime,numRows,self.errorMsg,sql,endTime.strftime("%Y-%m-%d %H:%M:%S"),
		startTime.strftime("%Y-%m-%d %H:%M:%S"),self.sqlSeqNo,dbname)
		self.execMySql(steplogsql,parseparams,self.jfmysqlcfg)
							
	def connect2mysql(self,hostname,port, username,password,database):
		"""Login to database and open cursor"""
		try:
			self.con = conn = pymysql.connect(host=hostname,port=int(port),user=username,passwd=password,db=database)
			self.con.autocommit(True)
			self.dboper = self.con.cursor()
		except Exception,e:
			self.__writeLog("cat not connect to "+database +" host="+host+" port="+port+" username="+username+" password="+password+" errinfo=["+str(e.args[0])+"]")
		
		

	def getProcName(self):
		self.absProcName=os.path.abspath(__file__)
		self.procName=sys.argv[0]
	
	def __logHead(self):
	 	self.__writeLog("",printFlag = 0,withTime=0,writeLog=0);
 		self.__writeLog("[procName]"+self.procName+"[/procName]",withTime=0);
 		self.__writeLog("[absProcName]"+self.absProcName+"[/absProcName]",withTime=0);
		self.__writeLog("[PARA]"+self.params+"[/PARA]",withTime=0)
		self.__writeLog("[BEGIN TIME]"+self.startTime.strftime("%Y-%m-%d %H:%M:%S")+"[/BEGIN TIME]",withTime=0)
		
		sql="delete from t_program_log where proc_name=%s and data_time=%s"
		parseparams=(self.procName,self.datatime)
		self.execMySql(sql,parseparams,self.jfmysqlcfg)
		
		sql="delete from t_program_step_log where proc_name=%s and data_time=%s"
		parseparams=(self.procName,self.datatime)
		self.execMySql(sql,parseparams,self.jfmysqlcfg)
		
		sql=""" insert into t_program_log(proc_name,data_time,proc_path,proc_author,modify_time,begin_time,
		    		status_code,source_table,target_table) VALUES (%s,%s,%s,%s,%s,%s,
		    		%s,%s,%s) """
		#params=(1,'selenium2','boy','U***@qq.com')
		parseparams=(self.procName,self.datatime,self.absProcName,self.procAuthor,self.modifyTime,self.startTime.strftime("%Y-%m-%d %H:%M:%S")
		,1,self.sourceTable,self.targetTable)
		self.execMySql(sql,parseparams,self.jfmysqlcfg)
			
		

	def __logEnd(self):
		#time.sleep(10)
		self.endTime=datetime.datetime.now()

		dursec = (self.endTime-self.startTime).seconds
		#print "dursec=",dursec
		hour = dursec/(60*60)
		dursec = dursec % (60*60) #ȡСʱº󶅊£Ԡī˽
		min = dursec / 60
		sec = dursec % (60)
  	
		sql="update t_program_log set  end_time=%s,status_code=%s,run_dura=%s,stepno=%s,error_msg=%s where proc_name=%s and data_time=%s"
		params=(self.endTime.strftime("%Y-%m-%d %H:%M:%S"),self.statusCode,dursec,self.sqlSeqNo,self.errorMsg,self.procName,self.datatime)
		self.execMySql(sql,params,self.jfmysqlcfg)
  		# #closeConnection()
		self.__writeLog("程序运行完成("+self.endTime.strftime("%Y-%m-%d %H:%M:%S")+") "+self.params+
			" 程序花费时间  "+str(hour)+" 时 "+str(min)+" 分 "+str(sec)+" 秒");
		self.__writeLog("[END TIME]"+time.strftime("%Y-%m-%d %H:%M:%S")+"[/END TIME]",withTime=0)
		
		

	def __writeLog(self,str,**flagDict):
		withTime=flagDict.get('withTime',1)
		if withTime==0:
			self.printf(str)
		else:
			self.printf(time.strftime("%Y-%m-%d %H:%M:%S")+" "+ str)

	def run_command_all(self,*popenargs, **kwargs):
		allresult = {}
		cmd = popenargs[0]
		if 'stdout' in kwargs or 'stderr' in kwargs :
			raise ValueError('xxxxxxx')
		process = subprocess.Popen(stdout=subprocess.PIPE,shell=True,stderr = subprocess.PIPE,*popenargs, **kwargs)
		output, unused_err = process.communicate()
		#print 'stdout : ',output
		#print 'stderr : ',unused_err
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
		#if dbname<>"":
		#	udbname="use "+dbname+";\n"
		hivecmd = 'hive -e "'+sql+'"'
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
				g = re.findall(r'(FAILED:([\s\S]*)+)',gstr,re.S)
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
		cp.read('migubass.conf')
		dbconfig['host'] = cp.get(dbname, 'host')
		dbconfig['port'] = cp.get(dbname, 'port')
		dbconfig['user'] = cp.get(dbname, 'user')
		dbconfig['password'] = cp.get(dbname, 'password')
		dbconfig['database'] = cp.get(dbname, 'database')
		return dbconfig

	def execMySql(self,sql,params,dbcfg):
		con = None
		cur =None
		try:
			conn=self.connect2db(dbcfg['host'],dbcfg['port'],dbcfg['user'],dbcfg['password'],dbcfg['database'])
			cur=conn.cursor()
			#sql="insert into user(id,name,sex,email) VALUES (%s,%s,%s,%s)"
			#params=(1,'selenium2','boy','U***@qq.com')
			cur.execute(sql,params)
			#conn.commit()
		except Exception,e:
			self.__writeLog("exec sql error "+str(e))
		finally:
			cur.close()
			conn.close()		

	def connect2db(self,hostname,port, username,password,database):
		"""Login to database and open cursor"""
		try:
			conn = pymysql.connect(host=hostname,port=int(port),user=username,passwd=password,db=database,charset="utf8")
			conn.autocommit(True)
		except Exception,e:
			self.__writeLog("cat not connect to "+database +" host="+host+" port="+port+" username="+username+" password="+password+" errinfo=["+str(e.args[0])+"]")
		return conn
				
				
class CalledCommandError(Exception):                                                       		
	def __init__(self, returncode, cmd, errorlog,output):
		self.returncode = returncode                                                           
		self.cmd = cmd                                                                         
		self.output = output                                                                   
		self.errorlog = errorlog                                                               
	def __str__(self):                                                                       
		return "命令运行错误:'%s',返回值: %s,错误信息： %s" % (self.cmd, str(self.returncode) ,self.errorlog)
		
		

		