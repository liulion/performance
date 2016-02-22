#-*- coding=utf-8 -*-
import cx_Oracle
import re

def connectDB(dbname='ORCL'):
	#连接Oracle数据库
	if dbname=='ORCL':
		connstr='perfdata/perfdata123@localhost/ORCL'
	db=cx_Oracle.connect(connstr)
	return db

def sqlSelect(sql,db):
	#执行查询语句
	cr=db.cursor()
	cr.execute(sql)
	rs=cr.fetchall()
	cr.close()
	return rs

def sqlDML(sql,db):
	#执行增删改语句
	cr=db.cursor()
	cr.execute(sql)
	cr.close()
	db.commit()

def sqlDML2(sql,params,db):
	# 执行带参数的数据库操纵语句
	cr=db.cursor()
	cr.execute(sql,params)
	cr.close()
	db.commit()

def sqlDDL(sql,db):
	#包括执行建表语句
	cr=db.cursor()
	cr.execute(sql)
	cr.close()
		
def GetRE(content,regexp):
	#获取正则结果
	temp = re.findall(regexp,content)
	return temp

def getElement(col,row_series):
	#获取指定的Excel中某一个单元格的数据
	element = row_series[col]
	return element

def getConfig(config,configSection,key):
	#获取INI中指定SECTION和KEY的值
	return (config.get(configSection,key)).decode('UTF-8')
	
def getDate(filename,regexp):
	#获取日期
	content = filename
	L_DATE = GetRE(content, regexp)
	L_YEAR = GetRE(L_DATE[0],'(\d{4})-\d{2}-\d{2}')
	L_MOUTH = GetRE(L_DATE[0],'\d{4}-(\d{2})-\d{2}')
	L_DAY = GetRE(L_DATE[0],'\d{4}-\d{2}-(\d{2})')
	L_DATE = L_YEAR[0]+L_MOUTH[0]+L_DAY[0]
	return L_DATE
	
def getFundID_PortID(funds,portName):
	#根据投资组合的中文名臣获取所在的FUND_ID和PORT_ID
	for fund in funds:
		ports = fund.getElementsByTagName("port")
		for port in ports:
			if port.getElementsByTagName('portName')[0].childNodes[0].data == portName:
				return [port.parentNode.getAttribute("fundId"), port.getAttribute("portId")]	