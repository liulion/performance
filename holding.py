#!/usr/bin/python
# -*- coding: UTF-8 -*-

import ConfigParser
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8' 
import pandas as pd
from pandas import Series, DataFrame
from xml.dom.minidom import parse
import xml.dom.minidom
import sys
import urllib
import numpy as np
import math
import cProfile
import shutil

import tool

class holdings():
	def __init__(self):
		self.sourceDirectoryName=(config.get("source","sourceFileDirectory")).decode('UTF-8')
		self.sourceFileName=(config.get("source","sourceFileName").decode('UTF-8')).split(";")
		self.backDirectoryName=(config.get("source","backFileDirectory")).decode('UTF-8')
	
	def getHoldingsReturn(self,sourceDirectory,sourceFileName=None):
		holdingDF=pd.DataFrame()
		sourceFileName = [x for x in sourceFileName if x != u'']
		listFiles = sourceFileName
		if sourceFileName == []:
			listFiles=os.listdir(sourceDirectory)
		sectionList = config.sections()
		colDic = {}
		colString = 'FUND_ID,PORT_ID,L_DATE,ACCOUNT_ID,ACCOUNT_NAME,POSITION_FLAG,MARKET_NO,SUBMARKET_NO,WINDSECURITY_CODE,SECURITY_CODE,SECURITY_TYPE,AMOUNT,UNIT_COST,TOTAL_COST,MARKET_PRICE,MARKET_VALUE,PANDL,NETASSET_PERCENT,NETTOTALASSET_PERCENT,BEGIN_AMOUNT,BUY_AMOUNT,SALE_AMOUNT,BUY_CASH,SALE_CASH,BUY_FEE,SALE_FEE' 
		colList = colString.split(",")
		for col in colList:
			colDic[col + 'List'] = []
		for file in listFiles:
			index_col = 0
			df = pd.read_excel(sourceDirectory+file, sheetname='Sheet1', header = 3, index_col = index_col)
			DOMTree = xml.dom.minidom.parse("fund_port_rel.xml")
			mapping = DOMTree.documentElement
			funds = mapping.getElementsByTagName("fund")
			FundID_PortID = tool.getFundID_PortID(funds,tool.GetRE(file, '.*_(.+)_.*')[0])
			l_date = tool.getDate(file,'.*(\d{4}-\d{2}-\d{2}).xls')
			def test(x):
				if isinstance(x,unicode):
					if tool.GetRE(x,'^(\d{6}).*') != []: 
						if tool.GetRE(x,'^(\d{6}).*')[0] == u'110201' or tool.GetRE(x,'^(\d{6}).*')[0] == u'110231' or tool.GetRE(x,'^(\d{6}).*')[0] == u'110241' or tool.GetRE(x,'^(\d{6}).*')[0] == u'110503' or tool.GetRE(x,'^(\d{6}).*')[0] == u'110901':
							if len(x) != 14:
								return False
							else:
								return True
						else:
							if len(x) != 6:
								return False
							else:
								return True
			indexList =  filter(test, list(df.index)) 
			for row in indexList:
				fieldDic = {}
				for col in colList:
					fieldDic[col] = 'null'
				market_noTEMP = 'null'
				submarket_noTEMP = 'null'
				security_codeTEMP = 'null'
				security_typeTEMP = 'null'
				windsecurity_codeTEMP = 'null'
				position_flagTEMP = 'long'
				if tool.GetRE(row,'^(\d{6}).*')[0] == u'100201':
					market_noTEMP = 'BANK'
					security_typeTEMP = 'deposit'
				if tool.GetRE(row,'^(\d{4}).*')[0] == u'1102':
					security_typeTEMP = 'stock'
					security_codeTEMP = tool.GetRE(row,'.*(.{6})$')[0]
				if tool.GetRE(row,'^(\d{4}).*')[0] == u'1105':
					security_typeTEMP = 'fund'
					security_codeTEMP = tool.GetRE(row,'.*(.{6})$')[0]
				if tool.GetRE(row,'^(\d{6}).*') != []:
					if tool.GetRE(row,'^(\d{6}).*')[0] == u'110201':
						market_noTEMP = 'SH'
						windsecurity_codeTEMP = tool.GetRE(row,'.*(.{6})$')[0] + u'.SH'
					if tool.GetRE(row,'^(\d{6}).*')[0] == u'110231':
						market_noTEMP = 'SZ'
						submarket_noTEMP = 'SZ.A'
						windsecurity_codeTEMP = tool.GetRE(row,'.*(.{6})$')[0] + u'.SZ'
					if tool.GetRE(row,'^(\d{6}).*')[0] == u'110241':
						market_noTEMP = 'SZ'
						submarket_noTEMP = 'SZ.CYB'
						windsecurity_codeTEMP = tool.GetRE(row,'.*(.{6})$')[0] + u'.SZ'
					if tool.GetRE(row,'^(\d{6}).*')[0] == u'110503':
						windsecurity_codeTEMP = tool.GetRE(row,'.*(.{6})$')[0] + u'.OF'
						
				fieldDic['FUND_ID'] = FundID_PortID[0]
				fieldDic['PORT_ID'] = FundID_PortID[1]
				fieldDic['L_DATE'] = l_date
				fieldDic['ACCOUNT_ID'] = row
				fieldDic['ACCOUNT_NAME'] = df.loc[row][u'科目名称']
				fieldDic['MARKET_NO'] = market_noTEMP
				fieldDic['SUBMARKET_NO'] = submarket_noTEMP
				fieldDic['WINDSECURITY_CODE'] = windsecurity_codeTEMP
				fieldDic['SECURITY_CODE'] = security_codeTEMP
				fieldDic['SECURITY_TYPE'] = security_typeTEMP
				fieldDic['POSITION_FLAG'] = position_flagTEMP
				fieldDic['UNIT_COST'] = df.loc[row][u'单位成本']
				fieldDic['TOTAL_COST'] = df.loc[row][u'成本']
				fieldDic['MARKET_PRICE'] = df.loc[row][u'市价']
				fieldDic['MARKET_VALUE'] = df.loc[row][u'市值']
				fieldDic['PANDL'] = df.loc[row][u'估值增值']
				fieldDic['AMOUNT'] = df.loc[row][u'数量']
				fieldDic['NETASSET_PERCENT'] = df.loc[row][u'市值占净值%']
				selectConstraint = [FundID_PortID[0],FundID_PortID[1],str(int(l_date)-1),row]
				db = tool.connectDB()
				sql='''select AMOUNT from portHolding 
			            where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND l_date = {0[2]} AND ACCOUNT_ID = '{0[3]}' '''.format(selectConstraint)
				begin_amount =  tool.sqlSelect(sql,db)
				if begin_amount != []: 
					fieldDic['BEGIN_AMOUNT'] = begin_amount[0][0]
				else:
					fieldDic['BEGIN_AMOUNT'] = 0
				#print fieldDic['BEGIN_AMOUNT']
				fieldDic['BUY_AMOUNT'] = 0
				fieldDic['BUY_CASH'] = 0
				fieldDic['SALE_AMOUNT'] = 0
				fieldDic['SALE_CASH'] = 0
				if isinstance(fieldDic['BEGIN_AMOUNT'],float):
					difference = fieldDic['AMOUNT']-fieldDic['BEGIN_AMOUNT']
					#print difference
					if difference > 0:
						fieldDic['BUY_AMOUNT'] = difference
						fieldDic['BUY_CASH'] = difference * fieldDic['MARKET_PRICE']
					if difference < 0:
						fieldDic['SALE_AMOUNT'] = 0-difference
						fieldDic['SALE_CASH'] = -(difference * fieldDic['MARKET_PRICE'])
				else:
					fieldDic['BUY_AMOUNT'] = fieldDic['AMOUNT']
					fieldDic['BUY_CASH'] = fieldDic['AMOUNT'] * fieldDic['MARKET_PRICE']
				for col in colList:
					colDic[col + 'List'].append(fieldDic[col])
					
		data = {}
		for col in colList:
			data[col] = colDic[col + 'List']
		holdingDF = pd.DataFrame(data)
		#print holdingDF
		# if os.path.exists(self.backDirectoryName) == False:
			# os.mkdir((self.backDirectoryName))
		# fileBakPath=self.backDirectoryName
		# for file in listFiles:
			# shutil.move(sourceDirectory+file,self.backDirectoryName+file)
		return holdingDF
	
	def getValue(self,keyValue,col,indexs,df):
		i = 0
		row_num = 0
		for index in indexs:
			if index == keyValue:
				row_num = i
			i = i + 1
		row_series = df.iloc[row_num]
		value = tool.getElement(col,row_series)
		return value
	
	#删除指定的持仓数据
	def deleteReload(self,db,deleteConstraint):
		sql = '''insert into portHolding_bak
		                select * from portHolding
						        where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND l_date = {0[2]}'''.format(deleteConstraint)
		tool.sqlDML(sql,db)
		sql='''delete from portHolding 
			        where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND l_date = {0[2]}'''.format(deleteConstraint)
		tool.sqlDML(sql,db)
		print('Delete Finish')
		
	#检查持仓信息是否重载，对于重载的数据DATA_STATUS置为'0'，LCU和LCD与新插入的相应数据中的FCU和FCD相同
	def checkReload(self,db,selectConstraint):
		sql='''select * from portHolding 
			            where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND l_date = {0[2]} AND ACCOUNT_ID = '{0[3]}' '''.format(selectConstraint)
		rs = tool.sqlSelect(sql,db)
		if rs != []:
			sql='''update portHolding set LCD = sysdate, LCU = ora_login_user, DATA_STATUS = \'0\'
				        where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND l_date = {0[2]} AND ACCOUNT_ID = '{0[3]}' '''.format(selectConstraint)
			tool.sqlDML(sql,db)
			#print('Exist')
		else:
			pass
			#print('Not Exist')
		#print('Check Finish')
	
	#将从估值表中取出的持仓信息写入数据库中
	def insertToOracle(self,db,holdingDF):
		for i in holdingDF.index:
			fund_id = holdingDF.iloc[i]['FUND_ID']
			port_id = holdingDF.iloc[i]['PORT_ID']
			l_date = holdingDF.iloc[i]['L_DATE']
			account_id  = holdingDF.iloc[i]['ACCOUNT_ID']
			selectConstraint = [fund_id,port_id,l_date,account_id]
			self.checkReload(db,selectConstraint)
			insertList  = []
			for j in holdingDF.columns:
				value = holdingDF.iloc[i][j]
				if isinstance(value,unicode):
					value = value.encode('UTF-8')
				if type(value) == type(np.float64(0)): 
					value = value.item()
					if math.isnan(value):
						value = 'null'
				if type(value) == type(None):
					value = 'null'
				insertList.append(value)
			sql='''insert into portHolding
						(id,ACCOUNT_ID,ACCOUNT_NAME,AMOUNT,BEGIN_AMOUNT,BUY_AMOUNT,BUY_CASH,BUY_FEE,FUND_ID,L_DATE,MARKET_NO,MARKET_PRICE,MARKET_VALUE,NETASSET_PERCENT,NETTOTALASSET_PERCENT,PANDL,PORT_ID,POSITION_FLAG,SALE_AMOUNT,SALE_CASH,SALE_FEE,SECURITY_CODE,SECURITY_TYPE,SUBMARKET_NO,TOTAL_COST,UNIT_COST,WINDSECURITY_CODE,FCU,LCU)
						VALUES(S_portHolding.Nextval,'{0[0]}','{0[1]}',{0[2]},{0[3]},{0[4]},{0[5]},{0[6]},'{0[7]}','{0[8]}','{0[9]}',{0[10]},{0[11]},{0[12]},{0[13]},{0[14]},'{0[15]}','{0[16]}',{0[17]},{0[18]},{0[19]},'{0[20]}','{0[21]}','{0[22]}',{0[23]},{0[24]},'{0[25]}',ora_login_user,ora_login_user)'''.format(insertList)
			tool.sqlDML(sql,db)
			
	
	def run(self):
		holdingDF=self.getHoldingsReturn(self.sourceDirectoryName,self.sourceFileName)
		db = tool.connectDB()
		op = raw_input("Please enter operation:[insert or delete] \n")
		if op == 'insert':
			self.insertToOracle(db,holdingDF)
		elif op == 'delete':
			self.deleteReload(db,['F00001','P00001','20150618'])
		else:
			print('Wrong input')
		db.close()




if __name__ == "__main__":
	config = ConfigParser.ConfigParser()
	config.readfp(open('assets.ini'))
	holdingsClass=holdings()
	holdingsClass.run()