#!/usr/bin/python
# -*- coding: UTF-8 -*-

import ConfigParser
import os
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


class assets():
	def __init__(self):
		self.sourceDirectoryName=(config.get("source","sourceFileDirectory")).decode('UTF-8')
		self.sourceFileName=(config.get("source","sourceFileName").decode('UTF-8')).split(";")
		self.backDirectoryName=(config.get("source","backFileDirectory")).decode('UTF-8')
	
	#从assets文件夹读出估值表中投资组合的资产信息
	def getAssetsReturn(self,sourceDirectory,sourceFileName=None):
		assetDF=pd.DataFrame()
		sourceFileName = [x for x in sourceFileName if x != u'']
		listFiles = sourceFileName
		if sourceFileName == []:
			listFiles=os.listdir(sourceDirectory)
		#print sourceDirectory
		#print sourceFileName
		#first=True
		sectionList = config.sections()
		indexList = config.options(sectionList[1])[4:]
		indexDic = {}
		for index in indexList:
			indexDic[index + 'List'] = []
		for file in listFiles:
			fieldDic = {}
			for sectionName in sectionList:
				if tool.GetRE(file, '^(.*?)_.*')[0] == sectionName.decode('UTF-8'):
					configSection = sectionName
					portName = sectionName.decode('UTF-8')
					#print(tool.GetRE(file, '^(.*?)_.*')[0])
				if tool.GetRE(file, '.*_(.+)_.*')[0] == sectionName.decode('UTF-8'):
					configSection = sectionName
					portName = sectionName.decode('UTF-8')
					#print(tool.GetRE(file, '.*_(.+)_.*')[0])
			df = pd.read_excel(sourceDirectory+file, sheetname=tool.getConfig(config,configSection,'sheet'), header = 3)
			indexs = list(df[tool.getConfig(config,configSection,'index')])
			marketValue = tool.getConfig(config,configSection,'marketValue')
			subjectName = tool.getConfig(config,configSection,'subjectName')
			DOMTree = xml.dom.minidom.parse("fund_port_rel.xml")
			mapping = DOMTree.documentElement
			funds = mapping.getElementsByTagName("fund")
			FundID_PortID = tool.getFundID_PortID(funds,portName)
			for index in indexList:
				if index == 'fund_id':
					fieldDic[index] = FundID_PortID[0]
				elif index == 'port_id':
					fieldDic[index] = FundID_PortID[1]
				elif index == 'l_date':
					fieldDic[index] = tool.getDate(file,'.*(\d{4}-\d{2}-\d{2}).xls')
				elif index == 'futures_asset':
					fieldDic[index] = None 
				elif index == 'accumulate_profit' or index == 'allocatble_profit' or index == 'accumulate_unit_value' or index == 'unit_value_yesterday' or index == 'unit_value':
					fieldDic[index] = self.getValue(tool.getConfig(config,configSection,index),subjectName,indexs,df)
				elif index != 'net_assets':
					fieldDic[index] = self.getValue(tool.getConfig(config,configSection,index),marketValue,indexs,df)
				else:
					fieldDic[index] = self.getValue(tool.getConfig(config,configSection,'total_assets'),marketValue,indexs,df)-self.getValue(tool.getConfig(config,configSection,'credit_value'),marketValue,indexs,df)
				indexDic[index + 'List'].append(fieldDic[index])
		data = {}
		for index in indexList:
			data[index] = indexDic[index + 'List']
		assetDF = pd.DataFrame(data)
		#print(assetDF)
		if os.path.exists(self.backDirectoryName) == False:
			os.mkdir((self.backDirectoryName))
		fileBakPath=self.backDirectoryName
		for file in listFiles:
			shutil.move(sourceDirectory+file,self.backDirectoryName+file)
		return assetDF
			
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
	
	#删除指定的资产数据
	def deleteReload(self,db,deleteConstraint):
		sql = '''insert into portAsset_bak
		                select * from portAsset
						        where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND l_date = {0[2]}'''.format(deleteConstraint)
		tool.sqlDML(sql,db)
		sql='''delete from portAsset 
			        where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND l_date = {0[2]}'''.format(deleteConstraint)
		tool.sqlDML(sql,db)
		print('Delete Finish')
		
	#检查资产信息是否重载，对于重载的数据DATA_STATUS置为'0'，LCU和LCD与新插入的相应数据中的FCU和FCD相同
	def checkReload(self,db,selectConstraint):
		sql='''select * from portAsset 
			            where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND l_date = {0[2]}'''.format(selectConstraint)
		rs = tool.sqlSelect(sql,db)
		#print(rs)
		if rs != []:
			sql='''update portAsset set LCD = sysdate, LCU = ora_login_user, DATA_STATUS = \'0\'
				        where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND l_date = {0[2]}'''.format(selectConstraint)
			tool.sqlDML(sql,db)
			print('Exist')
		else:
			print('Not Exist')
		print('Check Finish')
	
	#将从估值表中取出的资产信息写入数据库中
	def insertToOracle(self,db,assetDF):
		for i in assetDF.index:
			insertList  = []
			fund_id = assetDF.iloc[i]['fund_id']
			port_id = assetDF.iloc[i]['port_id']
			l_date = assetDF.iloc[i]['l_date']
			selectConstraint = [fund_id,port_id,l_date]
			self.checkReload(db,selectConstraint)
			for j in assetDF.columns:
				#print(i)
				#print(j)
				value = assetDF.iloc[i][j]
				if j != 'fund_id' and j != 'port_id' and j != 'l_date' and j != 'futures_asset':
					if type(value) == type(np.float64(0)): 
						value = value.item()
						if math.isnan(value):
							value = 'null'
					if type(value) == type(u''):
						value = float(value.replace(',',''))
				if j == 'futures_asset':
					value = 'null'
				if type(value) == type(u''):
					value = str(value)
				insertList.append(value)
			sql='''insert into portAsset 
						(id,accumulate_profit,accumulate_unit_value,allocatble_profit,begin_cash,bond_asset,credit_value,current_cash_eod,deposit_asset,fund_asset,fund_id,futures_asset,l_date,net_assets,other_currency_asset,port_id,repo_asset,security_settlement_cash,stock_asset,total_assets,unit_value,unit_value_yesterday,FCU,LCU)
						VALUES(S_portAsset.Nextval,{0[0]},{0[1]},{0[2]},{0[3]},{0[4]},{0[5]},{0[6]},{0[7]},{0[8]},'{0[9]}',{0[10]},{0[11]},{0[12]},{0[13]},'{0[14]}',{0[15]},{0[16]},{0[17]},{0[18]},{0[19]},{0[20]},ora_login_user,ora_login_user)'''.format(insertList)
			tool.sqlDML(sql,db)
		
	def run(self):
		assetDF=self.getAssetsReturn(self.sourceDirectoryName,self.sourceFileName)
		db = tool.connectDB()
		op = raw_input("Please enter operation:[insert or delete] \n")
		if op == 'insert':
			self.insertToOracle(db,assetDF)
		elif op == 'delete':
			self.deleteReload(db,['F00001','P00001','20150522'])
		else:
			print('Wrong input')
		db.close()
	
if __name__ == "__main__":
	config = ConfigParser.ConfigParser()
	config.readfp(open('assets.ini'))
	assetsClass=assets()
	assetsClass.run()
	