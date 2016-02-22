#-*- coding=utf-8 -*-

import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8' 
import matplotlib
from matplotlib import font_manager
matplotlib.use('Agg')
matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['axes.unicode_minus'] = False
import matplotlib.pyplot as plt
from matplotlib.pyplot import plot,savefig
import numpy as np
import scipy as sp
import statsmodels.api as sm
import pandas as pd
import WindPy as wind
from datetime import *
from ffn import *
import shutil
import xlsxwriter
import ConfigParser
import types
from xml.dom.minidom import parse
import xml.dom.minidom
import sys
import math

import tool


class performance():
	def __init__(self):
		self.prices=pd.DataFrame()
		self.benchmarkList=list()
		self.portList=list()
		self.benchmarkDF=pd.DataFrame()
		self.portDF=pd.DataFrame()
		config = ConfigParser.ConfigParser()
		config.readfp(open('performanceSetting.ini'))
		self.sourceDirectoryName=(config.get("source","sourceFileDirectory")).decode('UTF-8')
		self.sourceFileName=(config.get("source","sourceFileName").decode('UTF-8')).split(";")
		self.selectFOF=(config.get("source","selectFOF").decode('UTF-8')).split(";")
		self.destDirectoryName=(config.get("destination","destFileDirectory")).decode('UTF-8')
		self.destFileName=(config.get("destination","destFileName").decode('UTF-8')).split(";")
		self.startDate=(config.get("globalSetting","startDate")).decode('UTF-8')
		self.endDate=(config.get("globalSetting","endDate")).decode('UTF-8')
		self.windBenchmarkCode=(config.get("globalSetting","windBenchmarkCode")).decode('UTF-8')
		#self.perf 是 list[ ffn().PerformanceStats] ,例如 self.perf['000300.SH'] 就是ffn().PerformanceStats对象
		self.perf=None
		#engine3=create_engine('oracle://fdata:abc123@virtualPC:1521/orcl')
		#self.engine=engine3

	# 装载数据
	# data 输入格式：
	#               	aapl	msft
	# Date
	# 2010-01-04	28.466830	26.415914
	# 2010-01-05	28.516046	26.424448
	# 2010-01-06	28.062460	26.262284
	#
	def setupData(self,benchmarkDF,portDF):
		self.prices=pd.merge(benchmarkDF,portDF,left_index=True,right_index=True,how='inner')
		self.prices.fillna(method='ffill',inplace=True)

	def calcPerformanceIndexs(self):
		# ok now what about some performance metrics?
		perf = self.prices.calc_stats()
		#print(type(perf))
		#perf.display()
		self.perf=perf
		return perf # 无需返回内容

	# 取基准 收益率
	# 返回 基准的 收益率 DataFrame
	def getBenchmarkReturn(self,benchmarkCode,startDate,endDate):
		benchmarkDF=pd.DataFrame()
		wind.w.start()
		tmp=wind.w.wsd(benchmarkCode, "close", startDate, endDate, "Fill=Previous;PriceAdj=F")
		benchmarkDF[benchmarkCode]=tmp.Data[0]
		benchmarkDF['Date']=[datetime.strptime(x.strftime("%Y-%m-%d"),"%Y-%m-%d") for x in tmp.Times]
		benchmarkDF.index=benchmarkDF['Date']
		benchmarkDF.drop('Date',axis=1,inplace=True)
		self.benchmarkDF=benchmarkDF
		return benchmarkDF

	# 功能说明：将各个投资组合的收益率 组装成一个 portDF
	# 入参：
	#     各个组合的收益率的excel文件名，portDF1.xlsx,portDF2.xlsx等,...
	#返回：
	#             huabao1       shuangrong1  ...
	# 20140201
	# 20140202
	# 20140203
	def getPortsReturnXL(self,sourceDirectory,sourceFileName=None):
		#读取sourceDirectory下面的sourceFileName, 如果sourceFileName为空，则
		#遍历sourceDirectory下所有文件
		portDF=pd.DataFrame()
		sourceFileName = [x for x in sourceFileName if x != u'']
		listFiles = sourceFileName
		if sourceFileName == []:
			listFiles=os.listdir(sourceDirectory)
		first=True
		for file in listFiles:
			portDFTmp=pd.DataFrame()
			portDFTmp=pd.read_excel(sourceDirectory+file)
			portDFTmp.index=portDFTmp['Date']
			portDFTmp.drop('Date',axis=1,inplace=True)
			if first:
				portDF=portDFTmp
				#print(portDF)
			else:
				portDF=pd.merge(portDF,portDFTmp,left_index=True,right_index=True,how='outer')
			first=False
		#portDF.columns=[x.decode('UTF-8') for x in portDF.columns]
		return portDF
	
	#从Oracle中获取指定时间范围的当日单位净值，组合成一个port
	def getPortsReturnDB(self,period):
		DOMTree = xml.dom.minidom.parse("fund_port_rel.xml")
		mapping = DOMTree.documentElement
		funds = mapping.getElementsByTagName("fund")
		first=True
		for portName in self.selectFOF:
			FundID_PortID = tool.getFundID_PortID(funds,portName)
			#print(FundID_PortID)
			selectList = FundID_PortID + period
			#print(selectList)
			portSeries = self.selectOracle(selectList)
			data = {}
			data['Date'] = portSeries[0]
			data[portName] = portSeries[1]
			portDFTmp=pd.DataFrame(data)
			portDFTmp.index=portDFTmp['Date']
			portDFTmp.drop('Date',axis=1,inplace=True)
			if first:
				portDF=portDFTmp
				#print(portDF)
			else:
				portDF=pd.merge(portDF,portDFTmp,left_index=True,right_index=True,how='outer')
			first=False
		return portDF
	
	#单位净值的Oracle查询
	def selectOracle(self,selectList):
		db = tool.connectDB()
		#print(selectList)
		sql='''select l_date, unit_value from portAsset where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND DATA_STATUS = \'1\' AND l_date >= {0[2]} AND  l_date <= {0[3]} '''.format(selectList)
		rs=tool.sqlSelect(sql,db)
		dateList = []
		unit_valueList = []
		for x in rs:
			#print x
			dateList.append(x[0])
			unit_valueList.append(x[1])
		#print unit_valueList
		db.close()
		return [dateList,unit_valueList]
		
	
	# filePathList: 存放要保存图片的路径名，图片名字由performance包决定
	# printFlag:  Y: 表示打印到屏幕上
	#             N: 表示保存到文件中。
	def draw(self,printFlag='N',filePath=None):
		# we will rebase here to get a common starting point for both securities
		#ax = self.prices.rebase().plot()
		#print self.perf.display()

		#if filePath==None:
		#    filePath=self.workingFilePath

		if printFlag=='Y':
			self.prices.rebase().plot(figsize=(12,5))
		else:
			self.filePath=filePath #
			self.prices.rebase().plot(figsize=(12,5))
			filename='timeSeriesPic.png'
			fullname=filePath+filename
			# matplotlib.figure.test(fontproperties=myfont)
			savefig(fullname)
			#print 'finished plot prices picture'
			self.prices.to_drawdown_series().plot()
			filename='drawdown.png'
			fullname=filePath+filename
			savefig(fullname)
			#print 'finished print drawdown picture'
			#self.stats['spy'].display_monthly_returns()

	def chgFmt(self,statsDF):
		#更新statsDF 的格式，例如 %, 等等
		DOMTree = xml.dom.minidom.parse("viewConfig.xml")
		performance = DOMTree.documentElement
		indexs = performance.getElementsByTagName("index")

		indexList = []
		typeList = []
		displayList = []
		chineseList = []
		for index in indexs:
			type = index.getElementsByTagName('type')[0]
			display = index.getElementsByTagName('display')[0]
			language = index.getElementsByTagName('language')[0]
			chinese = language.getElementsByTagName('chinese')[0]
			typeList.append(type.childNodes[0].data)
			displayList.append(display.childNodes[0].data)
			chineseList.append(chinese.childNodes[0].data)
			indexList.append(index.getAttribute("content"))

		data = {'display' : displayList,
                'type' : typeList,
                'chinese' : chineseList}
		chgFmtDF = pd.DataFrame(data, index = indexList)
		statsChgFmtDF = pd.merge(statsDF,chgFmtDF,left_index=True,right_index=True,how='inner')
		# print(statsChgFmtDF)
		return statsChgFmtDF

	# 将所有绩效数据 存入 filePath 下面的fileName文件中
	def saveToFile(self,filePath=None,fileName='timeSeries.xlsx'):
		#self.prices.to_excel(u'D:\\05 FOF产品\\performance\\ifundResult.xlsx',sheet_name=u'华宝1号')
		#self.perf.to_csv(u'D:\\05 FOF产品\\performance\\ifundTmp.xlsx')
		#self.perf.to_csv(path=u"D:\\test10.csv")

		#fullname=filePath+u'indicators.csv'
		#self.perf.to_csv(path=fullname)


		#print 'finished to_csv'
		#写入到fileName中
		#self.prices.to_excel(fullname)
		# Create a Pandas Excel writer using XlsxWriter as the engine.
		#writer = pd.ExcelWriter(fullname, engine='xlsxwriter')
		#self.prices.to_excel(writer, sheet_name='Sheet1')
		# 写入统计数据
		xlstatsDF=pd.DataFrame()
		for x in self.perf:
			xlstatsDF['stat'+'_'+x]=self.perf[x].stats
		data = {}
		for x in xlstatsDF:
			#print(list(statsDF[x]))
			data[x] = list(xlstatsDF[x])
		statsChgFmtDF = self.chgFmt(xlstatsDF)
		chineseIndex = list(statsChgFmtDF['chinese'])
		xlstatsDF = pd.DataFrame(data, index = chineseIndex)
		i = 0
		deleteListNum = []
		deleteList = []
		for x in statsChgFmtDF['display']:
			if x == 'invisible':
				deleteListNum.append(i)
			i = i + 1
		for x in deleteListNum:
			#print(list(xlstatsDF.index)[x])
			deleteList.append(list(xlstatsDF.index)[x])
		xlstatsDF = xlstatsDF.drop(deleteList)
		#print(xlstatsDF)
		#print(xlstatsDF)
		#print(list(xlstatsDF.index))
		#print(xlstatsDF)
		#print(list(statsChgFmtDF['chinese']))
		#statsChgFmtDF = pd.DataFrame(statsChgFmtDF,list(statsChgFmtDF['chinese']))
		#print(statsChgFmtDF)
		# statsDF.index[0] = u'开始'
		#print(statsDF.index[0])
		#print statsDF

		# Create a Pandas Excel writer using XlsxWriter as the engine.
		writer = pd.ExcelWriter(filePath+fileName, engine='xlsxwriter',datetime_format='yyyy/mm/dd', date_format='yyyy/mm/dd')
		# Convert the dataframe to an XlsxWriter Excel object.
		perfclass.prices.to_excel(writer, sheet_name='Sheet1')
		#statsDF.to_excel(writer,sheet_name='statistics')
		worksheet = writer.sheets['Sheet1']
		worksheet.set_column(0,20,20)
		#colnum=len([x  for x in perfclass.prices])
		#rownum=len(perfclass.prices.iloc[:,0])
		colnum=0
		rownum=35
		#statsDF.to_excel(writer,sheet_name='Sheet2',startrow=0,startcol=(colnum+2))
		xlstatsDF.to_excel(writer,sheet_name='Sheet2',startrow=0,startcol=0)
		xlstatsDFLen=len([x  for x in xlstatsDF.columns])+1

		# Get the xlsxwriter objects from the dataframe writer object.
		workbook  = writer.book
		worksheet = writer.sheets['Sheet2']
		
		format = workbook.add_format()
		#format.set_bold()
		#format.set_font_color('red')
		startrow=rownum+5
		worksheet.insert_image('A'+str(startrow), filePath+u'timeSeriesPic.png')
		worksheet.insert_image('A'+str(startrow+25), filePath+u'drawdown.png')

		format_percent = workbook.add_format({'num_format': '0.00%'})
		format_float = workbook.add_format({'num_format': '0.000'})
		
		
		
		leftPoint_colnum=1
		leftPoint_rownum=1
		#样例
		#worksheet.set_column(0, 0, 20)   # Column  A   width set to 20.
		#worksheet.set_column(1, 3, 30)   # Columns B-D width set to 30.
		worksheet.set_column(0,(2+xlstatsDFLen),20,format_percent)
		
		#
		#worksheet.set_row(0, 20, cell_format) ,row (int) – The worksheet row (zero indexed); 20: height
		#
		i = 1
		for x in statsChgFmtDF['type']:
			if x == 'float':
				worksheet.set_row(i,None,format_float)
			i = i + 1
		
		'''
		worksheet.set_row(6-1,None,format_float)
		worksheet.set_row(14-1,None,format_float)
		# 隐藏部分行
		worksheet.set_row(13-1, None, None, {'hidden': True})
		worksheet.set_row(14-1, None, None, {'hidden': True})
		'''
		# Set the column width and format.
		worksheet.set_column('A:A', 18)
		worksheet.set_column(1,20,20)
		#worksheet2 = writer.sheets['statistics']
		#worksheet2.set_column(0,4,20)
		
		writer.save()
		writer.close()
		#os.rename(filePath+u'test.xlsx',filePath+fileName)
		
	
	#删除指定的绩效结果	
	def deleteReload(self,db,deleteConstraint):
		sql = '''insert into performance_statics_bak
		                select * from performance_statics
						        where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND start_date = {0[2]} AND end_date = {0[3]}'''.format(deleteConstraint)
		tool.sqlDML(sql,db)
		sql='''delete from performance_statics 
			        where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND start_date = {0[2]} AND end_date = {0[3]}'''.format(deleteConstraint)
		tool.sqlDML(sql,db)
		print('Delete Finish')
		
	#检查绩效是否重载，对于重载的数据DATA_STATUS置为'0'，LCU和LCD与新插入的相应数据中的FCU和FCD相同
	def checkReload(self,db,selectConstraint):
		sql='''select * from performance_statics 
			            where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND start_date = {0[2]} AND end_date = {0[3]} AND indicator_name = '{0[4]}' '''.format(selectConstraint)
		rs = tool.sqlSelect(sql,db)
		#print(rs)
		if rs != []:
			sql='''update performance_statics set LCD = sysdate, LCU = ora_login_user, DATA_STATUS = \'0\'
				        where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND start_date = {0[2]} AND end_date = {0[3]} AND indicator_name = '{0[4]}' '''.format(selectConstraint)
			tool.sqlDML(sql,db)
			print('Exist')
		else:
			print('Not Exist')
		print('Check Finish')	
		
	#将所有绩效存入oracle 中
	def saveToDb(self):
		#engine0=create_engine('oracle://fdata:abc123@T-wuc:1521/orcl')
		#engine2=create_engine('oracle://fdata:abc123@localhost:1521/orcl')
		#engine3=create_engine('oracle://fdata:abc123@virtualPC:1521/orcl')
		#self.engine=engine3
		
		db = tool.connectDB()
		statsDF=pd.DataFrame()
		for x in self.perf:
			statsDF[x]=self.perf[x].stats
		print statsDF
		
		DOMTree = xml.dom.minidom.parse("fund_port_rel.xml")
		mapping = DOMTree.documentElement
		funds = mapping.getElementsByTagName("fund")
		for col in statsDF.columns:
			
			for index in statsDF.index:
				if index != 'start' and index != 'end' :
					insertList = []
					FundID_PortID = tool.getFundID_PortID(funds,col)
					insertList.append(FundID_PortID[0])
					insertList.append(FundID_PortID[1])
					insertList.append(tool.getDate(str(statsDF.loc['start'][col]),'(\d{4}-\d{2}-\d{2}).*'))
					insertList.append(tool.getDate(str(statsDF.loc['end'][col]),'(\d{4}-\d{2}-\d{2}).*'))
					insertList.append(index)
					selectConstraint = [insertList[0],insertList[1],insertList[2],insertList[3],insertList[4]]
					#self.checkReload(db,selectConstraint)
					DOMTree = xml.dom.minidom.parse("viewConfig.xml")
					performance = DOMTree.documentElement
					indexs = performance.getElementsByTagName("index")
					for i in indexs:
						if index == i.getAttribute("content"):
							language = i.getElementsByTagName('language')[0]
							chinese = language.getElementsByTagName('chinese')[0]
							insertList.append((chinese.childNodes[0].data).encode("utf-8"))
							#print((chinese.childNodes[0].data).decode('utf-8'))
					statics_value = statsDF.loc[index][col]
					if type(statics_value) == type(np.float64(0)): 
						statics_value = statics_value.item()
					if math.isnan(statics_value):
						statics_value = 'null'
					
					insertList.append(statics_value)
					sql='''insert into performance_statics  
							(id,fund_id,port_id,start_date,end_date,indicator_name,chinese_name,statics_value,FCU,LCU)
							VALUES(S_performance_statics.Nextval,'{0[0]}','{0[1]}','{0[2]}','{0[3]}','{0[4]}','{0[5]}',{0[6]},ora_login_user,ora_login_user)'''.format(insertList)
					tool.sqlDML(sql,db)
				
				#i = statsDF.loc[index][col]
				#i = statsDF[statsDF.index == index][col]
				#print(list(i)[0])
				
		#self.deleteReload(db,['F00001','P00001','20150521','20150529'])	
		
		db.close()		
		#db = create_engine('oracle://perfdata:perfdata123@localhost:1521/orcl')
		#db.echo = False
		#metadata = MetaData(db)
		
		#pf_statics = Table('performance_statics', metadata, autoload=True)
		#i = pf_statics.insert()
		#i.execute(id=6,fund_id='022',port_id='003',cagr=30.11)
		
		#statsDF.to_sql(symbolFactorICDB[['tdate','symbolCode','factorName','standarize','close','D_1','D_5','D_10']],
		#           'factorIC',self.engine,index=False,if_exists='append')
		#print(statsDF)

	def getPerformaceIndexs(self):
		for x in self.perf:
			# self.perf['000300.SH'] 是PerformanceStats对象，该对象有如下属性：
			#    Attributes:
			#       * name (str): Name, derived from price series name
			#       * return_table (DataFrame): A table of monthly returns with
			#   YTD figures as well.
			#       * lookback_returns (Series): Returns for different
			#   lookback periods (1m, 3m, 6m, ytd...)
			#       * stats (Series): A series that contains all the stats
			self.perf['000300.SH'].avg_down_month

	#  filepath: 必须是 "d:\\aaa\\bbb\\" ，即最后是\\，表示是目录名字
	# filename: 必须是 xlsx，即excel文件格式。
	#
	def drawAndSave(self,filePath=None,fileName=u'timeSeries.xlsx'):
		# 注意 os 模块如要支持中文路径和 文件名，则字符串前要加前缀 'u'
		if isinstance(filePath,unicode):
			filePathUTF=filePath
		else:
			# 不是utf-8编码，则转换格式
			filePathUTF=filePath.decode('utf-8')
		if  isinstance(fileName,unicode):
			fileNameUTF=fileName
		else:
			fileNameUTF=fileName.decode('utf-8')

		if os.path.exists(filePathUTF+u'tmp'):
			shutil.rmtree(filePathUTF+u'tmp')
		os.mkdir((filePathUTF+u'tmp'))
		fileTmpPath=filePathUTF+u'tmp\\'
		self.draw('N',fileTmpPath)
		self.saveToFile(fileTmpPath,fileNameUTF)
		if os.path.isfile(filePathUTF+fileNameUTF):
			os.remove(filePathUTF+fileNameUTF)
		shutil.move(fileTmpPath+fileNameUTF,filePathUTF+fileNameUTF)
		
	def run(self):
		benchmarkDF=self.getBenchmarkReturn(self.windBenchmarkCode,self.startDate,self.endDate)
		chooseSourse = raw_input("Please enter source location:[Excel or Oracle] \n")
		if chooseSourse == 'Excel':
			portDF=self.getPortsReturnXL(self.sourceDirectoryName,self.sourceFileName)
		elif chooseSourse == 'Oracle':
			portDF=self.getPortsReturnDB(['20150521','20150627'])
		else:
			print 'Wrong Input'
			sys.exit('Function completed')
		self.setupData(benchmarkDF,portDF)
		self.calcPerformanceIndexs()
		destFileNameStr=self.destFileName[0]
		self.drawAndSave(self.destDirectoryName,fileName=destFileNameStr)
		self.saveToDb()
	
if __name__ == "__main__":
	perfclass=performance()
	#暂时屏蔽
	perfclass.run()
	# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	# 方法二
	#startDate='20140101'
	#endDate='20151231'
	#benchmarkCode="000300.SH"
	#benchmarkDF=perfclass.getBenchmarkReturn(benchmarkCode,startDate,endDate)

	#portDF=pd.read_excel(u'D:\\05 FOF产品\\performance\\ifund.xlsx')
	#portDF.index=portDF['Date']
	#portDF.drop('Date',axis=1,inplace=True)

	#perfclass.setupData(benchmarkDF,portDF)
	#perfindexs=perfclass.calcPerformanceIndexs()
	#filePath=u'D:\\05 FOF产品\\performance\\'
	#perfclass.draw('Y')
	#perfclass.saveToFile(filePath=filePath,fileName='ifundResult.xlsx')
	#perfclass.drawAndSave(filePath,fileName='ifundResult.xlsx')



