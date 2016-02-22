#!/usr/bin/python
# -*- coding: UTF-8 -*-
#author: github.com/Huisong-Li
#desc: holding_engine
#-----------------------------
#2016-01-28 created
#-----------------------------


import sqlalchemy
from sqlalchemy import *
from sqlalchemy.orm import *
import pandas as pd
from pandas import Series, DataFrame
import xlsxwriter
import ConfigParser
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import WindPy as wind
from datetime import *
from xml.dom.minidom import parse
import xml.dom.minidom

import tool


class holdings_pf():

	def __init__(self):
		self.db = create_engine('oracle://perfdata:perfdata123@localhost:1521/orcl')
		config = ConfigParser.ConfigParser()
		config.readfp(open('performanceSetting.ini'))
		self.filePath = (config.get("destination", "destFileDirectory")).decode('UTF-8')
		self.fileName = (config.get("destination","destFileName").decode('UTF-8')).split(";")[0]
		self.selectList = ['F00001', 'P00001', '20150521', '20150522', 10]
		DOMTree = xml.dom.minidom.parse("fund_port_rel.xml")
		mapping = DOMTree.documentElement
		self.funds = mapping.getElementsByTagName("fund")
		self.titleList = [tool.getPortName(self.funds,self.selectList[1]),self.selectList[2],self.selectList[3]]
		
	# 组合十大权重股表	
	def top10Stocks(self):
		sql = '''
              select * FROM
              (SELECT SECURITY_CODE as 股票代码,ACCOUNT_NAME as 股票名称,SUM(NETTOTALASSET_PERCENT) as 持仓占比,AVG(NETTOTALASSET_PERCENT) as 平均持仓占比,MAX(NETTOTALASSET_PERCENT) as 最大持仓占比,MIN(NETTOTALASSET_PERCENT) as 最小持仓占比 FROM Portholding
              WHERE FUND_ID = '{0[0]}' AND PORT_ID = '{0[1]}' AND L_DATE BETWEEN '{0[2]}' AND '{0[3]}'  AND SECURITY_TYPE = 'stock' AND DATA_STATUS = '1'
              GROUP BY SECURITY_CODE,ACCOUNT_NAME
              ORDER BY SUM(NETTOTALASSET_PERCENT) DESC)
              WHERE ROWNUM <= 10
              '''.format(self.selectList)
				
		df = pd.read_sql(sql, self.db)
		df.index=df[u'股票代码']
		df.drop(u'股票代码',axis=1,inplace=True)
		return df
		
	# 组合行业市值与权重表 
	def industryValue_Weight(self):
		pass
		
	# 组合日收益率与净值表 
	def dailyReturn_netValue(self):
		sql = '''
              select a.L_DATE,a.TOTAL_ASSETS as 组合总市值,a.UNIT_VALUE as 组合日净值, a.return as 组合日收益率, a.STOCK_ASSET as 股票持仓市值, a.stock_percent as 股票仓位占比, a.cash as 现金金额, a.cash_percent as 现金仓位占比, b.position/a.TOTAL_ASSETS as 当日加减仓比例 from
              ((select L_DATE, FUND_ID, PORT_ID,UNIT_VALUE, TOTAL_ASSETS, ((UNIT_VALUE-UNIT_VALUE_YESTERDAY)/UNIT_VALUE_YESTERDAY)*100 as return, STOCK_ASSET, (STOCK_ASSET/TOTAL_ASSETS) as stock_percent, CURRENT_CASH_EOD as cash, (CURRENT_CASH_EOD/TOTAL_ASSETS) as cash_percent from portasset
              where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1')a
              left join
              (select L_DATE, FUND_ID, PORT_ID, SUM(BUY_CASH-SALE_CASH) as position from Portholding
              where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1'
              group by L_DATE, FUND_ID, PORT_ID)b
              on a.L_DATE = b.L_DATE and a.FUND_ID  = b.FUND_ID and a.PORT_ID = b.PORT_ID)
			  order by L_DATE
              '''.format(self.selectList)
		dfTMP = pd.read_sql(sql, self.db)
		dfTMP.index=dfTMP['l_date']
		dfTMP.drop('l_date',axis=1,inplace=True)
		benchmarkDF=self.getBenchmarkReturn('000300.SH', self.selectList[2], self.selectList[3])
		df = pd.merge(dfTMP, benchmarkDF, left_index=True,right_index=True,how='inner')
		df[u'日超额收益率'] = df[u'组合日收益率']-df[u'日基准收益率']
		return df	
		
	# 组合持仓查询
	def selectHolding(self):
		sql = '''
		      select L_DATE, SECURITY_CODE as 股票代码, ACCOUNT_NAME as 股票名称, AMOUNT as 持仓 from Portholding
			  where SECURITY_TYPE = 'stock' and FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}'
			  order by L_DATE
			  '''.format(self.selectList)
		df = pd.read_sql(sql, self.db)
		df.index=df['l_date']
		df.drop('l_date',axis=1,inplace=True)
		return df
	
	# 组合期间排名查询
	## 1.涨跌幅SQL优化
	def selectRank(self):
		# 跌幅 
		## 找到每个股票的区间时间序列的第一个和最后一个
		##（1+（T）return * 1 + （T+1）return ... 1 +（T+n）return）- 1 
		sql_decrease = '''  
		               select * FROM
		               (select a.SECURITY_CODE as 股票代码, a.ACCOUNT_NAME as 股票名称, ((b.MARKET_PRICE-a.MARKET_PRICE)/a.MARKET_PRICE) as 幅度 from (
                       (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, MARKET_PRICE  from portholding
                       where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE = '{0[2]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock' )a
                       left join
                       (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, MARKET_PRICE  from portholding
                       where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE = '{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock' )b
                       on a.SECURITY_CODE = b.SECURITY_CODE  )
                       order by 幅度)
					   WHERE ROWNUM <= {0[4]}
                       '''.format(self.selectList)
		# 涨幅
		## 找到每个股票的区间时间序列的第一个和最后一个
		##（1+（T）return * 1 + （T+1）return ... 1 +（T+n）return）- 1 
		sql_increase = '''  
		               select * FROM
		               (select a.SECURITY_CODE as 股票代码, a.ACCOUNT_NAME as 股票名称, ((b.MARKET_PRICE-a.MARKET_PRICE)/a.MARKET_PRICE) as 幅度 from (
                       (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, MARKET_PRICE  from portholding
                       where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE = '{0[2]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock' )a
                       left join
                       (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, MARKET_PRICE  from portholding
                       where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE = '{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock' )b
                       on a.SECURITY_CODE = b.SECURITY_CODE  )
                       order by 幅度 DESC)
					   WHERE ROWNUM <= {0[4]}
                       '''.format(self.selectList)
		# PANDL 跌
		sql_pandlDe = '''
		              select * FROM
		              (select SECURITY_CODE as 股票代码, ACCOUNT_NAME as 股票名称, sum(PANDL) as 金额 from
                      (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, PANDL  from portholding
                      where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                      group by SECURITY_CODE, ACCOUNT_NAME
                      order by 金额)
					  WHERE ROWNUM <= {0[4]}
		              '''.format(self.selectList)
		# PANDL 涨
		sql_pandlIn = '''
		              select * FROM
		              (select SECURITY_CODE as 股票代码, ACCOUNT_NAME as 股票名称, sum(PANDL) as 金额 from
                      (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, PANDL  from portholding
                      where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                      group by SECURITY_CODE, ACCOUNT_NAME
                      order by 金额 DESC)
					  WHERE ROWNUM <= {0[4]}
		              '''.format(self.selectList)	  
		
		# 减仓金额
		sql_positionDe = '''
		                 select * FROM
		                 (select SECURITY_CODE as 股票代码, ACCOUNT_NAME as 股票名称, sum(BUY_CASH)-sum(SALE_CASH) as 金额 from
                         (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, BUY_CASH, SALE_CASH  from portholding
                         where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}'  and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                         group by SECURITY_CODE, ACCOUNT_NAME
                         order by 金额)
						 WHERE ROWNUM <= {0[4]}
			             '''.format(self.selectList)
		
		# 增仓金额
		sql_positionIn = '''
		                 select * FROM
		                 (select SECURITY_CODE as 股票代码, ACCOUNT_NAME as 股票名称, sum(BUY_CASH)-sum(SALE_CASH) as 金额 from
                         (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, BUY_CASH, SALE_CASH  from portholding
                         where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}'  and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                         group by SECURITY_CODE, ACCOUNT_NAME
                         order by 金额 DESC)
						 WHERE ROWNUM <= {0[4]}
		                 '''.format(self.selectList)
		
		selectRankDic = {}
		selectRankDic['decrease'] = pd.read_sql(sql_decrease, self.db)
		selectRankDic['decrease'].index=selectRankDic['decrease'][u'股票代码']
		selectRankDic['decrease'].drop(u'股票代码',axis=1,inplace=True)
		selectRankDic['increase'] = pd.read_sql(sql_increase, self.db)
		selectRankDic['increase'].index=selectRankDic['increase'][u'股票代码']
		selectRankDic['increase'].drop(u'股票代码',axis=1,inplace=True)
		selectRankDic['pandlDe'] = pd.read_sql(sql_pandlDe, self.db)
		selectRankDic['pandlDe'].index=selectRankDic['pandlDe'][u'股票代码']
		selectRankDic['pandlDe'].drop(u'股票代码',axis=1,inplace=True)
		selectRankDic['pandlIn'] = pd.read_sql(sql_pandlIn, self.db)
		selectRankDic['pandlIn'].index=selectRankDic['pandlIn'][u'股票代码']
		selectRankDic['pandlIn'].drop(u'股票代码',axis=1,inplace=True)
		selectRankDic['positionDe'] = pd.read_sql(sql_positionDe, self.db)
		selectRankDic['positionDe'].index=selectRankDic['positionDe'][u'股票代码']
		selectRankDic['positionDe'].drop(u'股票代码',axis=1,inplace=True)
		selectRankDic['positionIn'] = pd.read_sql(sql_positionIn, self.db)
		selectRankDic['positionIn'].index=selectRankDic['positionIn'][u'股票代码']
		selectRankDic['positionIn'].drop(u'股票代码',axis=1,inplace=True)
		return selectRankDic

	# 对冲损益查询(损益)
	def hedgeProfit(self):
		sql = '''
              select a.L_DATE, b.STOCK_ASSET as 对冲前组合市值 , (a.future+b.STOCK_ASSET) as 对冲后组合市值, a.future as 期货头寸, (a.future+b.STOCK_ASSET) as 风险敞口 from
              ((select FUND_ID, PORT_ID, L_DATE, SUM(MARKET_VALUE) as future from portholding
              where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and SECURITY_TYPE = 'future' and POSITION_FLAG = 'short' and DATA_STATUS = '1'
              group by FUND_ID, PORT_ID ,L_DATE)a
              left join
              (select STOCK_ASSET, FUND_ID, PORT_ID ,L_DATE from Portasset
              where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1')b
              on a.FUND_ID = b.FUND_ID and a.PORT_ID = b.PORT_ID and a.L_DATE = b.L_DATE)
              '''.format(self.selectList)
		df = pd.read_sql(sql, self.db)
		df.index=df['l_date']
		df.drop('l_date',axis=1,inplace=True)
		return df
		
	# 对冲损益查询(标准差)
	def hedgeProfit_stddev(self):
		sql = '''
              select stddev(a.before) as 对冲前组合 ,stddev(a.afer) as 对冲后组合 from
              (select a.L_DATE,a.FUND_ID, a.PORT_ID, b.STOCK_ASSET as before , (a.future+b.STOCK_ASSET) as afer, a.future as future, (a.future+b.STOCK_ASSET) as window from
              ((select FUND_ID, PORT_ID, L_DATE, SUM(MARKET_VALUE) as future from portholding
              where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and SECURITY_TYPE = 'future' and POSITION_FLAG = 'short' and DATA_STATUS = '1'
              group by FUND_ID, PORT_ID ,L_DATE)a
              left join
              (select STOCK_ASSET, FUND_ID, PORT_ID ,L_DATE from Portasset
              where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1')b
              on a.FUND_ID = b.FUND_ID and a.PORT_ID = b.PORT_ID and a.L_DATE = b.L_DATE))a
              '''.format(self.selectList)
		df = pd.read_sql(sql, self.db)
		df.index=df[u'对冲前组合']
		df.drop(u'对冲前组合',axis=1,inplace=True)
		return df
		
	# 组合行业换手率
	def industryTR(self):
		pass	
	
	# 取基准 收益率
	# 返回 基准的 收益率 DataFrame
	def getBenchmarkReturn(self,benchmarkCode,startDate,endDate):
		benchmarkDF=pd.DataFrame()
		wind.w.start()
		tmp=wind.w.wsd(benchmarkCode, "pct_chg", startDate, endDate, "Fill=Previous;PriceAdj=F")
		benchmarkDF[u'日基准收益率']=tmp.Data[0]
		benchmarkDF['Date']=[str(datetime.strptime(x.strftime("%Y-%m-%d"),"%Y-%m-%d"))[0:10].replace('-','') for x in tmp.Times]
		#print str(benchmarkDF['Date'][0])[0:10].replace('-','')
		benchmarkDF.index=benchmarkDF['Date']
		benchmarkDF.drop('Date',axis=1,inplace=True)
		self.benchmarkDF=benchmarkDF
		return benchmarkDF
		
	# 将要写入EXCEL文件的dataframe组成一个列表
	def combine(self):
		insertDic = {}
		insertDic['top10Stocks'] = self.top10Stocks()
		insertDic['industryValue_Weight'] = self.industryValue_Weight()
		insertDic['dailyReturn_netValue'] = self.dailyReturn_netValue()
		insertDic['selectHolding'] = self.selectHolding()
		insertDic['selectRank_decrease'] = self.selectRank()['decrease']
		insertDic['selectRank_increase'] = self.selectRank()['increase']
		insertDic['selectRank_pandlDe'] = self.selectRank()['pandlDe']
		insertDic['selectRank_pandlIn'] = self.selectRank()['pandlIn']
		insertDic['selectRank_positionDe'] = self.selectRank()['positionDe']
		insertDic['selectRank_positionIn'] = self.selectRank()['positionIn']
		insertDic['hedgeProfit'] = self.hedgeProfit()
		insertDic['hedgeProfit_stddev'] = self.hedgeProfit_stddev()
		insertDic['industryTR'] = self.industryTR()
		return insertDic
		
	# 将持仓指标存入EXCEL文件
	## 1.在已经存在EXCEL中插入新的SHEET页
	## 2.按照需求文档1.2中的报表样式写入
	def save_file(self, insertDic):
		#try:
			writer = pd.ExcelWriter(self.filePath + self.fileName, engine='xlsxwriter')
			workbook = writer.book
			bold = workbook.add_format({'bold': True})
			format=workbook.add_format()
			format.set_border(1)
			date_dif = int(self.selectList[3])-int(self.selectList[2])
			merge_format = workbook.add_format({'bold': True,'align':'center','valign':'vcenter'})
			# 组合十大权重股表
			insertDic['top10Stocks'].to_excel(writer, sheet_name=u'组合十大权重股表', startrow=4, startcol=1)
			worksheet = writer.sheets[u'组合十大权重股表']
			worksheet.write(2,1, u'组合十大权重股表({0[0]},{0[1]}到{0[2]})'.format(self.titleList), bold)
			worksheet.conditional_format(4,1,14,6, {'type': 'blanks','format':format})
			worksheet.conditional_format(4,1,14,6, {'type': 'no_blanks','format':format})
			worksheet.set_column(1, 6, 12)
			# 组合日收益率与净值表
			insertDic['dailyReturn_netValue'].to_excel(writer, sheet_name=u'组合日收益率与净值表', startrow=4, startcol=1)
			worksheet = writer.sheets[u'组合日收益率与净值表']
			worksheet.write(2,1, u'组合日收益率与净值表({0[0]},{0[1]}到{0[2]})'.format(self.titleList), bold)
			worksheet.conditional_format(4,1,4+date_dif+1,11, {'type': 'blanks','format':format})
			worksheet.conditional_format(4,1,4+date_dif+1,11, {'type': 'no_blanks','format':format})
			worksheet.set_column(1, 11, 14)
			# 组合持仓查询
			insertDic['selectHolding'].to_excel(writer, sheet_name=u'组合持仓查询', startrow=4, startcol=1)
			indexInt = len(insertDic['selectHolding'].index)
			worksheet = writer.sheets[u'组合持仓查询']
			worksheet.write(2,1, u'组合持仓查询({0[0]},{0[1]}到{0[2]})'.format(self.titleList), bold)
			worksheet.conditional_format(4,1,14,4, {'type': 'blanks','format':format})
			worksheet.conditional_format(4,1,4 + indexInt,4, {'type': 'no_blanks','format':format})
			worksheet.set_column(1, 4, 12)
			# 组合期间排名查询
			insertDic['selectRank_decrease'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=1)
			worksheet = writer.sheets[u'组合期间排名查询']
			worksheet.write(2,1, u'组合期间排名查询({0[0]},{0[1]}到{0[2]})'.format(self.titleList), bold)
			worksheet.merge_range(4,1,4,3,u'涨幅前{0[4]}名'.format(self.selectList),merge_format)
			insertDic['selectRank_increase'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=4)
			worksheet.merge_range(4,4,4,6,u'跌幅前{0[4]}名'.format(self.selectList),merge_format)
			insertDic['selectRank_pandlDe'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=7)
			worksheet.merge_range(4,7,4,9,u'亏损额最大前{0[4]}名'.format(self.selectList),merge_format)
			insertDic['selectRank_pandlIn'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=10)
			worksheet.merge_range(4,10,4,12,u'亏损额最小前{0[4]}名'.format(self.selectList),merge_format)
			insertDic['selectRank_positionDe'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=13)
			worksheet.merge_range(4,13,4,15,u'本期增仓前{0[4]}名'.format(self.selectList),merge_format)
			insertDic['selectRank_positionIn'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=16)
			worksheet.merge_range(4,16,4,18,u'本期增仓后{0[4]}名'.format(self.selectList),merge_format)
			worksheet.conditional_format(5,1,5+self.selectList[4],18, {'type': 'blanks','format':format})
			worksheet.conditional_format(5,1,5+self.selectList[4],18, {'type': 'no_blanks','format':format})
			worksheet.set_column(1, 18, 12)
			# 对冲损益查询
			insertDic['hedgeProfit'].to_excel(writer, sheet_name=u'对冲损益查询', startrow=4, startcol=1)
			insertDic['hedgeProfit_stddev'].to_excel(writer, sheet_name=u'对冲损益查询', startrow=4+date_dif+1+4, startcol=1)
			worksheet = writer.sheets[u'对冲损益查询']
			worksheet.write(2,1, u'对冲损益查询({0[0]},{0[1]}到{0[2]})'.format(self.titleList), bold)
			worksheet.write(4+date_dif+1+2,1, u'对冲损益查询(标准差)', bold)
			worksheet.conditional_format(4,1,4+date_dif+1,5, {'type': 'blanks','format':format})
			worksheet.conditional_format(4,1,4+date_dif+1,5, {'type': 'no_blanks','format':format})
			worksheet.conditional_format(4+date_dif+1+4,1,4+date_dif+1+5,2, {'type': 'blanks','format':format})
			worksheet.conditional_format(4+date_dif+1+4,1,4+date_dif+1+5,2, {'type': 'no_blanks','format':format})
			worksheet.set_column(1, 5, 15)
			
			writer.save()
			writer.close()
		#except:
			#sys.exit('Oops,Close Current EXCEL File and Retry,please!')
	
	# DEBUG
	def run(self):
		self.save_file(self.combine())
		# pass
		
if __name__ == "__main__":
	holdings_pfClass = holdings_pf()
	holdings_pfClass.run()
