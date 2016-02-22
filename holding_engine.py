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
		
	# ���ʮ��Ȩ�عɱ�	
	def top10Stocks(self):
		sql = '''
              select * FROM
              (SELECT SECURITY_CODE as ��Ʊ����,ACCOUNT_NAME as ��Ʊ����,SUM(NETTOTALASSET_PERCENT) as �ֲ�ռ��,AVG(NETTOTALASSET_PERCENT) as ƽ���ֲ�ռ��,MAX(NETTOTALASSET_PERCENT) as ���ֲ�ռ��,MIN(NETTOTALASSET_PERCENT) as ��С�ֲ�ռ�� FROM Portholding
              WHERE FUND_ID = '{0[0]}' AND PORT_ID = '{0[1]}' AND L_DATE BETWEEN '{0[2]}' AND '{0[3]}'  AND SECURITY_TYPE = 'stock' AND DATA_STATUS = '1'
              GROUP BY SECURITY_CODE,ACCOUNT_NAME
              ORDER BY SUM(NETTOTALASSET_PERCENT) DESC)
              WHERE ROWNUM <= 10
              '''.format(self.selectList)
				
		df = pd.read_sql(sql, self.db)
		df.index=df[u'��Ʊ����']
		df.drop(u'��Ʊ����',axis=1,inplace=True)
		return df
		
	# �����ҵ��ֵ��Ȩ�ر� 
	def industryValue_Weight(self):
		pass
		
	# ������������뾻ֵ�� 
	def dailyReturn_netValue(self):
		sql = '''
              select a.L_DATE,a.TOTAL_ASSETS as �������ֵ,a.UNIT_VALUE as ����վ�ֵ, a.return as �����������, a.STOCK_ASSET as ��Ʊ�ֲ���ֵ, a.stock_percent as ��Ʊ��λռ��, a.cash as �ֽ���, a.cash_percent as �ֽ��λռ��, b.position/a.TOTAL_ASSETS as ���ռӼ��ֱ��� from
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
		df[u'�ճ���������'] = df[u'�����������']-df[u'�ջ�׼������']
		return df	
		
	# ��ϳֲֲ�ѯ
	def selectHolding(self):
		sql = '''
		      select L_DATE, SECURITY_CODE as ��Ʊ����, ACCOUNT_NAME as ��Ʊ����, AMOUNT as �ֲ� from Portholding
			  where SECURITY_TYPE = 'stock' and FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}'
			  order by L_DATE
			  '''.format(self.selectList)
		df = pd.read_sql(sql, self.db)
		df.index=df['l_date']
		df.drop('l_date',axis=1,inplace=True)
		return df
	
	# ����ڼ�������ѯ
	## 1.�ǵ���SQL�Ż�
	def selectRank(self):
		# ���� 
		## �ҵ�ÿ����Ʊ������ʱ�����еĵ�һ�������һ��
		##��1+��T��return * 1 + ��T+1��return ... 1 +��T+n��return��- 1 
		sql_decrease = '''  
		               select * FROM
		               (select a.SECURITY_CODE as ��Ʊ����, a.ACCOUNT_NAME as ��Ʊ����, ((b.MARKET_PRICE-a.MARKET_PRICE)/a.MARKET_PRICE) as ���� from (
                       (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, MARKET_PRICE  from portholding
                       where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE = '{0[2]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock' )a
                       left join
                       (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, MARKET_PRICE  from portholding
                       where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE = '{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock' )b
                       on a.SECURITY_CODE = b.SECURITY_CODE  )
                       order by ����)
					   WHERE ROWNUM <= {0[4]}
                       '''.format(self.selectList)
		# �Ƿ�
		## �ҵ�ÿ����Ʊ������ʱ�����еĵ�һ�������һ��
		##��1+��T��return * 1 + ��T+1��return ... 1 +��T+n��return��- 1 
		sql_increase = '''  
		               select * FROM
		               (select a.SECURITY_CODE as ��Ʊ����, a.ACCOUNT_NAME as ��Ʊ����, ((b.MARKET_PRICE-a.MARKET_PRICE)/a.MARKET_PRICE) as ���� from (
                       (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, MARKET_PRICE  from portholding
                       where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE = '{0[2]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock' )a
                       left join
                       (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, MARKET_PRICE  from portholding
                       where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE = '{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock' )b
                       on a.SECURITY_CODE = b.SECURITY_CODE  )
                       order by ���� DESC)
					   WHERE ROWNUM <= {0[4]}
                       '''.format(self.selectList)
		# PANDL ��
		sql_pandlDe = '''
		              select * FROM
		              (select SECURITY_CODE as ��Ʊ����, ACCOUNT_NAME as ��Ʊ����, sum(PANDL) as ��� from
                      (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, PANDL  from portholding
                      where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                      group by SECURITY_CODE, ACCOUNT_NAME
                      order by ���)
					  WHERE ROWNUM <= {0[4]}
		              '''.format(self.selectList)
		# PANDL ��
		sql_pandlIn = '''
		              select * FROM
		              (select SECURITY_CODE as ��Ʊ����, ACCOUNT_NAME as ��Ʊ����, sum(PANDL) as ��� from
                      (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, PANDL  from portholding
                      where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                      group by SECURITY_CODE, ACCOUNT_NAME
                      order by ��� DESC)
					  WHERE ROWNUM <= {0[4]}
		              '''.format(self.selectList)	  
		
		# ���ֽ��
		sql_positionDe = '''
		                 select * FROM
		                 (select SECURITY_CODE as ��Ʊ����, ACCOUNT_NAME as ��Ʊ����, sum(BUY_CASH)-sum(SALE_CASH) as ��� from
                         (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, BUY_CASH, SALE_CASH  from portholding
                         where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}'  and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                         group by SECURITY_CODE, ACCOUNT_NAME
                         order by ���)
						 WHERE ROWNUM <= {0[4]}
			             '''.format(self.selectList)
		
		# ���ֽ��
		sql_positionIn = '''
		                 select * FROM
		                 (select SECURITY_CODE as ��Ʊ����, ACCOUNT_NAME as ��Ʊ����, sum(BUY_CASH)-sum(SALE_CASH) as ��� from
                         (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, BUY_CASH, SALE_CASH  from portholding
                         where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}'  and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                         group by SECURITY_CODE, ACCOUNT_NAME
                         order by ��� DESC)
						 WHERE ROWNUM <= {0[4]}
		                 '''.format(self.selectList)
		
		selectRankDic = {}
		selectRankDic['decrease'] = pd.read_sql(sql_decrease, self.db)
		selectRankDic['decrease'].index=selectRankDic['decrease'][u'��Ʊ����']
		selectRankDic['decrease'].drop(u'��Ʊ����',axis=1,inplace=True)
		selectRankDic['increase'] = pd.read_sql(sql_increase, self.db)
		selectRankDic['increase'].index=selectRankDic['increase'][u'��Ʊ����']
		selectRankDic['increase'].drop(u'��Ʊ����',axis=1,inplace=True)
		selectRankDic['pandlDe'] = pd.read_sql(sql_pandlDe, self.db)
		selectRankDic['pandlDe'].index=selectRankDic['pandlDe'][u'��Ʊ����']
		selectRankDic['pandlDe'].drop(u'��Ʊ����',axis=1,inplace=True)
		selectRankDic['pandlIn'] = pd.read_sql(sql_pandlIn, self.db)
		selectRankDic['pandlIn'].index=selectRankDic['pandlIn'][u'��Ʊ����']
		selectRankDic['pandlIn'].drop(u'��Ʊ����',axis=1,inplace=True)
		selectRankDic['positionDe'] = pd.read_sql(sql_positionDe, self.db)
		selectRankDic['positionDe'].index=selectRankDic['positionDe'][u'��Ʊ����']
		selectRankDic['positionDe'].drop(u'��Ʊ����',axis=1,inplace=True)
		selectRankDic['positionIn'] = pd.read_sql(sql_positionIn, self.db)
		selectRankDic['positionIn'].index=selectRankDic['positionIn'][u'��Ʊ����']
		selectRankDic['positionIn'].drop(u'��Ʊ����',axis=1,inplace=True)
		return selectRankDic

	# �Գ������ѯ(����)
	def hedgeProfit(self):
		sql = '''
              select a.L_DATE, b.STOCK_ASSET as �Գ�ǰ�����ֵ , (a.future+b.STOCK_ASSET) as �Գ�������ֵ, a.future as �ڻ�ͷ��, (a.future+b.STOCK_ASSET) as ���ճ��� from
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
		
	# �Գ������ѯ(��׼��)
	def hedgeProfit_stddev(self):
		sql = '''
              select stddev(a.before) as �Գ�ǰ��� ,stddev(a.afer) as �Գ����� from
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
		df.index=df[u'�Գ�ǰ���']
		df.drop(u'�Գ�ǰ���',axis=1,inplace=True)
		return df
		
	# �����ҵ������
	def industryTR(self):
		pass	
	
	# ȡ��׼ ������
	# ���� ��׼�� ������ DataFrame
	def getBenchmarkReturn(self,benchmarkCode,startDate,endDate):
		benchmarkDF=pd.DataFrame()
		wind.w.start()
		tmp=wind.w.wsd(benchmarkCode, "pct_chg", startDate, endDate, "Fill=Previous;PriceAdj=F")
		benchmarkDF[u'�ջ�׼������']=tmp.Data[0]
		benchmarkDF['Date']=[str(datetime.strptime(x.strftime("%Y-%m-%d"),"%Y-%m-%d"))[0:10].replace('-','') for x in tmp.Times]
		#print str(benchmarkDF['Date'][0])[0:10].replace('-','')
		benchmarkDF.index=benchmarkDF['Date']
		benchmarkDF.drop('Date',axis=1,inplace=True)
		self.benchmarkDF=benchmarkDF
		return benchmarkDF
		
	# ��Ҫд��EXCEL�ļ���dataframe���һ���б�
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
		
	# ���ֲ�ָ�����EXCEL�ļ�
	## 1.���Ѿ�����EXCEL�в����µ�SHEETҳ
	## 2.���������ĵ�1.2�еı�����ʽд��
	def save_file(self, insertDic):
		#try:
			writer = pd.ExcelWriter(self.filePath + self.fileName, engine='xlsxwriter')
			workbook = writer.book
			bold = workbook.add_format({'bold': True})
			format=workbook.add_format()
			format.set_border(1)
			date_dif = int(self.selectList[3])-int(self.selectList[2])
			merge_format = workbook.add_format({'bold': True,'align':'center','valign':'vcenter'})
			# ���ʮ��Ȩ�عɱ�
			insertDic['top10Stocks'].to_excel(writer, sheet_name=u'���ʮ��Ȩ�عɱ�', startrow=4, startcol=1)
			worksheet = writer.sheets[u'���ʮ��Ȩ�عɱ�']
			worksheet.write(2,1, u'���ʮ��Ȩ�عɱ�({0[0]},{0[1]}��{0[2]})'.format(self.titleList), bold)
			worksheet.conditional_format(4,1,14,6, {'type': 'blanks','format':format})
			worksheet.conditional_format(4,1,14,6, {'type': 'no_blanks','format':format})
			worksheet.set_column(1, 6, 12)
			# ������������뾻ֵ��
			insertDic['dailyReturn_netValue'].to_excel(writer, sheet_name=u'������������뾻ֵ��', startrow=4, startcol=1)
			worksheet = writer.sheets[u'������������뾻ֵ��']
			worksheet.write(2,1, u'������������뾻ֵ��({0[0]},{0[1]}��{0[2]})'.format(self.titleList), bold)
			worksheet.conditional_format(4,1,4+date_dif+1,11, {'type': 'blanks','format':format})
			worksheet.conditional_format(4,1,4+date_dif+1,11, {'type': 'no_blanks','format':format})
			worksheet.set_column(1, 11, 14)
			# ��ϳֲֲ�ѯ
			insertDic['selectHolding'].to_excel(writer, sheet_name=u'��ϳֲֲ�ѯ', startrow=4, startcol=1)
			indexInt = len(insertDic['selectHolding'].index)
			worksheet = writer.sheets[u'��ϳֲֲ�ѯ']
			worksheet.write(2,1, u'��ϳֲֲ�ѯ({0[0]},{0[1]}��{0[2]})'.format(self.titleList), bold)
			worksheet.conditional_format(4,1,14,4, {'type': 'blanks','format':format})
			worksheet.conditional_format(4,1,4 + indexInt,4, {'type': 'no_blanks','format':format})
			worksheet.set_column(1, 4, 12)
			# ����ڼ�������ѯ
			insertDic['selectRank_decrease'].to_excel(writer, sheet_name=u'����ڼ�������ѯ', startrow=5, startcol=1)
			worksheet = writer.sheets[u'����ڼ�������ѯ']
			worksheet.write(2,1, u'����ڼ�������ѯ({0[0]},{0[1]}��{0[2]})'.format(self.titleList), bold)
			worksheet.merge_range(4,1,4,3,u'�Ƿ�ǰ{0[4]}��'.format(self.selectList),merge_format)
			insertDic['selectRank_increase'].to_excel(writer, sheet_name=u'����ڼ�������ѯ', startrow=5, startcol=4)
			worksheet.merge_range(4,4,4,6,u'����ǰ{0[4]}��'.format(self.selectList),merge_format)
			insertDic['selectRank_pandlDe'].to_excel(writer, sheet_name=u'����ڼ�������ѯ', startrow=5, startcol=7)
			worksheet.merge_range(4,7,4,9,u'��������ǰ{0[4]}��'.format(self.selectList),merge_format)
			insertDic['selectRank_pandlIn'].to_excel(writer, sheet_name=u'����ڼ�������ѯ', startrow=5, startcol=10)
			worksheet.merge_range(4,10,4,12,u'�������Сǰ{0[4]}��'.format(self.selectList),merge_format)
			insertDic['selectRank_positionDe'].to_excel(writer, sheet_name=u'����ڼ�������ѯ', startrow=5, startcol=13)
			worksheet.merge_range(4,13,4,15,u'��������ǰ{0[4]}��'.format(self.selectList),merge_format)
			insertDic['selectRank_positionIn'].to_excel(writer, sheet_name=u'����ڼ�������ѯ', startrow=5, startcol=16)
			worksheet.merge_range(4,16,4,18,u'�������ֺ�{0[4]}��'.format(self.selectList),merge_format)
			worksheet.conditional_format(5,1,5+self.selectList[4],18, {'type': 'blanks','format':format})
			worksheet.conditional_format(5,1,5+self.selectList[4],18, {'type': 'no_blanks','format':format})
			worksheet.set_column(1, 18, 12)
			# �Գ������ѯ
			insertDic['hedgeProfit'].to_excel(writer, sheet_name=u'�Գ������ѯ', startrow=4, startcol=1)
			insertDic['hedgeProfit_stddev'].to_excel(writer, sheet_name=u'�Գ������ѯ', startrow=4+date_dif+1+4, startcol=1)
			worksheet = writer.sheets[u'�Գ������ѯ']
			worksheet.write(2,1, u'�Գ������ѯ({0[0]},{0[1]}��{0[2]})'.format(self.titleList), bold)
			worksheet.write(4+date_dif+1+2,1, u'�Գ������ѯ(��׼��)', bold)
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
