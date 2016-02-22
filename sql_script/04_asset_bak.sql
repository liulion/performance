--Create table
create table perfdata.portAsset_bak 
(
  ID                          varchar2(40),
  FUND_ID                     varchar2(20),
  PORT_ID                     varchar2(20),
  L_DATE                      varchar2(8),
  BEGIN_CASH                  number(20,5),
  CURRENT_CASH_EOD            number(20,5),
  STOCK_ASSET                 number(20,5),
  FUND_ASSET                  number(20,5),
  BOND_ASSET                  number(20,5),
  DEPOSIT_ASSET               number(20,5),
  REPO_ASSET                  number(20,5),
  OTHER_CURRENCY_ASSET        number(20,5),
  SECURITY_SETTLEMENT_CASH    number(20,5),
  FUTURES_ASSET               number(20,5),
  ACCUMULATE_PROFIT           number(20,5),
  ALLOCATBLE_PROFIT           number(20,5),
  NET_ASSETS                  number(20,5),
  TOTAL_ASSETS                number(20,5),
  CREDIT_VALUE                number(20,5),
  ACCUMULATE_UNIT_VALUE       number(20,5),
  UNIT_VALUE_YESTERDAY        number(20,5),
  UNIT_VALUE                  number(20,5),
  FCD                         date,
  FCU                         varchar2(40),
  LCD                         date,
  LCU                         varchar2(40),
  DATA_STATUS                 char(1)
)
tablespace perfdata
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
  next 1M
  minextents 1
  maxextents unlimited
  );
--Add comments to the columns
comment on column perfdata.portAsset.ID
  is '唯一标识'; 
comment on column perfdata.portAsset.FUND_ID
  is '基金序号'; 
comment on column perfdata.portAsset.PORT_ID
  is '组合编号'; 
comment on column perfdata.portAsset.L_DATE
  is '发生日期'; 
comment on column perfdata.portAsset.BEGIN_CASH
  is '期初CASH余额'; 
comment on column perfdata.portAsset.CURRENT_CASH_EOD
  is '日中当前资金余额'; 
comment on column perfdata.portAsset.STOCK_ASSET
  is '股票资产'; 
comment on column perfdata.portAsset.FUND_ASSET
  is '基金资产'; 
comment on column perfdata.portAsset.BOND_ASSET
  is '债券资产'; 
comment on column perfdata.portAsset.DEPOSIT_ASSET
  is '银行存款'; 
comment on column perfdata.portAsset.REPO_ASSET
  is '买入售出资产'; 
comment on column perfdata.portAsset.OTHER_CURRENCY_ASSET
  is '其他货币资金'; 
comment on column perfdata.portAsset.SECURITY_SETTLEMENT_CASH
  is '证券清算款'; 
comment on column perfdata.portAsset.FUTURES_ASSET
  is '期货盈亏'; 
comment on column perfdata.portAsset.ACCUMULATE_UNIT_VALUE
  is '累计实现收益'; 
comment on column perfdata.portAsset.ALLOCATBLE_PROFIT
  is '可分配利润'; 
comment on column perfdata.portAsset.NET_ASSETS
  is '组合净资产'; 
comment on column perfdata.portAsset.TOTAL_ASSETS 
  is '资产类合计'; 
comment on column perfdata.portAsset.CREDIT_VALUE
  is '负债类合计'; 
comment on column perfdata.portAsset.ACCUMULATE_UNIT_VALUE
  is '组合累计单位净值'; 
comment on column perfdata.portAsset.UNIT_VALUE_YESTERDAY
  is '组合昨日单位净值'; 
comment on column perfdata.portAsset.UNIT_VALUE
  is '组合当日单位净值'; 
comment on column perfdata.portAsset.FCD
  is '首次操作时间'; 
comment on column perfdata.portAsset.FCU
  is '首次操作用户'; 
comment on column perfdata.portAsset.LCD
  is '上次操作时间'; 
comment on column perfdata.portAsset.LCU
  is '上次操作用户'; 
comment on column perfdata.portAsset.DATA_STATUS
  is '数据状态'; 
