--Create table
create table perfdata.portHolding_bak
(
  ID                          varchar2(40),
  FUND_ID                     varchar2(20),
  PORT_ID                     varchar2(20),
  L_DATE                      varchar2(8),
  ACCOUNT_ID                  varchar2(20),
  ACCOUNT_NAME                varchar2(40),
  POSITION_FLAG               varchar2(20),
  MARKET_NO                   varchar2(20),
  SUBMARKET_NO                varchar2(20),
  WINDSECURITY_CODE           varchar2(20),
  SECURITY_CODE               varchar2(20),
  SECURITY_TYPE               varchar2(20),
  AMOUNT                      number(20,5),
  UNIT_COST                   number(20,5),
  TOTAL_COST                  number(20,5),
  MARKET_PRICE                number(20,5),
  MARKET_VALUE                number(20,5),
  PANDL                       number(20,5),
  NETASSET_PERCENT            number(20,5),
  NETTOTALASSET_PERCENT       number(20,5),
  BEGIN_AMOUNT                number(20,5),
  BUY_AMOUNT                  number(20,5),
  SALE_AMOUNT                 number(20,5),
  BUY_CASH                    number(20,5),
  SALE_CASH                   number(20,5),
  BUY_FEE                     number(20,5),
  SALE_FEE                    number(20,5),
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
comment on column perfdata.portHolding.ID
  is '唯一标识'; 
comment on column perfdata.portHolding.FUND_ID
  is '基金序号'; 
comment on column perfdata.portHolding.PORT_ID
  is '组合编号'; 
comment on column perfdata.portHolding.L_DATE
  is '发生日期'; 
comment on column perfdata.portHolding.ACCOUNT_ID
  is '科目代码'; 
comment on column perfdata.portHolding.ACCOUNT_NAME
  is '科目名称'; 
comment on column perfdata.portHolding.POSITION_FLAG
  is '持仓多空标志'; 
comment on column perfdata.portHolding.MARKET_NO
  is '交易市场编号'; 
comment on column perfdata.portHolding.SUBMARKET_NO
  is '交易市场子编号'; 
comment on column perfdata.portHolding.WINDSECURITY_CODE
  is 'WIND代码'; 
comment on column perfdata.portHolding.SECURITY_CODE
  is '证券代码'; 
comment on column perfdata.portHolding.SECURITY_TYPE
  is '证券类型'; 
comment on column perfdata.portHolding.AMOUNT
  is '当前持仓数量'; 
comment on column perfdata.portHolding.UNIT_COST
  is '单位成本'; 
comment on column perfdata.portHolding.TOTAL_COST
  is '成本总额'; 
comment on column perfdata.portHolding.MARKET_PRICE
  is '市值'; 
comment on column perfdata.portHolding.MARKET_VALUE
  is '市价'; 
comment on column perfdata.portHolding.PANDL
  is '累计损益'; 
comment on column perfdata.portHolding.NETASSET_PERCENT
  is '市值占净值百分比'; 
comment on column perfdata.portHolding.NETTOTALASSET_PERCENT
  is '市值占总资产百分比'; 
comment on column perfdata.portHolding.BEGIN_AMOUNT
  is '当日期初数量'; 
comment on column perfdata.portHolding.BUY_AMOUNT
  is '当日买入数量'; 
comment on column perfdata.portHolding.SALE_AMOUNT
  is '当日卖出数量'; 
comment on column perfdata.portHolding.BUY_CASH
  is '当日买入金额'; 
comment on column perfdata.portHolding.SALE_CASH
  is '当日卖出金额'; 
comment on column perfdata.portHolding.BUY_FEE
  is '当日买入费用'; 
comment on column perfdata.portHolding.SALE_FEE
  is '当日卖出费用'; 
comment on column perfdata.portHolding.FCD
  is '首次操作时间'; 
comment on column perfdata.portHolding.FCU
  is '首次操作用户'; 
comment on column perfdata.portHolding.LCD
  is '上次操作时间'; 
comment on column perfdata.portHolding.LCU
  is '上次操作用户'; 
comment on column perfdata.portHolding.DATA_STATUS
  is '数据状态'; 
