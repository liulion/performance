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
  is 'Ψһ��ʶ'; 
comment on column perfdata.portHolding.FUND_ID
  is '�������'; 
comment on column perfdata.portHolding.PORT_ID
  is '��ϱ��'; 
comment on column perfdata.portHolding.L_DATE
  is '��������'; 
comment on column perfdata.portHolding.ACCOUNT_ID
  is '��Ŀ����'; 
comment on column perfdata.portHolding.ACCOUNT_NAME
  is '��Ŀ����'; 
comment on column perfdata.portHolding.POSITION_FLAG
  is '�ֲֶ�ձ�־'; 
comment on column perfdata.portHolding.MARKET_NO
  is '�����г����'; 
comment on column perfdata.portHolding.SUBMARKET_NO
  is '�����г��ӱ��'; 
comment on column perfdata.portHolding.WINDSECURITY_CODE
  is 'WIND����'; 
comment on column perfdata.portHolding.SECURITY_CODE
  is '֤ȯ����'; 
comment on column perfdata.portHolding.SECURITY_TYPE
  is '֤ȯ����'; 
comment on column perfdata.portHolding.AMOUNT
  is '��ǰ�ֲ�����'; 
comment on column perfdata.portHolding.UNIT_COST
  is '��λ�ɱ�'; 
comment on column perfdata.portHolding.TOTAL_COST
  is '�ɱ��ܶ�'; 
comment on column perfdata.portHolding.MARKET_PRICE
  is '��ֵ'; 
comment on column perfdata.portHolding.MARKET_VALUE
  is '�м�'; 
comment on column perfdata.portHolding.PANDL
  is '�ۼ�����'; 
comment on column perfdata.portHolding.NETASSET_PERCENT
  is '��ֵռ��ֵ�ٷֱ�'; 
comment on column perfdata.portHolding.NETTOTALASSET_PERCENT
  is '��ֵռ���ʲ��ٷֱ�'; 
comment on column perfdata.portHolding.BEGIN_AMOUNT
  is '�����ڳ�����'; 
comment on column perfdata.portHolding.BUY_AMOUNT
  is '������������'; 
comment on column perfdata.portHolding.SALE_AMOUNT
  is '������������'; 
comment on column perfdata.portHolding.BUY_CASH
  is '����������'; 
comment on column perfdata.portHolding.SALE_CASH
  is '�����������'; 
comment on column perfdata.portHolding.BUY_FEE
  is '�����������'; 
comment on column perfdata.portHolding.SALE_FEE
  is '������������'; 
comment on column perfdata.portHolding.FCD
  is '�״β���ʱ��'; 
comment on column perfdata.portHolding.FCU
  is '�״β����û�'; 
comment on column perfdata.portHolding.LCD
  is '�ϴβ���ʱ��'; 
comment on column perfdata.portHolding.LCU
  is '�ϴβ����û�'; 
comment on column perfdata.portHolding.DATA_STATUS
  is '����״̬'; 
