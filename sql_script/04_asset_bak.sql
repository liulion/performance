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
  is 'Ψһ��ʶ'; 
comment on column perfdata.portAsset.FUND_ID
  is '�������'; 
comment on column perfdata.portAsset.PORT_ID
  is '��ϱ��'; 
comment on column perfdata.portAsset.L_DATE
  is '��������'; 
comment on column perfdata.portAsset.BEGIN_CASH
  is '�ڳ�CASH���'; 
comment on column perfdata.portAsset.CURRENT_CASH_EOD
  is '���е�ǰ�ʽ����'; 
comment on column perfdata.portAsset.STOCK_ASSET
  is '��Ʊ�ʲ�'; 
comment on column perfdata.portAsset.FUND_ASSET
  is '�����ʲ�'; 
comment on column perfdata.portAsset.BOND_ASSET
  is 'ծȯ�ʲ�'; 
comment on column perfdata.portAsset.DEPOSIT_ASSET
  is '���д��'; 
comment on column perfdata.portAsset.REPO_ASSET
  is '�����۳��ʲ�'; 
comment on column perfdata.portAsset.OTHER_CURRENCY_ASSET
  is '���������ʽ�'; 
comment on column perfdata.portAsset.SECURITY_SETTLEMENT_CASH
  is '֤ȯ�����'; 
comment on column perfdata.portAsset.FUTURES_ASSET
  is '�ڻ�ӯ��'; 
comment on column perfdata.portAsset.ACCUMULATE_UNIT_VALUE
  is '�ۼ�ʵ������'; 
comment on column perfdata.portAsset.ALLOCATBLE_PROFIT
  is '�ɷ�������'; 
comment on column perfdata.portAsset.NET_ASSETS
  is '��Ͼ��ʲ�'; 
comment on column perfdata.portAsset.TOTAL_ASSETS 
  is '�ʲ���ϼ�'; 
comment on column perfdata.portAsset.CREDIT_VALUE
  is '��ծ��ϼ�'; 
comment on column perfdata.portAsset.ACCUMULATE_UNIT_VALUE
  is '����ۼƵ�λ��ֵ'; 
comment on column perfdata.portAsset.UNIT_VALUE_YESTERDAY
  is '������յ�λ��ֵ'; 
comment on column perfdata.portAsset.UNIT_VALUE
  is '��ϵ��յ�λ��ֵ'; 
comment on column perfdata.portAsset.FCD
  is '�״β���ʱ��'; 
comment on column perfdata.portAsset.FCU
  is '�״β����û�'; 
comment on column perfdata.portAsset.LCD
  is '�ϴβ���ʱ��'; 
comment on column perfdata.portAsset.LCU
  is '�ϴβ����û�'; 
comment on column perfdata.portAsset.DATA_STATUS
  is '����״̬'; 
