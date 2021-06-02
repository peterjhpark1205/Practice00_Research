'''
This script is written 4 anyalyzing SNUCH's Inpatients' Data based on 'DRGNO by the order of payment'.

Written Date: 2019.12.17.
Written By: Peter JH Park

'''

### Import modules in needs

import os, sys, csv
import pandas as pd
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt
import datetime, time
import re
from itertools import product
import math

print("\n Current Working Directory is: ", os.getcwd())

### READ Files & Check

SNUCHIn = pd.read_csv("./SNUCH/SNUCHInP_R4A.csv", encoding="utf-8")
DiagCN = pd.read_csv("./master_Dcode&Dname.csv", encoding="utf-8")
SurCN = pd.read_csv("./master_Scode&Sname.csv", encoding="utf-8")
DRGCNmain5 = pd.read_csv("./SevCri_5digit(DRGname).csv", encoding="utf-8")
DRGCNsub4 = pd.read_csv("./SevCri_4digit(DRGname).csv", encoding="utf-8")


DiagCN.drop(['Dname(ENG)'], axis=1, inplace=True)
DiagCN.rename(columns={'Dcode' : 'D_Code', 'Dname(KOR)' :'D_Name'}, inplace=True)
DiagCN=DiagCN.groupby(by='D_Code', as_index=False).agg({'D_Name': lambda a: a.value_counts().index[0]})

SurCN = SurCN.rename(columns={'Scode':'Sur_Code', 'Sname':'Sur_Name'})
SurCN=SurCN.groupby(by='Sur_Code', as_index=False).agg({'Sur_Name': lambda a: a.value_counts().index[0]})

DRGCNmain5=DRGCNmain5.groupby(by='DRGNO', as_index=False).agg({'DRGname': lambda a: a.value_counts().index[0]})
DRGCNsub4=DRGCNsub4.groupby(by='DRGNO', as_index=False).agg({'DRGname': lambda a: a.value_counts().index[0]})

print(SNUCHIn.info())
print(SNUCHIn.columns)


############################# Analyzing #############################


## RAW DATA based on DRGNO by the order of payment: append rows
SNUCHIn4SevPbase = SNUCHIn.copy()
#SNUCHIn4SevPbase.DRGNO = SNUCHIn4SevPbase.DRGNO.str.split('/')
#SNUCHIn4SevPbase.Severity = SNUCHIn4SevPbase.Severity.str.split('/')
#SNUCHIn4SevPbase = SNUCHIn4SevPbase.apply(pd.Series.explode).reset_index(drop=True)
print(SNUCHIn4SevPbase.info())
print(SNUCHIn4SevPbase.columns)
'''
SNUCHIn4SevPbase['Severity'] = SNUCHIn4SevPbase['Severity'].map({'Severe' : 4, 'Normal' : 3, 'Simple' : 2, 'SortError' : 1})

SNUCHIn4SevPbasesub = SNUCHIn4SevPbase.copy()
SNUCHIn4SevPbase = SNUCHIn4SevPbase.groupby(['PT_No'], as_index= False)['Severity'].agg(lambda x: x.max())
SNUCHIn4SevPbase= SNUCHIn4SevPbase.merge(SNUCHIn4SevPbasesub, on=['PT_No', 'Severity'], how='left')
SNUCHIn4SevPbase.drop_duplicates(subset =['PT_No', 'Severity'], inplace = True)
SNUCHIn4SevPbase.reset_index(drop=True, inplace=True)

SNUCHIn4SevPbase['Severity'] = SNUCHIn4SevPbase['Severity'].map({4:'Severe', 3:'Normal', 2:'Simple', 1:'SortError'})
'''

DRGCNmain5.rename(columns={DRGCNmain5.columns[0] : 'DRGNO01', DRGCNmain5.columns[1] : 'DRGName01'}, inplace=True)
SNUCHIn4SevPbase['DRGNO01'] = SNUCHIn4SevPbase['DRGNO'].astype(str).str[0:5]
SNUCHIn4SevPbase['DRGNO01'].fillna('NoDRG', inplace=True)

DRGCNsub4.rename(columns={DRGCNsub4.columns[0] : 'DRGNO02', DRGCNsub4.columns[1] : 'DRGName02'}, inplace=True)
SNUCHIn4SevPbase['DRGNO02'] = SNUCHIn4SevPbase['DRGNO'].astype(str).str[0:4]
SNUCHIn4SevPbase['DRGNO02'].fillna('NoDRG', inplace=True)

SNUCHIn4SevPbase = SNUCHIn4SevPbase.merge(DRGCNmain5, on='DRGNO01', how='left').merge(DRGCNsub4, on='DRGNO02', how='left')
SNUCHIn4SevPbase.reset_index(drop=True, inplace=True)

SNUCHIn4SevPbase.loc[(SNUCHIn4SevPbase['DRGName01'].isna() &  SNUCHIn4SevPbase['DRGName02'].notna()), 'DRGName01'] = SNUCHIn4SevPbase['DRGName02']
SNUCHIn4SevPbase.rename(columns={'DRGName01' : 'DRGName'}, inplace=True)
del SNUCHIn4SevPbase['DRGNO01']; del SNUCHIn4SevPbase['DRGNO02']; del SNUCHIn4SevPbase['DRGName02']
SNUCHIn4SevPbase.DRGName.fillna('NoDRG', inplace=True)

SNUCHIn4SevPbase.loc[(SNUCHIn4SevPbase['DRGName']  == 'NoDRG'), 'DRGNO'] = 'NoDRG'
SNUCHIn4SevPbase = SNUCHIn4SevPbase[SNUCHIn4SevPbase.D_Code != 'NoDiag']
print(SNUCHIn4SevPbase.info())

SNUCHIn4SevPbase['TotalPay'] = SNUCHIn4SevPbase['Pay_InsSelf'] + SNUCHIn4SevPbase['Pay_InsCorp'] + SNUCHIn4SevPbase['Pay_NoIns'] + SNUCHIn4SevPbase['Pay_Sel']

bpay=SNUCHIn4SevPbase.groupby(by='DRGNO', as_index=False).agg({'TotalPay': pd.Series.mean})
bpay = bpay[bpay.DRGNO != 'NoDRG']
bpay.sort_values(by='TotalPay', ascending=False, inplace=True)
bpay.reset_index(drop=True, inplace=True)
bpay.index += 1
bpay = bpay.rename_axis('Rank').reset_index()
bpay['TotalPay'] = bpay['TotalPay'].astype(int)
print (bpay)

bdname=SNUCHIn4SevPbase.groupby(by='DRGNO', as_index=False).agg({'DRGName': lambda a: a.value_counts().index[0]})
bdname = bdname[bdname.DRGNO != 'NoDRG']
bdname.reset_index(drop=True, inplace=True)
#print (bdname)


bsev=SNUCHIn4SevPbase.groupby(by='DRGNO', as_index=False).agg({'Severity': pd.Series.unique})
bsev.reset_index(drop=True, inplace=True)
print (bsev)


## 01. BASIC INFORMATION by Severity Payment
pnum=SNUCHIn4SevPbase.groupby(by='DRGNO', as_index=False).agg({'PT_No': pd.Series.nunique})
pnum.rename(columns={'PT_No':'P_Num'}, inplace=True)
pnum['P_Num'] = pnum['P_Num'].astype(int)
pnum.reset_index(drop=True, inplace=True)
print (pnum)

avage=SNUCHIn4SevPbase.groupby(by='DRGNO', as_index=False).agg({'Age': pd.Series.mean})
avage.rename(columns={'Age':'AvAge'}, inplace=True)
avage.AvAge = avage.AvAge.round(1)
avage.reset_index(drop=True, inplace=True)
print (avage)

avinprd=SNUCHIn4SevPbase.groupby(by='DRGNO', as_index=False).agg({'In_Prd': pd.Series.mean})
avinprd.rename(columns={'In_Prd':'AvInPrd'}, inplace=True)
avinprd.AvInPrd = avinprd.AvInPrd.round(1)
avinprd.reset_index(drop=True, inplace=True)
print (avinprd)

SNUCHIn4SevPbase_Ins = SNUCHIn4SevPbase.copy()
SNUCHIn4SevPbase_Ins['IndPaidExp'] = SNUCHIn4SevPbase_Ins['Pay_InsSelf'] + SNUCHIn4SevPbase_Ins['Pay_NoIns'] + SNUCHIn4SevPbase_Ins['Pay_Sel']
inspaid = SNUCHIn4SevPbase_Ins.groupby(by='DRGNO', as_index=False).agg({'IndPaidExp' : pd.Series.mean, 'Pay_InsCorp' : pd.Series.mean})
inspaid.rename(columns={'Pay_InsCorp':'CorpPaidExp'}, inplace=True)
inspaid['IndPaidExp'] = inspaid['IndPaidExp'].astype(int)
inspaid['CorpPaidExp'] = inspaid['CorpPaidExp'].astype(int)
inspaid['IndPaidExp'] = inspaid.apply(lambda x: "{:,}".format(x['IndPaidExp']), axis=1)
inspaid['CorpPaidExp'] = inspaid.apply(lambda x: "{:,}".format(x['CorpPaidExp']), axis=1)
inspaid.reset_index(drop=True, inplace=True)
print (inspaid)


SNUCHInSevP_base = bpay.merge(bdname,on='DRGNO',how='left').merge(bsev,on='DRGNO',how='left').merge(pnum,on='DRGNO',how='left').merge(avage,on='DRGNO',how='left').merge(avinprd,on='DRGNO',how='left').merge(inspaid,on='DRGNO',how='left')
SNUCHInSevP_base = SNUCHInSevP_base[['Rank', 'DRGNO', 'DRGName', 'Severity', 'TotalPay', 'IndPaidExp', 'CorpPaidExp', 'P_Num', 'AvAge', 'AvInPrd']]
SNUCHInSevP_base.reset_index(drop=True, inplace=True)
print(SNUCHInSevP_base)
print(SNUCHInSevP_base.columns)


SNUCHInSevP_base.Severity = SNUCHInSevP_base.Severity.map({'Severe' : '전문', 'Normal' : '일반', 'Simple' : '단순', 'SortError' : '분류오류'})


SNUCHInSevP_baseSev = SNUCHInSevP_base[SNUCHInSevP_base.Severity == '전문']
SNUCHInSevP_baseSev = SNUCHInSevP_baseSev.sort_values(by ='TotalPay', ascending = 0)
SNUCHInSevP_baseSev.drop(['Rank'], axis=1, inplace=True)
SNUCHInSevP_baseSev.reset_index(drop=True, inplace=True)
SNUCHInSevP_baseSev.index += 1
SNUCHInSevP_baseSev['TotalPay'] = SNUCHInSevP_baseSev.apply(lambda x: "{:,}".format(x['TotalPay']), axis=1)
SNUCHInSevP_baseSev = SNUCHInSevP_baseSev.rename_axis('Rank').reset_index()


SNUCHInSevP_baseNorm = SNUCHInSevP_base[SNUCHInSevP_base.Severity == '일반']
SNUCHInSevP_baseNorm = SNUCHInSevP_baseNorm.sort_values(by ='TotalPay', ascending = 0)
SNUCHInSevP_baseNorm.drop(['Rank'], axis=1, inplace=True)
SNUCHInSevP_baseNorm.reset_index(drop=True, inplace=True)
SNUCHInSevP_baseNorm.index += 1
SNUCHInSevP_baseNorm['TotalPay'] = SNUCHInSevP_baseNorm.apply(lambda x: "{:,}".format(x['TotalPay']), axis=1)
SNUCHInSevP_baseNorm = SNUCHInSevP_baseNorm.rename_axis('Rank').reset_index()


SNUCHInSevP_baseSimp = SNUCHInSevP_base[SNUCHInSevP_base.Severity == '단순']
SNUCHInSevP_baseSimp = SNUCHInSevP_baseSimp.sort_values(by ='TotalPay', ascending = 0)
SNUCHInSevP_baseSimp.drop(['Rank'], axis=1, inplace=True)
SNUCHInSevP_baseSimp.reset_index(drop=True, inplace=True)
SNUCHInSevP_baseSimp.index += 1
SNUCHInSevP_baseSimp['TotalPay'] = SNUCHInSevP_baseSimp.apply(lambda x: "{:,}".format(x['TotalPay']), axis=1)
SNUCHInSevP_baseSimp = SNUCHInSevP_baseSimp.rename_axis('Rank').reset_index()


SNUCHInSevP_base['TotalPay'] = SNUCHInSevP_base.apply(lambda x: "{:,}".format(x['TotalPay']), axis=1)


SNUCHInSevP_base = SNUCHInSevP_base.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'TotalPay':'총부담금(원)',
                                                    'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)'})
SNUCHInSevP_baseSev = SNUCHInSevP_baseSev.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'TotalPay':'총부담금(원)',
                                                          'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)'})
SNUCHInSevP_baseNorm = SNUCHInSevP_baseNorm.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'TotalPay':'총부담금(원)',
                                                            'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)'})
SNUCHInSevP_baseSimp = SNUCHInSevP_baseSimp.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'TotalPay':'총부담금(원)',
                                                            'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)'})

print(SNUCHInSevP_base.columns)

SNUCHInSevP_base50 = SNUCHInSevP_base.loc[0:49, :]
SNUCHInSevP_base50Sev = SNUCHInSevP_baseSev.loc[0:49, :]
SNUCHInSevP_base50Norm = SNUCHInSevP_baseNorm.loc[0:49, :]
SNUCHInSevP_base50Simp = SNUCHInSevP_baseSimp.loc[0:49, :]



'''
# No Executuon
## 02. DIAGNOSIS & SURGERY INFORMATION by Severity Payment
SNUCHIn4SevPbase_Diag = SNUCHIn4SevPbase.copy()
SNUCHIn4SevPbase_Diag.D_Code = SNUCHIn4SevPbase_Diag.D_Code.str.split('/')
SNUCHIn4SevPbase_Diag.D_Name = SNUCHIn4SevPbase_Diag.D_Name.str.split('/')
SNUCHIn4SevPbase_Diag = SNUCHIn4SevPbase_Diag.apply(pd.Series.explode).reset_index(drop=True)
SNUCHIn4SevPbase_Diag = SNUCHIn4SevPbase_Diag.drop_duplicates(['PT_No', 'In_Date', 'DRGNO', 'Severity', 'D_Code', 'D_Name'], keep='first') # Need D_Date after for proper drop
SNUCHIn4SevPbase_Diag = SNUCHIn4SevPbase_Diag[SNUCHIn4SevPbase_Diag.D_Code != 'NoDiag']

##### another way to selcet most frequent value using groupby(Not Accurate in Certain Condition)#####
#dcodename = SNUCHIn4SevPbase_Diag.groupby(by='DRGNO', as_index=False).agg({'D_Code': lambda a: ' / '.join(pd.Series.mode(a)), 'D_Name': lambda b: ' / '.join(pd.Series.mode(b))})


dcode = SNUCHIn4SevPbase_Diag.groupby(by='DRGNO', as_index=False)['D_Code'].agg(lambda x:x.value_counts().index[0])
dname = SNUCHIn4SevPbase_Diag.groupby(by='D_Code', as_index=False).agg({'D_Name': lambda a: '/'.join(pd.unique(a))})
dcode.reset_index(drop=True, inplace=True)
dname.reset_index(drop=True, inplace=True)
dcodename = dcode.merge(dname, on='D_Code', how='left')
dcodename.reset_index(drop=True, inplace=True)
dfreq = SNUCHIn4SevPbase_Diag.groupby(by='DRGNO', as_index=False)['D_Code'].agg(lambda x: x.value_counts().head(1))
dfreq.rename(columns={'D_Code':'Dfreq'}, inplace=True)
dfreq['Dfreq'] = dfreq['Dfreq'].astype('int64')
dfreq.reset_index(drop=True, inplace=True)
dtot = SNUCHIn4SevPbase_Diag.groupby(by='DRGNO', as_index=False)['D_Code'].agg(lambda x: x.count())
dtot.rename(columns={'D_Code':'Dtot'}, inplace=True)
dtot['Dtot'] = dtot['Dtot'].astype('int64')
dtot.reset_index(drop=True, inplace=True)
dcodenamefreq = dcodename.merge(dfreq,on='DRGNO',how='left').merge(dtot,on='DRGNO',how='left')
dcodenamefreq.rename(columns={'D_Code':'Dcode', 'D_Name':'Dname'}, inplace=True)
dcodenamefreq.reset_index(drop=True, inplace=True)
dcodenamefreq['Dratio'] = (dcodenamefreq.Dfreq / dcodenamefreq.Dtot) * 100
dcodenamefreq.Dratio = dcodenamefreq.Dratio.round(2)
dcodenamefreq['Dratio'] = dcodenamefreq['Dratio'].astype(str) + ' %'
with pd.option_context('display.max_columns', None):
    print (dcodenamefreq)


SNUCHIn4SevPbase_Sur = SNUCHIn4SevPbase.copy()
SNUCHIn4SevPbase_Sur.Sur_Date = SNUCHIn4SevPbase_Sur.Sur_Date.str.split('/')
SNUCHIn4SevPbase_Sur.Sur_Code = SNUCHIn4SevPbase_Sur.Sur_Code.str.split('/')
SNUCHIn4SevPbase_Sur.Sur_Name = SNUCHIn4SevPbase_Sur.Sur_Name.str.split('/')
SNUCHIn4SevPbase_Sur = SNUCHIn4SevPbase_Sur.apply(pd.Series.explode).reset_index(drop=True)
SNUCHIn4SevPbase_Sur = SNUCHIn4SevPbase_Sur.drop_duplicates(['PT_No', 'In_Date', 'DRGNO', 'Severity', 'Sur_Date', 'Sur_Code', 'Sur_Name'], keep='first')
SNUCHIn4SevPbase_OnlySur = SNUCHIn4SevPbase_Sur[SNUCHIn4SevPbase_Sur.Sur_Code != 'NoSur']
SNUCHIn4SevPbase_OnlySur = SNUCHIn4SevPbase_OnlySur[SNUCHIn4SevPbase_OnlySur.Sur_Code != 'GroupPay_SNUCH']

##### another way to selcet most frequent value using groupby(Not Accurate in Certain Condition)#####
#scodename = SNUCHIn4SevPbase_Sur.groupby(by='DRGNO', as_index=False).agg({'Sur_Code': lambda a: ' / '.join(pd.Series.mode(a)), 'Sur_Name' : lambda b: ' / '.join(pd.Series.mode(b))})


scode = SNUCHIn4SevPbase_OnlySur.groupby(by='DRGNO', as_index=False)['Sur_Code'].agg(lambda x : x.value_counts().index[0])
sname = SNUCHIn4SevPbase_OnlySur.groupby(by='Sur_Code', as_index=False)['Sur_Name'].agg(lambda x : x.value_counts().index[0])
scode.reset_index(drop=True, inplace=True)
sname.reset_index(drop=True, inplace=True)
scodename = scode.merge(sname, on='Sur_Code', how='left')
scodename.reset_index(drop=True, inplace=True)
sfreq = SNUCHIn4SevPbase_OnlySur.groupby(by='DRGNO', as_index=False)['Sur_Code'].agg(lambda x : x.value_counts().head(1))
sfreq.rename(columns={'Sur_Code':'Sfreq'}, inplace=True)
sfreq['Sfreq'] = sfreq['Sfreq'].astype('int64')
sfreq.reset_index(drop=True, inplace=True)
stot = SNUCHIn4SevPbase_OnlySur.groupby(by='DRGNO', as_index=False)['Sur_Code'].agg(lambda x : x.count())
stot.rename(columns={'Sur_Code':'Stot'}, inplace=True)
stot['Stot'] = stot['Stot'].astype('int64')
stot.reset_index(drop=True, inplace=True)
scodenamefreq = scodename.merge(sfreq,on='DRGNO',how='left').merge(stot,on='DRGNO',how='left')
scodenamefreq.rename(columns={'Sur_Code':'Scode', 'Sur_Name':'Sname'}, inplace=True)
scodenamefreq.reset_index(drop=True, inplace=True)
scodenamefreq['Sratio'] = (scodenamefreq.Sfreq / scodenamefreq.Stot) * 100
scodenamefreq.Sratio = scodenamefreq.Sratio.round(2)
scodenamefreq['Sratio'] = scodenamefreq['Sratio'].astype(str) + ' %'
with pd.option_context('display.max_columns', None):
    print (scodenamefreq)


SNUCHInSevP_diagsur = bpay.merge(bsev,on='DRGNO',how='left').merge(dcodenamefreq,on='DRGNO',how='left').merge(scodenamefreq,on='DRGNO',how='left')
SNUCHInSevP_diagsur.drop(['TotalPay' ,'Dtot', 'Stot'], axis=1, inplace=True)
SNUCHInSevP_diagsur.reset_index(drop=True, inplace=True)
SNUCHInSevP_diagsur['Dcode'].fillna('NoDiag', inplace=True)
SNUCHInSevP_diagsur['Dname'].fillna('NoDiag', inplace=True)
SNUCHInSevP_diagsur['Dfreq'].fillna(0, inplace=True)
SNUCHInSevP_diagsur['Dratio'].fillna('0.0 %', inplace=True)
SNUCHInSevP_diagsur['Scode'].fillna('NoSur', inplace=True)
SNUCHInSevP_diagsur['Sname'].fillna('NoSur', inplace=True)
SNUCHInSevP_diagsur['Sfreq'].fillna(0, inplace=True)
SNUCHInSevP_diagsur['Sratio'].fillna('0.0 %', inplace=True)

with pd.option_context('display.max_columns', None):
    print (SNUCHInSevP_diagsur)

print(SNUCHInSevP_diagsur.columns)

SNUCHInSevP_diagsur50 = SNUCHInSevP_diagsur.loc[0:49, :]
'''


SNUCHInSevP_base50.to_csv('./SNUCH/SNUCH 입원환자 중증도 총부담금별 순위.csv', encoding='cp949', index=False)
SNUCHInSevP_base50Sev.to_csv('./SNUCH/SNUCH 입원환자 중증도 총부담금별 순위(전문상위50).csv', encoding='cp949', index=False)
SNUCHInSevP_base50Norm.to_csv('./SNUCH/SNUCH 입원환자 중증도 총부담금별 순위(일반상위50).csv', encoding='cp949', index=False)
SNUCHInSevP_base50Simp.to_csv('./SNUCH/SNUCH 입원환자 중증도 총부담금별 순위(단순상위50).csv', encoding='cp949', index=False)
#SNUCHInSevP_diagsur50.to_csv('./SNUCH/SNUCHInSevP_diagsur50.csv', index=False)

SNUCHInSevP_base.to_csv('./SNUCH/(원)SNUCH 입원환자 중증도 총부담금별 순위.csv', encoding='cp949', index=False)
SNUCHInSevP_baseSev.to_csv('./SNUCH/(원)SNUCH 입원환자 중증도 총부담금별 순위(전문상위50).csv', encoding='cp949', index=False)
SNUCHInSevP_baseNorm.to_csv('./SNUCH/(원)SNUCH 입원환자 중증도 총부담금별 순위(일반상위50).csv', encoding='cp949', index=False)
SNUCHInSevP_baseSimp.to_csv('./SNUCH/(원)SNUCH 입원환자 중증도 총부담금별 순위(단순상위50).csv', encoding='cp949', index=False)

#SNUCHInSevP_inst = pd.DataFrame(columns=['Rank' ,'DRGNO', 'Severity', 'A_Ratio', 'B_Ratio', 'C_Ratio', 'D_Ratio', 'E_Ratio', 'F_Ratio', 'G_Ratio', 'H_Ratio'])
#SNUCHInSevP_regn = pd.DataFrame(columns=['Rank' ,'DRGNO', 'Severity', 'fSeoul', 'fBusan', 'fDaegu', 'fGwangju', 'fDaejeon', 'fIncheon', 'fJeju', 'fSejong', 'fJeonnam', 'fJeonbuk',
#                                      'fGyeongnam', 'fGyeongbuk', 'fChungnam', 'fChungbuk', 'fGangwon', 'fGyeonggi'])



