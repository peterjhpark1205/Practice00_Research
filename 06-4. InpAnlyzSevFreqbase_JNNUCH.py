'''
This script is written 4 anyalyzing JNNUCH's Inpatients' Data based on 'DRGNO Frequency and highest '.

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

JNNUCHIn = pd.read_csv("./JNNUCH/JNNUCHInP_R4A.csv", encoding="utf-8")
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

print(JNNUCHIn.info())
print(JNNUCHIn.columns)


############################# Analyzing #############################


## RAW DATA based one Severity Frequency: append rows
JNNUCHIn4SevFbase = JNNUCHIn.copy()
#JNNUCHIn4SevFbase.DRGNO = JNNUCHIn4SevFbase.DRGNO.str.split('/')
#JNNUCHIn4SevFbase.Severity = JNNUCHIn4SevFbase.Severity.str.split('/')
#JNNUCHIn4SevFbase = JNNUCHIn4SevFbase.apply(pd.Series.explode).reset_index(drop=True)
print(JNNUCHIn4SevFbase.info())
print(JNNUCHIn4SevFbase.columns)
'''
JNNUCHIn4SevFbase['Severity'] = JNNUCHIn4SevFbase['Severity'].map({'Severe' : 4, 'Normal' : 3, 'Simple' : 2, 'SortError' : 1})

JNNUCHIn4SevFbasesub = JNNUCHIn4SevFbase.copy()
JNNUCHIn4SevFbase = JNNUCHIn4SevFbase.groupby(['PT_No'], as_index= False)['Severity'].agg(lambda x: x.max())
JNNUCHIn4SevFbase= JNNUCHIn4SevFbase.merge(JNNUCHIn4SevFbasesub, on=['PT_No', 'Severity'], how='left')
JNNUCHIn4SevFbase.drop_duplicates(subset =['PT_No', 'Severity'], inplace = True)
JNNUCHIn4SevFbase.reset_index(drop=True, inplace=True)

JNNUCHIn4SevFbase['Severity'] = JNNUCHIn4SevFbase['Severity'].map({4:'Severe', 3:'Normal', 2:'Simple', 1:'SortError'})
'''

DRGCNmain5.rename(columns={DRGCNmain5.columns[0] : 'DRGNO01', DRGCNmain5.columns[1] : 'DRGName01'}, inplace=True)
JNNUCHIn4SevFbase['DRGNO01'] = JNNUCHIn4SevFbase['DRGNO'].astype(str).str[0:5]
JNNUCHIn4SevFbase['DRGNO01'].fillna('NoDRG', inplace=True)

DRGCNsub4.rename(columns={DRGCNsub4.columns[0] : 'DRGNO02', DRGCNsub4.columns[1] : 'DRGName02'}, inplace=True)
JNNUCHIn4SevFbase['DRGNO02'] = JNNUCHIn4SevFbase['DRGNO'].astype(str).str[0:4]
JNNUCHIn4SevFbase['DRGNO02'].fillna('NoDRG', inplace=True)

JNNUCHIn4SevFbase = JNNUCHIn4SevFbase.merge(DRGCNmain5, on='DRGNO01', how='left').merge(DRGCNsub4, on='DRGNO02', how='left')
JNNUCHIn4SevFbase.reset_index(drop=True, inplace=True)

JNNUCHIn4SevFbase.loc[(JNNUCHIn4SevFbase['DRGName01'].isna() &  JNNUCHIn4SevFbase['DRGName02'].notna()), 'DRGName01'] = JNNUCHIn4SevFbase['DRGName02']
JNNUCHIn4SevFbase.rename(columns={'DRGName01' : 'DRGName'}, inplace=True)
del JNNUCHIn4SevFbase['DRGNO01']; del JNNUCHIn4SevFbase['DRGNO02']; del JNNUCHIn4SevFbase['DRGName02']
JNNUCHIn4SevFbase.DRGName.fillna('NoDRG', inplace=True)

JNNUCHIn4SevFbase.loc[(JNNUCHIn4SevFbase['DRGName']  == 'NoDRG'), 'DRGNO'] = 'NoDRG'
JNNUCHIn4SevFbase = JNNUCHIn4SevFbase[JNNUCHIn4SevFbase.D_Code != 'NoDiag']
print(JNNUCHIn4SevFbase.info())

bfreq=JNNUCHIn4SevFbase.groupby(by='DRGNO', as_index=False).agg({'Severity': pd.Series.count})
bfreq.rename(columns={'Severity':'Frequency'}, inplace=True)
bfreq = bfreq[bfreq.DRGNO != 'NoDRG']
bfreq.sort_values(by='Frequency', ascending=False, inplace=True)
bfreq.reset_index(drop=True, inplace=True)
bfreq.index += 1
bfreq = bfreq.rename_axis('Rank').reset_index()
bfreq['Ratio'] = (bfreq.Frequency / bfreq.Frequency.sum()) * 100
bfreq.Ratio = bfreq.Ratio.round(1)
bfreq['Ratio'] = bfreq['Ratio'].astype(str)
bfreq['Frequency'] = bfreq['Frequency'].astype(int)
print (bfreq)

bdname=JNNUCHIn4SevFbase.groupby(by='DRGNO', as_index=False).agg({'DRGName': lambda a: a.value_counts().index[0]})
bdname = bdname[bdname.DRGNO != 'NoDRG']
bdname.reset_index(drop=True, inplace=True)
#print (bdname)


bsev=JNNUCHIn4SevFbase.groupby(by='DRGNO', as_index=False).agg({'Severity': pd.Series.unique})
bsev.reset_index(drop=True, inplace=True)
print (bsev)


## 01. BASIC INFORMATION by Severity Frequency
pnum=JNNUCHIn4SevFbase.groupby(by='DRGNO', as_index=False).agg({'PT_No': pd.Series.nunique})
pnum.rename(columns={'PT_No':'P_Num'}, inplace=True)
pnum['P_Num'] = pnum['P_Num'].astype(int)
pnum.reset_index(drop=True, inplace=True)
print (pnum)

avage=JNNUCHIn4SevFbase.groupby(by='DRGNO', as_index=False).agg({'Age': pd.Series.mean})
avage.rename(columns={'Age':'AvAge'}, inplace=True)
avage.AvAge = avage.AvAge.round(1)
avage.reset_index(drop=True, inplace=True)
print (avage)

avinprd=JNNUCHIn4SevFbase.groupby(by='DRGNO', as_index=False).agg({'In_Prd': pd.Series.mean})
avinprd.rename(columns={'In_Prd':'AvInPrd'}, inplace=True)
avinprd.AvInPrd = avinprd.AvInPrd.round(1)
avinprd.reset_index(drop=True, inplace=True)
print (avinprd)

JNNUCHIn4SevFbase_Ins = JNNUCHIn4SevFbase.copy()
JNNUCHIn4SevFbase_Ins['IndPaidExp'] = JNNUCHIn4SevFbase_Ins['Pay_InsSelf'] + JNNUCHIn4SevFbase_Ins['Pay_NoIns'] + JNNUCHIn4SevFbase_Ins['Pay_Sel']
inspaid = JNNUCHIn4SevFbase_Ins.groupby(by='DRGNO', as_index=False).agg({'IndPaidExp' : pd.Series.mean, 'Pay_InsCorp' : pd.Series.mean})
inspaid.rename(columns={'Pay_InsCorp':'CorpPaidExp'}, inplace=True)
inspaid['IndPaidExp'] = inspaid['IndPaidExp'].astype(int)
inspaid['CorpPaidExp'] = inspaid['CorpPaidExp'].astype(int)
inspaid['IndPaidExp'] = inspaid.apply(lambda x: "{:,}".format(x['IndPaidExp']), axis=1)
inspaid['CorpPaidExp'] = inspaid.apply(lambda x: "{:,}".format(x['CorpPaidExp']), axis=1)
inspaid.reset_index(drop=True, inplace=True)
print (inspaid)


JNNUCHInSevF_base = bfreq.merge(bdname,on='DRGNO',how='left').merge(bsev,on='DRGNO',how='left').merge(pnum,on='DRGNO',how='left').merge(avage,on='DRGNO',how='left').merge(avinprd,on='DRGNO',how='left').merge(inspaid,on='DRGNO',how='left')
JNNUCHInSevF_base = JNNUCHInSevF_base[['Rank', 'DRGNO', 'DRGName', 'Severity', 'Frequency', 'Ratio', 'P_Num', 'AvAge', 'AvInPrd', 'IndPaidExp', 'CorpPaidExp']]
JNNUCHInSevF_base.reset_index(drop=True, inplace=True)
print(JNNUCHInSevF_base)
print(JNNUCHInSevF_base.columns)

JNNUCHInSevF_base.Severity = JNNUCHInSevF_base.Severity.map({'Severe' : '전문', 'Normal' : '일반', 'Simple' : '단순', 'SortError' : '분류오류'})


JNNUCHInSevF_baseSev = JNNUCHInSevF_base[JNNUCHInSevF_base.Severity == '전문']
JNNUCHInSevF_baseSev = JNNUCHInSevF_baseSev.sort_values(by ='Frequency', ascending = 0)
JNNUCHInSevF_baseSev.drop(['Rank'], axis=1, inplace=True)
JNNUCHInSevF_baseSev.reset_index(drop=True, inplace=True)
JNNUCHInSevF_baseSev.index += 1
JNNUCHInSevF_baseSev = JNNUCHInSevF_baseSev.rename_axis('Rank').reset_index()


JNNUCHInSevF_baseNorm = JNNUCHInSevF_base[JNNUCHInSevF_base.Severity == '일반']
JNNUCHInSevF_baseNorm = JNNUCHInSevF_baseNorm.sort_values(by ='Frequency', ascending = 0)
JNNUCHInSevF_baseNorm.drop(['Rank'], axis=1, inplace=True)
JNNUCHInSevF_baseNorm.reset_index(drop=True, inplace=True)
JNNUCHInSevF_baseNorm.index += 1
JNNUCHInSevF_baseNorm = JNNUCHInSevF_baseNorm.rename_axis('Rank').reset_index()


JNNUCHInSevF_baseSimp = JNNUCHInSevF_base[JNNUCHInSevF_base.Severity == '단순']
JNNUCHInSevF_baseSimp = JNNUCHInSevF_baseSimp.sort_values(by ='Frequency', ascending = 0)
JNNUCHInSevF_baseSimp.drop(['Rank'], axis=1, inplace=True)
JNNUCHInSevF_baseSimp.reset_index(drop=True, inplace=True)
JNNUCHInSevF_baseSimp.index += 1
JNNUCHInSevF_baseSimp = JNNUCHInSevF_baseSimp.rename_axis('Rank').reset_index()




JNNUCHInSevF_base = JNNUCHInSevF_base.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'Frequency':'빈도(건)',
                                                    'Ratio':'비율(%)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)', 'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)'})
JNNUCHInSevF_baseSev = JNNUCHInSevF_baseSev.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'Frequency':'빈도(건)',
                                                          'Ratio':'비율(%)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)', 'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)'})
JNNUCHInSevF_baseNorm = JNNUCHInSevF_baseNorm.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'Frequency':'빈도(건)',
                                                            'Ratio':'비율(%)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)', 'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)'})
JNNUCHInSevF_baseSimp = JNNUCHInSevF_baseSimp.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'Frequency':'빈도(건)',
                                                            'Ratio':'비율(%)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)', 'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)'})

print(JNNUCHInSevF_base.columns)

JNNUCHInSevF_base50 = JNNUCHInSevF_base.loc[0:49, :]
JNNUCHInSevF_base50Sev = JNNUCHInSevF_baseSev.loc[0:49, :]
JNNUCHInSevF_base50Norm = JNNUCHInSevF_baseNorm.loc[0:49, :]
JNNUCHInSevF_base50Simp = JNNUCHInSevF_baseSimp.loc[0:49, :]

'''
# No Executuon
## 02. DIAGNOSIS & SURGERY INFORMATION by Severity Frequency
JNNUCHIn4SevFbase_Diag = JNNUCHIn4SevFbase.copy()
JNNUCHIn4SevFbase_Diag.D_Code = JNNUCHIn4SevFbase_Diag.D_Code.str.split('/')
JNNUCHIn4SevFbase_Diag['D_Code'].fillna('NoDiag', inplace=True)
JNNUCHIn4SevFbase_Diag = JNNUCHIn4SevFbase_Diag.apply(pd.Series.explode).reset_index(drop=True)
JNNUCHIn4SevFbase_Diag = JNNUCHIn4SevFbase_Diag.drop_duplicates(['PT_No', 'In_Date', 'DRGNO', 'Severity', 'D_Code'], keep='first') # Need D_Date after for proper drop
JNNUCHIn4SevFbase_Diag = JNNUCHIn4SevFbase_Diag[JNNUCHIn4SevFbase_Diag.D_Code != 'NoDiag']

##### another way to selcet most frequent value using groupby(Not Accurate in Certain Condition)#####
#dcodename = JNNUCHIn4SevFbase_Diag.groupby(by='DRGNO', as_index=False).agg({'D_Code': lambda a: ' / '.join(pd.Series.mode(a)), 'D_Name': lambda b: ' / '.join(pd.Series.mode(b))})


dcode = JNNUCHIn4SevFbase_Diag.groupby(by='DRGNO', as_index=False)['D_Code'].agg(lambda x:x.value_counts().index[0])
dname = JNNUCHIn4SevFbase_Diag.groupby(by='D_Code', as_index=False).agg({'D_Name': lambda a: '/'.join(pd.unique(a))})
dcode.reset_index(drop=True, inplace=True)
dname.reset_index(drop=True, inplace=True)
dcodename = dcode.merge(dname, on='D_Code', how='left')
dcodename.reset_index(drop=True, inplace=True)
dfreq = JNNUCHIn4SevFbase_Diag.groupby(by='DRGNO', as_index=False)['D_Code'].agg(lambda x: x.value_counts().head(1))
dfreq.rename(columns={'D_Code':'Dfreq'}, inplace=True)
dfreq['Dfreq'] = dfreq['Dfreq'].astype('int64')
dfreq.reset_index(drop=True, inplace=True)
dtot = JNNUCHIn4SevFbase_Diag.groupby(by='DRGNO', as_index=False)['D_Code'].agg(lambda x: x.count())
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


JNNUCHIn4SevFbase_Sur = JNNUCHIn4SevFbase.copy()
JNNUCHIn4SevFbase_Sur.Sur_Date = JNNUCHIn4SevFbase_Sur.Sur_Date.str.split('/')
JNNUCHIn4SevFbase_Sur.Sur_Code = JNNUCHIn4SevFbase_Sur.Sur_Code.str.split('/')
JNNUCHIn4SevFbase_Sur.Sur_Name = JNNUCHIn4SevFbase_Sur.Sur_Name.str.split('/')
JNNUCHIn4SevFbase_Sur['Sur_Date'].fillna('NoSur', inplace=True)
JNNUCHIn4SevFbase_Diag['Sur_Code'].fillna('NoSur', inplace=True)
JNNUCHIn4SevFbase_Diag['Sur_Name'].fillna('NoSur', inplace=True)
JNNUCHIn4SevFbase_Sur = JNNUCHIn4SevFbase_Sur.apply(pd.Series.explode).reset_index(drop=True)
JNNUCHIn4SevFbase_Sur = JNNUCHIn4SevFbase_Sur.drop_duplicates(['PT_No', 'In_Date', 'DRGNO', 'Severity', 'Sur_Date','Sur_Code', 'Sur_Name'], keep='first')
JNNUCHIn4SevFbase_OnlySur = JNNUCHIn4SevFbase_Sur[JNNUCHIn4SevFbase_Sur.Sur_Code != 'NoSur']
JNNUCHIn4SevFbase_OnlySur = JNNUCHIn4SevFbase_OnlySur[JNNUCHIn4SevFbase_OnlySur.Sur_Code != 'GroupPay_JNNUCH']


##### another way to selcet most frequent value using groupby(Not Accurate in Certain Condition)#####
#scodename = JNNUCHIn4SevFbase_Sur.groupby(by='DRGNO', as_index=False).agg({'Sur_Code': lambda a: ' / '.join(pd.Series.mode(a)), 'Sur_Name' : lambda b: ' / '.join(pd.Series.mode(b))})


scode = JNNUCHIn4SevFbase_OnlySur.groupby(by='DRGNO', as_index=False)['Sur_Code'].agg(lambda x : x.value_counts().index[0])
sname = JNNUCHIn4SevFbase_OnlySur.groupby(by='Sur_Code', as_index=False)['Sur_Name'].agg(lambda x : x.value_counts().index[0])
scode.reset_index(drop=True, inplace=True)
sname.reset_index(drop=True, inplace=True)
scodename = scode.merge(sname, on='Sur_Code', how='left')
scodename.reset_index(drop=True, inplace=True)
sfreq = JNNUCHIn4SevFbase_OnlySur.groupby(by='DRGNO', as_index=False)['Sur_Code'].agg(lambda x : x.value_counts().head(1))
sfreq.rename(columns={'Sur_Code':'Sfreq'}, inplace=True)
sfreq['Sfreq'] = sfreq['Sfreq'].astype('int64')
sfreq.reset_index(drop=True, inplace=True)
stot = JNNUCHIn4SevFbase_OnlySur.groupby(by='DRGNO', as_index=False)['Sur_Code'].agg(lambda x : x.count())
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


JNNUCHInSevF_diagsur = bfreq.merge(bsev,on='DRGNO',how='left').merge(dcodenamefreq,on='DRGNO',how='left').merge(scodenamefreq,on='DRGNO',how='left')
JNNUCHInSevF_diagsur.drop(['Frequency', 'Ratio', 'Dtot', 'Stot'], axis=1, inplace=True)
JNNUCHInSevF_diagsur.reset_index(drop=True, inplace=True)
JNNUCHInSevF_diagsur['Dcode'].fillna('NoDiag', inplace=True)
JNNUCHInSevF_diagsur['Dname'].fillna('NoDiag', inplace=True)
JNNUCHInSevF_diagsur['Dfreq'].fillna(0, inplace=True)
JNNUCHInSevF_diagsur['Dratio'].fillna('0.0 %', inplace=True)
JNNUCHInSevF_diagsur['Scode'].fillna('NoSur', inplace=True)
JNNUCHInSevF_diagsur['Sname'].fillna('NoSur', inplace=True)
JNNUCHInSevF_diagsur['Sfreq'].fillna(0, inplace=True)
JNNUCHInSevF_diagsur['Sratio'].fillna('0.0 %', inplace=True)

with pd.option_context('display.max_columns', None):
    print (JNNUCHInSevF_diagsur)


print(JNNUCHInSevF_diagsur.columns)

JNNUCHInSevF_diagsur50 = JNNUCHInSevF_diagsur.loc[0:49, :]
'''




JNNUCHInSevF_base50.to_csv('./JNNUCH/JNNUCH 입원환자 중증도 빈도별 순위.csv', encoding='cp949', index=False)
JNNUCHInSevF_base50Sev.to_csv('./JNNUCH/JNNUCH 입원환자 중증도 빈도별 순위(전문상위50).csv', encoding='cp949', index=False)
JNNUCHInSevF_base50Norm.to_csv('./JNNUCH/JNNUCH 입원환자 중증도 빈도별 순위(일반상위50).csv', encoding='cp949', index=False)
JNNUCHInSevF_base50Simp.to_csv('./JNNUCH/JNNUCH 입원환자 중증도 빈도별 순위(단순상위50).csv', encoding='cp949', index=False)
#JNNUCHInSevF_diagsur50.to_csv('./JNNUCH/JNNUCHInSevF_diagsur50.csv', index=False)

JNNUCHInSevF_base.to_csv('./JNNUCH/(원)JNNUCH 입원환자 중증도 빈도별 순위.csv', encoding='cp949', index=False)
JNNUCHInSevF_baseSev.to_csv('./JNNUCH/(원)JNNUCH 입원환자 중증도 빈도별 순위(전문상위50).csv', encoding='cp949', index=False)
JNNUCHInSevF_baseNorm.to_csv('./JNNUCH/(원)JNNUCH 입원환자 중증도 빈도별 순위(일반상위50).csv', encoding='cp949', index=False)
JNNUCHInSevF_baseSimp.to_csv('./JNNUCH/(원)JNNUCH 입원환자 중증도 빈도별 순위(단순상위50).csv', encoding='cp949', index=False)

#JNNUCHInSevF_inst = pd.DataFrame(columns=['Rank' ,'DRGNO', 'Severity', 'A_Ratio', 'B_Ratio', 'C_Ratio', 'D_Ratio', 'E_Ratio', 'F_Ratio', 'G_Ratio', 'H_Ratio'])
#JNNUCHInSevF_regn = pd.DataFrame(columns=['Rank' ,'DRGNO', 'Severity', 'fSeoul', 'fBusan', 'fDaegu', 'fGwangju', 'fDaejeon', 'fIncheon', 'fJeju', 'fSejong', 'fJeonnam', 'fJeonbuk',
#                                      'fGyeongnam', 'fGyeongbuk', 'fChungnam', 'fChungbuk', 'fGangwon', 'fGyeonggi'])



