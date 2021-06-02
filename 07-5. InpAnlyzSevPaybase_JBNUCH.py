'''
This script is written 4 anyalyzing JBNUCH's Inpatients' Data based on 'DRGNO by the order of payment'.

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

JBNUCHIn = pd.read_csv("./JBNUCH/JBNUCHInP_R4A.csv", encoding="utf-8")
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

print(JBNUCHIn.info())
print(JBNUCHIn.columns)


############################# Analyzing #############################


## RAW DATA based on DRGNO by the order of payment: append rows
JBNUCHIn4SevPbase = JBNUCHIn.copy()
#JBNUCHIn4SevPbase.DRGNO = JBNUCHIn4SevPbase.DRGNO.str.split('/')
#JBNUCHIn4SevPbase.Severity = JBNUCHIn4SevPbase.Severity.str.split('/')
#JBNUCHIn4SevPbase = JBNUCHIn4SevPbase.apply(pd.Series.explode).reset_index(drop=True)
print(JBNUCHIn4SevPbase.info())
print(JBNUCHIn4SevPbase.columns)
'''
JBNUCHIn4SevPbase['Severity'] = JBNUCHIn4SevPbase['Severity'].map({'Severe' : 4, 'Normal' : 3, 'Simple' : 2, 'SortError' : 1})

JBNUCHIn4SevPbasesub = JBNUCHIn4SevPbase.copy()
JBNUCHIn4SevPbase = JBNUCHIn4SevPbase.groupby(['PT_No'], as_index= False)['Severity'].agg(lambda x: x.max())
JBNUCHIn4SevPbase= JBNUCHIn4SevPbase.merge(JBNUCHIn4SevPbasesub, on=['PT_No', 'Severity'], how='left')
JBNUCHIn4SevPbase.drop_duplicates(subset =['PT_No', 'Severity'], inplace = True)
JBNUCHIn4SevPbase.reset_index(drop=True, inplace=True)

JBNUCHIn4SevPbase['Severity'] = JBNUCHIn4SevPbase['Severity'].map({4:'Severe', 3:'Normal', 2:'Simple', 1:'SortError'})
'''

DRGCNmain5.rename(columns={DRGCNmain5.columns[0] : 'DRGNO01', DRGCNmain5.columns[1] : 'DRGName01'}, inplace=True)
JBNUCHIn4SevPbase['DRGNO01'] = JBNUCHIn4SevPbase['DRGNO'].astype(str).str[0:5]
JBNUCHIn4SevPbase['DRGNO01'].fillna('NoDRG', inplace=True)

DRGCNsub4.rename(columns={DRGCNsub4.columns[0] : 'DRGNO02', DRGCNsub4.columns[1] : 'DRGName02'}, inplace=True)
JBNUCHIn4SevPbase['DRGNO02'] = JBNUCHIn4SevPbase['DRGNO'].astype(str).str[0:4]
JBNUCHIn4SevPbase['DRGNO02'].fillna('NoDRG', inplace=True)

JBNUCHIn4SevPbase = JBNUCHIn4SevPbase.merge(DRGCNmain5, on='DRGNO01', how='left').merge(DRGCNsub4, on='DRGNO02', how='left')
JBNUCHIn4SevPbase.reset_index(drop=True, inplace=True)

JBNUCHIn4SevPbase.loc[(JBNUCHIn4SevPbase['DRGName01'].isna() &  JBNUCHIn4SevPbase['DRGName02'].notna()), 'DRGName01'] = JBNUCHIn4SevPbase['DRGName02']
JBNUCHIn4SevPbase.rename(columns={'DRGName01' : 'DRGName'}, inplace=True)
del JBNUCHIn4SevPbase['DRGNO01']; del JBNUCHIn4SevPbase['DRGNO02']; del JBNUCHIn4SevPbase['DRGName02']
JBNUCHIn4SevPbase.DRGName.fillna('NoDRG', inplace=True)

JBNUCHIn4SevPbase.loc[(JBNUCHIn4SevPbase['DRGName']  == 'NoDRG'), 'DRGNO'] = 'NoDRG'
JBNUCHIn4SevPbase = JBNUCHIn4SevPbase[JBNUCHIn4SevPbase.D_Code != 'NoDiag']
print(JBNUCHIn4SevPbase.info())

JBNUCHIn4SevPbase['TotalPay'] = JBNUCHIn4SevPbase['Pay_InsSelf'] + JBNUCHIn4SevPbase['Pay_InsCorp'] + JBNUCHIn4SevPbase['Pay_NoIns'] + JBNUCHIn4SevPbase['Pay_Sel']

bpay=JBNUCHIn4SevPbase.groupby(by='DRGNO', as_index=False).agg({'TotalPay': pd.Series.mean})
bpay = bpay[bpay.DRGNO != 'NoDRG']
bpay.sort_values(by='TotalPay', ascending=False, inplace=True)
bpay.reset_index(drop=True, inplace=True)
bpay.index += 1
bpay = bpay.rename_axis('Rank').reset_index()
bpay['TotalPay'] = bpay['TotalPay'].astype(int)
print (bpay)

bdname=JBNUCHIn4SevPbase.groupby(by='DRGNO', as_index=False).agg({'DRGName': lambda a: a.value_counts().index[0]})
bdname = bdname[bdname.DRGNO != 'NoDRG']
bdname.reset_index(drop=True, inplace=True)
#print (bdname)


bsev=JBNUCHIn4SevPbase.groupby(by='DRGNO', as_index=False).agg({'Severity': pd.Series.unique})
bsev.reset_index(drop=True, inplace=True)
print (bsev)


## 01. BASIC INFORMATION by Severity Payment
pnum=JBNUCHIn4SevPbase.groupby(by='DRGNO', as_index=False).agg({'PT_No': pd.Series.nunique})
pnum.rename(columns={'PT_No':'P_Num'}, inplace=True)
pnum['P_Num'] = pnum['P_Num'].astype(int)
pnum.reset_index(drop=True, inplace=True)
print (pnum)

avage=JBNUCHIn4SevPbase.groupby(by='DRGNO', as_index=False).agg({'Age': pd.Series.mean})
avage.rename(columns={'Age':'AvAge'}, inplace=True)
avage.AvAge = avage.AvAge.round(1)
avage.reset_index(drop=True, inplace=True)
print (avage)

avinprd=JBNUCHIn4SevPbase.groupby(by='DRGNO', as_index=False).agg({'In_Prd': pd.Series.mean})
avinprd.rename(columns={'In_Prd':'AvInPrd'}, inplace=True)
avinprd.AvInPrd = avinprd.AvInPrd.round(1)
avinprd.reset_index(drop=True, inplace=True)
print (avinprd)

JBNUCHIn4SevPbase_Ins = JBNUCHIn4SevPbase.copy()
JBNUCHIn4SevPbase_Ins['IndPaidExp'] = JBNUCHIn4SevPbase_Ins['Pay_InsSelf'] + JBNUCHIn4SevPbase_Ins['Pay_NoIns'] + JBNUCHIn4SevPbase_Ins['Pay_Sel']
inspaid = JBNUCHIn4SevPbase_Ins.groupby(by='DRGNO', as_index=False).agg({'IndPaidExp' : pd.Series.mean, 'Pay_InsCorp' : pd.Series.mean})
inspaid.rename(columns={'Pay_InsCorp':'CorpPaidExp'}, inplace=True)
inspaid['IndPaidExp'] = inspaid['IndPaidExp'].astype(int)
inspaid['CorpPaidExp'] = inspaid['CorpPaidExp'].astype(int)
inspaid['IndPaidExp'] = inspaid.apply(lambda x: "{:,}".format(x['IndPaidExp']), axis=1)
inspaid['CorpPaidExp'] = inspaid.apply(lambda x: "{:,}".format(x['CorpPaidExp']), axis=1)
inspaid.reset_index(drop=True, inplace=True)
print (inspaid)


JBNUCHInSevP_base = bpay.merge(bdname,on='DRGNO',how='left').merge(bsev,on='DRGNO',how='left').merge(pnum,on='DRGNO',how='left').merge(avage,on='DRGNO',how='left').merge(avinprd,on='DRGNO',how='left').merge(inspaid,on='DRGNO',how='left')
JBNUCHInSevP_base = JBNUCHInSevP_base[['Rank', 'DRGNO', 'DRGName', 'Severity', 'TotalPay', 'IndPaidExp', 'CorpPaidExp', 'P_Num', 'AvAge', 'AvInPrd']]
JBNUCHInSevP_base.reset_index(drop=True, inplace=True)
print(JBNUCHInSevP_base)
print(JBNUCHInSevP_base.columns)


JBNUCHInSevP_base.Severity = JBNUCHInSevP_base.Severity.map({'Severe' : '전문', 'Normal' : '일반', 'Simple' : '단순', 'SortError' : '분류오류'})


JBNUCHInSevP_baseSev = JBNUCHInSevP_base[JBNUCHInSevP_base.Severity == '전문']
JBNUCHInSevP_baseSev = JBNUCHInSevP_baseSev.sort_values(by ='TotalPay', ascending = 0)
JBNUCHInSevP_baseSev.drop(['Rank'], axis=1, inplace=True)
JBNUCHInSevP_baseSev.reset_index(drop=True, inplace=True)
JBNUCHInSevP_baseSev.index += 1
JBNUCHInSevP_baseSev['TotalPay'] = JBNUCHInSevP_baseSev.apply(lambda x: "{:,}".format(x['TotalPay']), axis=1)
JBNUCHInSevP_baseSev = JBNUCHInSevP_baseSev.rename_axis('Rank').reset_index()


JBNUCHInSevP_baseNorm = JBNUCHInSevP_base[JBNUCHInSevP_base.Severity == '일반']
JBNUCHInSevP_baseNorm = JBNUCHInSevP_baseNorm.sort_values(by ='TotalPay', ascending = 0)
JBNUCHInSevP_baseNorm.drop(['Rank'], axis=1, inplace=True)
JBNUCHInSevP_baseNorm.reset_index(drop=True, inplace=True)
JBNUCHInSevP_baseNorm.index += 1
JBNUCHInSevP_baseNorm['TotalPay'] = JBNUCHInSevP_baseNorm.apply(lambda x: "{:,}".format(x['TotalPay']), axis=1)
JBNUCHInSevP_baseNorm = JBNUCHInSevP_baseNorm.rename_axis('Rank').reset_index()


JBNUCHInSevP_baseSimp = JBNUCHInSevP_base[JBNUCHInSevP_base.Severity == '단순']
JBNUCHInSevP_baseSimp = JBNUCHInSevP_baseSimp.sort_values(by ='TotalPay', ascending = 0)
JBNUCHInSevP_baseSimp.drop(['Rank'], axis=1, inplace=True)
JBNUCHInSevP_baseSimp.reset_index(drop=True, inplace=True)
JBNUCHInSevP_baseSimp.index += 1
JBNUCHInSevP_baseSimp['TotalPay'] = JBNUCHInSevP_baseSimp.apply(lambda x: "{:,}".format(x['TotalPay']), axis=1)
JBNUCHInSevP_baseSimp = JBNUCHInSevP_baseSimp.rename_axis('Rank').reset_index()


JBNUCHInSevP_base['TotalPay'] = JBNUCHInSevP_base.apply(lambda x: "{:,}".format(x['TotalPay']), axis=1)


JBNUCHInSevP_base = JBNUCHInSevP_base.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'TotalPay':'총부담금(원)',
                                                    'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)'})
JBNUCHInSevP_baseSev = JBNUCHInSevP_baseSev.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'TotalPay':'총부담금(원)',
                                                          'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)'})
JBNUCHInSevP_baseNorm = JBNUCHInSevP_baseNorm.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'TotalPay':'총부담금(원)',
                                                            'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)'})
JBNUCHInSevP_baseSimp = JBNUCHInSevP_baseSimp.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'TotalPay':'총부담금(원)',
                                                            'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)'})

print(JBNUCHInSevP_base.columns)

JBNUCHInSevP_base50 = JBNUCHInSevP_base.loc[0:49, :]
JBNUCHInSevP_base50Sev = JBNUCHInSevP_baseSev.loc[0:49, :]
JBNUCHInSevP_base50Norm = JBNUCHInSevP_baseNorm.loc[0:49, :]
JBNUCHInSevP_base50Simp = JBNUCHInSevP_baseSimp.loc[0:49, :]



'''
# No Executuon
## 02. DIAGNOSIS & SURGERY INFORMATION by Severity Payment
JBNUCHIn4SevPbase_Diag = JBNUCHIn4SevPbase.copy()
JBNUCHIn4SevPbase_Diag.D_Code = JBNUCHIn4SevPbase_Diag.D_Code.str.split('/')
JBNUCHIn4SevPbase_Diag.D_Name = JBNUCHIn4SevPbase_Diag.D_Name.str.split('/')
JBNUCHIn4SevPbase_Diag = JBNUCHIn4SevPbase_Diag.apply(pd.Series.explode).reset_index(drop=True)
JBNUCHIn4SevPbase_Diag = JBNUCHIn4SevPbase_Diag.drop_duplicates(['PT_No', 'In_Date', 'DRGNO', 'Severity', 'D_Code', 'D_Name'], keep='first') # Need D_Date after for proper drop
JBNUCHIn4SevPbase_Diag = JBNUCHIn4SevPbase_Diag[JBNUCHIn4SevPbase_Diag.D_Code != 'NoDiag']

##### another way to selcet most frequent value using groupby(Not Accurate in Certain Condition)#####
#dcodename = JBNUCHIn4SevPbase_Diag.groupby(by='DRGNO', as_index=False).agg({'D_Code': lambda a: ' / '.join(pd.Series.mode(a)), 'D_Name': lambda b: ' / '.join(pd.Series.mode(b))})


dcode = JBNUCHIn4SevPbase_Diag.groupby(by='DRGNO', as_index=False)['D_Code'].agg(lambda x:x.value_counts().index[0])
dname = JBNUCHIn4SevPbase_Diag.groupby(by='D_Code', as_index=False).agg({'D_Name': lambda a: '/'.join(pd.unique(a))})
dcode.reset_index(drop=True, inplace=True)
dname.reset_index(drop=True, inplace=True)
dcodename = dcode.merge(dname, on='D_Code', how='left')
dcodename.reset_index(drop=True, inplace=True)
dfreq = JBNUCHIn4SevPbase_Diag.groupby(by='DRGNO', as_index=False)['D_Code'].agg(lambda x: x.value_counts().head(1))
dfreq.rename(columns={'D_Code':'Dfreq'}, inplace=True)
dfreq['Dfreq'] = dfreq['Dfreq'].astype('int64')
dfreq.reset_index(drop=True, inplace=True)
dtot = JBNUCHIn4SevPbase_Diag.groupby(by='DRGNO', as_index=False)['D_Code'].agg(lambda x: x.count())
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


JBNUCHIn4SevPbase_Sur = JBNUCHIn4SevPbase.copy()
JBNUCHIn4SevPbase_Sur.Sur_Date = JBNUCHIn4SevPbase_Sur.Sur_Date.str.split('/')
JBNUCHIn4SevPbase_Sur.Sur_Code = JBNUCHIn4SevPbase_Sur.Sur_Code.str.split('/')
JBNUCHIn4SevPbase_Sur.Sur_Name = JBNUCHIn4SevPbase_Sur.Sur_Name.str.split('/')
JBNUCHIn4SevPbase_Sur = JBNUCHIn4SevPbase_Sur.apply(pd.Series.explode).reset_index(drop=True)
JBNUCHIn4SevPbase_Sur = JBNUCHIn4SevPbase_Sur.drop_duplicates(['PT_No', 'In_Date', 'DRGNO', 'Severity', 'Sur_Date', 'Sur_Code', 'Sur_Name'], keep='first')
JBNUCHIn4SevPbase_OnlySur = JBNUCHIn4SevPbase_Sur[JBNUCHIn4SevPbase_Sur.Sur_Code != 'NoSur']
JBNUCHIn4SevPbase_OnlySur = JBNUCHIn4SevPbase_OnlySur[JBNUCHIn4SevPbase_OnlySur.Sur_Code != 'GroupPay_JBNUCH']

##### another way to selcet most frequent value using groupby(Not Accurate in Certain Condition)#####
#scodename = JBNUCHIn4SevPbase_Sur.groupby(by='DRGNO', as_index=False).agg({'Sur_Code': lambda a: ' / '.join(pd.Series.mode(a)), 'Sur_Name' : lambda b: ' / '.join(pd.Series.mode(b))})


scode = JBNUCHIn4SevPbase_OnlySur.groupby(by='DRGNO', as_index=False)['Sur_Code'].agg(lambda x : x.value_counts().index[0])
sname = JBNUCHIn4SevPbase_OnlySur.groupby(by='Sur_Code', as_index=False)['Sur_Name'].agg(lambda x : x.value_counts().index[0])
scode.reset_index(drop=True, inplace=True)
sname.reset_index(drop=True, inplace=True)
scodename = scode.merge(sname, on='Sur_Code', how='left')
scodename.reset_index(drop=True, inplace=True)
sfreq = JBNUCHIn4SevPbase_OnlySur.groupby(by='DRGNO', as_index=False)['Sur_Code'].agg(lambda x : x.value_counts().head(1))
sfreq.rename(columns={'Sur_Code':'Sfreq'}, inplace=True)
sfreq['Sfreq'] = sfreq['Sfreq'].astype('int64')
sfreq.reset_index(drop=True, inplace=True)
stot = JBNUCHIn4SevPbase_OnlySur.groupby(by='DRGNO', as_index=False)['Sur_Code'].agg(lambda x : x.count())
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


JBNUCHInSevP_diagsur = bpay.merge(bsev,on='DRGNO',how='left').merge(dcodenamefreq,on='DRGNO',how='left').merge(scodenamefreq,on='DRGNO',how='left')
JBNUCHInSevP_diagsur.drop(['TotalPay' ,'Dtot', 'Stot'], axis=1, inplace=True)
JBNUCHInSevP_diagsur.reset_index(drop=True, inplace=True)
JBNUCHInSevP_diagsur['Dcode'].fillna('NoDiag', inplace=True)
JBNUCHInSevP_diagsur['Dname'].fillna('NoDiag', inplace=True)
JBNUCHInSevP_diagsur['Dfreq'].fillna(0, inplace=True)
JBNUCHInSevP_diagsur['Dratio'].fillna('0.0 %', inplace=True)
JBNUCHInSevP_diagsur['Scode'].fillna('NoSur', inplace=True)
JBNUCHInSevP_diagsur['Sname'].fillna('NoSur', inplace=True)
JBNUCHInSevP_diagsur['Sfreq'].fillna(0, inplace=True)
JBNUCHInSevP_diagsur['Sratio'].fillna('0.0 %', inplace=True)

with pd.option_context('display.max_columns', None):
    print (JBNUCHInSevP_diagsur)

print(JBNUCHInSevP_diagsur.columns)

JBNUCHInSevP_diagsur50 = JBNUCHInSevP_diagsur.loc[0:49, :]
'''


JBNUCHInSevP_base50.to_csv('./JBNUCH/JBNUCH 입원환자 중증도 총부담금별 순위.csv', encoding='cp949', index=False)
JBNUCHInSevP_base50Sev.to_csv('./JBNUCH/JBNUCH 입원환자 중증도 총부담금별 순위(전문상위50).csv', encoding='cp949', index=False)
JBNUCHInSevP_base50Norm.to_csv('./JBNUCH/JBNUCH 입원환자 중증도 총부담금별 순위(일반상위50).csv', encoding='cp949', index=False)
JBNUCHInSevP_base50Simp.to_csv('./JBNUCH/JBNUCH 입원환자 중증도 총부담금별 순위(단순상위50).csv', encoding='cp949', index=False)
#JBNUCHInSevP_diagsur50.to_csv('./JBNUCH/JBNUCHInSevP_diagsur50.csv', index=False)

JBNUCHInSevP_base.to_csv('./JBNUCH/(원)JBNUCH 입원환자 중증도 총부담금별 순위.csv', encoding='cp949', index=False)
JBNUCHInSevP_baseSev.to_csv('./JBNUCH/(원)JBNUCH 입원환자 중증도 총부담금별 순위(전문상위50).csv', encoding='cp949', index=False)
JBNUCHInSevP_baseNorm.to_csv('./JBNUCH/(원)JBNUCH 입원환자 중증도 총부담금별 순위(일반상위50).csv', encoding='cp949', index=False)
JBNUCHInSevP_baseSimp.to_csv('./JBNUCH/(원)JBNUCH 입원환자 중증도 총부담금별 순위(단순상위50).csv', encoding='cp949', index=False)

#JBNUCHInSevP_inst = pd.DataFrame(columns=['Rank' ,'DRGNO', 'Severity', 'A_Ratio', 'B_Ratio', 'C_Ratio', 'D_Ratio', 'E_Ratio', 'F_Ratio', 'G_Ratio', 'H_Ratio'])
#JBNUCHInSevP_regn = pd.DataFrame(columns=['Rank' ,'DRGNO', 'Severity', 'fSeoul', 'fBusan', 'fDaegu', 'fGwangju', 'fDaejeon', 'fIncheon', 'fJeju', 'fSejong', 'fJeonnam', 'fJeonbuk',
#                                      'fGyeongnam', 'fGyeongbuk', 'fChungnam', 'fChungbuk', 'fGangwon', 'fGyeonggi'])



