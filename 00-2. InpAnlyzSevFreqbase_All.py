'''
This script is written 4 anyalyzing All's Inpatients' Data based on 'DRGNO Frequency and highest '.

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
#KNUCHIn = pd.read_csv("./KNUCH/KNUCHInP_R4A.csv", encoding="utf-8")
#SACHIn = pd.read_csv("./SACH/SACHInP_R4A.csv", encoding="utf-8")
#SCHIn = pd.read_csv("./SCH/SCHInP_R4A.csv", encoding="utf-8")
PNUCHIn = pd.read_csv("./PNUCH/PNUCHInP_R4A.csv", encoding="utf-8")
JNNUCHIn = pd.read_csv("./JNNUCH/JNNUCHInP_R4A.csv", encoding="utf-8")
JBNUCHIn = pd.read_csv("./JBNUCH/JBNUCHInP_R4A.csv", encoding="utf-8")
KBNUCHIn = pd.read_csv("./KBNUCH/KBNUCHInP_R4A.csv", encoding="utf-8")

DiagCN = pd.read_csv("./master_Dcode&Dname.csv", encoding="utf-8")
SurCN = pd.read_csv("./master_Scode&Sname.csv", encoding="utf-8")
DRGCNmain5 = pd.read_csv("./SevCri_5digit(DRGname).csv", encoding="utf-8")
DRGCNsub4 = pd.read_csv("./SevCri_4digit(DRGname).csv", encoding="utf-8")


DiagCN.drop(['Dname(ENG)'], axis=1, inplace=True)
DiagCN.rename(columns={'Dcode' : 'D_Code', 'Dname(KOR)' :'D_Name'}, inplace=True)
DiagCN=DiagCN.drop_duplicates(['D_Code'], keep='first')

SurCN = SurCN.rename(columns={'Scode':'Sur_Code', 'Sname':'Sur_Name'})
SurCN=SurCN.drop_duplicates(['Sur_Code'], keep='first')

DRGCNmain5['DRGNO'] = DRGCNmain5['DRGNO'].astype(str).str[0:4]
DRGCNmain5['DRGname'] = DRGCNmain5['DRGname'].str.split(', 연령').str[0]
DRGCNmain5=DRGCNmain5.drop_duplicates(['DRGNO'], keep='first')


DRGCNsub4=DRGCNsub4.drop_duplicates(['DRGNO'], keep='first')


## unite columns
SNUCHIn.drop(columns=['Birth'], inplace=True)
# KNUCHIn.drop(columns=['Birth'], inplace=True)
# SACHIn.drop(columns=['Birth'], inplace=True)
# SCHIn.drop(columns=['Birth'], inplace=True)
PNUCHIn.drop(columns=['Birth'], inplace=True)
# JNNUCHIn.drop(columns=['Birth'], inplace=True)
JBNUCHIn.drop(columns=['Birth'], inplace=True)
KBNUCHIn.drop(columns=['Birth'], inplace=True)


## DRGNO 4digit
SNUCHIn['DRGNO'] = SNUCHIn['DRGNO'].astype(str).str[0:4]
#KNUCHIn['DRGNO'] = KNUCHIn['DRGNO'].astype(str).str[0:4]
#SACHIn['DRGNO'] = SACHIn['DRGNO'].astype(str).str[0:4]
#SCHIn['DRGNO'] = SCHIn['DRGNO'].astype(str).str[0:4]
PNUCHIn['DRGNO'] = PNUCHIn['DRGNO'].astype(str).str[0:4]
JNNUCHIn['DRGNO'] = JNNUCHIn['DRGNO'].astype(str).str[0:4]
JBNUCHIn['DRGNO'] = JBNUCHIn['DRGNO'].astype(str).str[0:4]
KBNUCHIn['DRGNO'] = KBNUCHIn['DRGNO'].astype(str).str[0:4]

print(SNUCHIn.columns)
#print(KNUCHIn.columns)
#print(SACHIn.columns)
#print(SCHIn.columns)
print(PNUCHIn.columns)
print(JNNUCHIn.columns)
print(JBNUCHIn.columns)
print(KBNUCHIn.columns)


SubIns_Cri = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018-2019SubIns_Cri.csv", encoding="utf-8", low_memory=False)

AllIn = pd.concat([SNUCHIn,
#                   KNUCHIn,
#                   SACHIn,
#                   SCHIn,
                   PNUCHIn,
                   JNNUCHIn,
                   JBNUCHIn,
                   KBNUCHIn], ignore_index=True)

print(AllIn.info())
print(AllIn.columns)

AllIn['PT_No'] = AllIn['PT_No'].astype(str)
AllIn['PT_No'] = AllIn[['InstName', 'PT_No']].apply(lambda x: ''.join(x), axis=1)


AllIn.to_csv('./All/AllInP_R4A.csv', index=False)



############################# Analyzing #############################


## RAW DATA based one Severity Frequency: append rows
AllIn4SevFbase = AllIn.copy()
#AllIn4SevFbase.DRGNO = AllIn4SevFbase.DRGNO.str.split('/')
#AllIn4SevFbase.Severity = AllIn4SevFbase.Severity.str.split('/')
#AllIn4SevFbase = AllIn4SevFbase.apply(pd.Series.explode).reset_index(drop=True)
print(AllIn4SevFbase.info())
print(AllIn4SevFbase.columns)


DRGCNmain5.rename(columns={DRGCNmain5.columns[0] : 'DRGNO01', DRGCNmain5.columns[1] : 'DRGName01'}, inplace=True)
AllIn4SevFbase['DRGNO01'] = AllIn4SevFbase['DRGNO'].astype(str).str[0:4]
AllIn4SevFbase['DRGNO01'].fillna('NoDRG', inplace=True)

DRGCNsub4.rename(columns={DRGCNsub4.columns[0] : 'DRGNO02', DRGCNsub4.columns[1] : 'DRGName02'}, inplace=True)
AllIn4SevFbase['DRGNO02'] = AllIn4SevFbase['DRGNO'].astype(str).str[0:4]
AllIn4SevFbase['DRGNO02'].fillna('NoDRG', inplace=True)

AllIn4SevFbase = AllIn4SevFbase.merge(DRGCNmain5, on='DRGNO01', how='left').merge(DRGCNsub4, on='DRGNO02', how='left')
AllIn4SevFbase.reset_index(drop=True, inplace=True)

AllIn4SevFbase.loc[(AllIn4SevFbase['DRGName01'].isna() &  AllIn4SevFbase['DRGName02'].notna()), 'DRGName01'] = AllIn4SevFbase['DRGName02']
AllIn4SevFbase.rename(columns={'DRGName01' : 'DRGName'}, inplace=True)
del AllIn4SevFbase['DRGNO01']; del AllIn4SevFbase['DRGNO02']; del AllIn4SevFbase['DRGName02']
AllIn4SevFbase.DRGName.fillna('NoDRG', inplace=True)

AllIn4SevFbase.loc[(AllIn4SevFbase['DRGName']  == 'NoDRG'), 'DRGNO'] = 'NoDRG'
AllIn4SevFbase = AllIn4SevFbase[AllIn4SevFbase.D_Code != 'NoDiag']
print(AllIn4SevFbase.info())

bfreq=AllIn4SevFbase.groupby(by='DRGNO', as_index=False).agg({'Severity': pd.Series.count})
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

bdname=AllIn4SevFbase.groupby(by='DRGNO', as_index=False).agg({'DRGName': lambda a: a.value_counts().index[0]})
bdname = bdname[bdname.DRGNO != 'NoDRG']
bdname.reset_index(drop=True, inplace=True)
#print (bdname)

AllIn4SevFbase_tobsev = AllIn4SevFbase.copy()
AllIn4SevFbase_tobsev['Severity'] = AllIn4SevFbase_tobsev['Severity'].map({'Severe' : 4, 'Normal' : 3, 'Simple' : 2, 'SortError' : 1, 'NoDRG' : 0})

AllIn4SevFbase_tobsevsub = AllIn4SevFbase_tobsev.copy()
AllIn4SevFbase_tobsev = AllIn4SevFbase_tobsev.groupby(['DRGNO'], as_index= False)['Severity'].agg(lambda x: x.max())
AllIn4SevFbase_tobsev= AllIn4SevFbase_tobsev.merge(AllIn4SevFbase_tobsevsub, on=['DRGNO', 'Severity'], how='left')
AllIn4SevFbase_tobsev.drop_duplicates(subset =['PT_No', 'In_Date', 'Severity'], inplace = True)
AllIn4SevFbase_tobsev.reset_index(drop=True, inplace=True)

AllIn4SevFbase_tobsev['Severity'] = AllIn4SevFbase_tobsev['Severity'].map({4:'Severe', 3:'Normal', 2:'Simple', 1:'SortError', 0:'NoDRG'})

bsev=AllIn4SevFbase_tobsev.groupby(by='DRGNO', as_index=False).agg({'Severity': pd.Series.unique})
bsev.reset_index(drop=True, inplace=True)
print (bsev)


## 01. BASIC INFORMATION by Severity Frequency
pnum=AllIn4SevFbase.groupby(by='DRGNO', as_index=False).agg({'PT_No': pd.Series.nunique})
pnum.rename(columns={'PT_No':'P_Num'}, inplace=True)
pnum['P_Num'] = pnum['P_Num'].astype(int)
pnum.reset_index(drop=True, inplace=True)
print (pnum)

avage=AllIn4SevFbase.groupby(by='DRGNO', as_index=False).agg({'Age': pd.Series.mean})
avage.rename(columns={'Age':'AvAge'}, inplace=True)
avage.AvAge = avage.AvAge.round(1)
avage.reset_index(drop=True, inplace=True)
print (avage)

avinprd=AllIn4SevFbase.groupby(by='DRGNO', as_index=False).agg({'In_Prd': pd.Series.mean})
avinprd.rename(columns={'In_Prd':'AvInPrd'}, inplace=True)
avinprd.AvInPrd = avinprd.AvInPrd.round(1)
avinprd.reset_index(drop=True, inplace=True)
print (avinprd)

AllIn4SevFbase_Ins = AllIn4SevFbase.copy()
AllIn4SevFbase_Ins['IndPaidExp'] = AllIn4SevFbase_Ins['Pay_InsSelf'] + AllIn4SevFbase_Ins['Pay_NoIns'] + AllIn4SevFbase_Ins['Pay_Sel']
inspaid = AllIn4SevFbase_Ins.groupby(by='DRGNO', as_index=False).agg({'IndPaidExp' : pd.Series.mean, 'Pay_InsCorp' : pd.Series.mean})
inspaid.rename(columns={'Pay_InsCorp':'CorpPaidExp'}, inplace=True)
inspaid['IndPaidExp'] = inspaid['IndPaidExp'].astype(int)
inspaid['CorpPaidExp'] = inspaid['CorpPaidExp'].astype(int)
inspaid['IndPaidExp'] = inspaid.apply(lambda x: "{:,}".format(x['IndPaidExp']), axis=1)
inspaid['CorpPaidExp'] = inspaid.apply(lambda x: "{:,}".format(x['CorpPaidExp']), axis=1)
inspaid.reset_index(drop=True, inplace=True)
print (inspaid)


AllInSevF_base = bfreq.merge(bdname,on='DRGNO',how='left').merge(bsev,on='DRGNO',how='left').merge(pnum,on='DRGNO',how='left').merge(avage,on='DRGNO',how='left').merge(avinprd,on='DRGNO',how='left').merge(inspaid,on='DRGNO',how='left')
AllInSevF_base = AllInSevF_base[['Rank', 'DRGNO', 'DRGName', 'Severity', 'Frequency', 'Ratio', 'P_Num', 'AvAge', 'AvInPrd', 'IndPaidExp', 'CorpPaidExp']]
AllInSevF_base.reset_index(drop=True, inplace=True)
print(AllInSevF_base)
print(AllInSevF_base.columns)


AllInSevF_base.Severity = AllInSevF_base.Severity.map({'Severe' : '전문', 'Normal' : '일반', 'Simple' : '단순', 'SortError' : '분류오류'})


AllInSevF_baseSev = AllInSevF_base[AllInSevF_base.Severity == '전문']
AllInSevF_baseSev = AllInSevF_baseSev.sort_values(by ='Frequency', ascending = 0)
AllInSevF_baseSev.drop(['Rank'], axis=1, inplace=True)
AllInSevF_baseSev.reset_index(drop=True, inplace=True)
AllInSevF_baseSev.index += 1
AllInSevF_baseSev = AllInSevF_baseSev.rename_axis('Rank').reset_index()


AllInSevF_baseNorm = AllInSevF_base[AllInSevF_base.Severity == '일반']
AllInSevF_baseNorm = AllInSevF_baseNorm.sort_values(by ='Frequency', ascending = 0)
AllInSevF_baseNorm.drop(['Rank'], axis=1, inplace=True)
AllInSevF_baseNorm.reset_index(drop=True, inplace=True)
AllInSevF_baseNorm.index += 1
AllInSevF_baseNorm = AllInSevF_baseNorm.rename_axis('Rank').reset_index()


AllInSevF_baseSimp = AllInSevF_base[AllInSevF_base.Severity == '단순']
AllInSevF_baseSimp = AllInSevF_baseSimp.sort_values(by ='Frequency', ascending = 0)
AllInSevF_baseSimp.drop(['Rank'], axis=1, inplace=True)
AllInSevF_baseSimp.reset_index(drop=True, inplace=True)
AllInSevF_baseSimp.index += 1
AllInSevF_baseSimp = AllInSevF_baseSimp.rename_axis('Rank').reset_index()




AllInSevF_base = AllInSevF_base.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'Frequency':'빈도(건)',
                                                    'Ratio':'비율(%)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)', 'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)'})
AllInSevF_baseSev = AllInSevF_baseSev.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'Frequency':'빈도(건)',
                                                          'Ratio':'비율(%)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)', 'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)'})
AllInSevF_baseNorm = AllInSevF_baseNorm.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'Frequency':'빈도(건)',
                                                            'Ratio':'비율(%)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)', 'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)'})
AllInSevF_baseSimp = AllInSevF_baseSimp.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'Frequency':'빈도(건)',
                                                            'Ratio':'비율(%)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)', 'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)'})

print(AllInSevF_base.columns)

AllInSevF_base50 = AllInSevF_base.loc[0:49, :]
AllInSevF_base50Sev = AllInSevF_baseSev.loc[0:49, :]
AllInSevF_base50Norm = AllInSevF_baseNorm.loc[0:49, :]
AllInSevF_base50Simp = AllInSevF_baseSimp.loc[0:49, :]

#AllInSevF_base.Severity = AllInSevF_base.Severity.map({'Severe' : '전문', 'Normal' : '일반', 'Simple' : '단순', 'SortError' : '분류오류'})

'''
AllInSevF_baseSev = AllInSevF_base[AllInSevF_base.Severity == '전문']
AllInSevF_baseSev = AllInSevF_baseSev.sort_values(by ='Frequency', ascending = 0)
AllInSevF_baseSev.drop(['Rank'], axis=1, inplace=True)
AllInSevF_baseSev.reset_index(drop=True, inplace=True)
AllInSevF_baseSev.index += 1
AllInSevF_baseSev = AllInSevF_baseSev.rename_axis('Rank').reset_index()


AllInSevF_baseNorm = AllInSevF_base[AllInSevF_base.Severity == '일반']
AllInSevF_baseNorm = AllInSevF_baseNorm.sort_values(by ='Frequency', ascending = 0)
AllInSevF_baseNorm.drop(['Rank'], axis=1, inplace=True)
AllInSevF_baseNorm.reset_index(drop=True, inplace=True)
AllInSevF_baseNorm.index += 1
AllInSevF_baseNorm = AllInSevF_baseNorm.rename_axis('Rank').reset_index()


AllInSevF_baseSimp = AllInSevF_base[AllInSevF_base.Severity == '단순']
AllInSevF_baseSimp = AllInSevF_baseSimp.sort_values(by ='Frequency', ascending = 0)
AllInSevF_baseSimp.drop(['Rank'], axis=1, inplace=True)
AllInSevF_baseSimp.reset_index(drop=True, inplace=True)
AllInSevF_baseSimp.index += 1
AllInSevF_baseSimp = AllInSevF_baseSimp.rename_axis('Rank').reset_index()




AllInSevF_base = AllInSevF_base.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'Frequency':'빈도(건)',
                                                    'Ratio':'비율(%)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)', 'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)'})
AllInSevF_baseSev = AllInSevF_baseSev.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'Frequency':'빈도(건)',
                                                          'Ratio':'비율(%)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)', 'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)'})
AllInSevF_baseNorm = AllInSevF_baseNorm.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'Frequency':'빈도(건)',
                                                            'Ratio':'비율(%)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)', 'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)'})
AllInSevF_baseSimp = AllInSevF_baseSimp.rename(columns={'Rank':'순위', 'DRGNO':'DRG번호', 'DRGName':'DRG명', 'Severity':'중증도', 'Frequency':'빈도(건)',
                                                            'Ratio':'비율(%)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)', 'AvInPrd':'평균재원기간(일)', 'IndPaidExp':'본인부담금(원)', 'CorpPaidExp':'공단부담금(원)'})

print(AllInSevF_base.columns)


AllInSevF_base50Sev = AllInSevF_baseSev.loc[0:49, :]
AllInSevF_base50Norm = AllInSevF_baseNorm.loc[0:49, :]
AllInSevF_base50Simp = AllInSevF_baseSimp.loc[0:49, :]
'''
#AllInSevF_base50 = AllInSevF_base.loc[0:49, :]

'''
# No Executuon
## 02. DIAGNOSIS & SURGERY INFORMATION by Severity Frequency
AllIn4SevFbase_Diag = AllIn4SevFbase.copy()
AllIn4SevFbase_Diag.D_Code = AllIn4SevFbase_Diag.D_Code.str.split('/')
AllIn4SevFbase_Diag['D_Code'].fillna('NoDiag', inplace=True)
AllIn4SevFbase_Diag = AllIn4SevFbase_Diag.apply(pd.Series.explode).reset_index(drop=True)
AllIn4SevFbase_Diag = AllIn4SevFbase_Diag.drop_duplicates(['PT_No', 'In_Date', 'DRGNO', 'Severity', 'D_Code'], keep='first') # Need D_Date after for proper drop
AllIn4SevFbase_Diag = AllIn4SevFbase_Diag[AllIn4SevFbase_Diag.D_Code != 'NoDiag']

##### another way to selcet most frequent value using groupby(Not Accurate in Certain Condition)#####
#dcodename = AllIn4SevFbase_Diag.groupby(by='DRGNO', as_index=False).agg({'D_Code': lambda a: ' / '.join(pd.Series.mode(a)), 'D_Name': lambda b: ' / '.join(pd.Series.mode(b))})


dcode = AllIn4SevFbase_Diag.groupby(by='DRGNO', as_index=False)['D_Code'].agg(lambda x:x.value_counts().index[0])
dname = AllIn4SevFbase_Diag.groupby(by='D_Code', as_index=False).agg({'D_Name': lambda a: '/'.join(pd.unique(a))})
dcode.reset_index(drop=True, inplace=True)
dname.reset_index(drop=True, inplace=True)
dcodename = dcode.merge(dname, on='D_Code', how='left')
dcodename.reset_index(drop=True, inplace=True)
dfreq = AllIn4SevFbase_Diag.groupby(by='DRGNO', as_index=False)['D_Code'].agg(lambda x: x.value_counts().head(1))
dfreq.rename(columns={'D_Code':'Dfreq'}, inplace=True)
dfreq['Dfreq'] = dfreq['Dfreq'].astype('int64')
dfreq.reset_index(drop=True, inplace=True)
dtot = AllIn4SevFbase_Diag.groupby(by='DRGNO', as_index=False)['D_Code'].agg(lambda x: x.count())
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


AllIn4SevFbase_Sur = AllIn4SevFbase.copy()
AllIn4SevFbase_Sur.Sur_Date = AllIn4SevFbase_Sur.Sur_Date.str.split('/')
AllIn4SevFbase_Sur.Sur_Code = AllIn4SevFbase_Sur.Sur_Code.str.split('/')
AllIn4SevFbase_Sur.Sur_Name = AllIn4SevFbase_Sur.Sur_Name.str.split('/')
AllIn4SevFbase_Sur['Sur_Date'].fillna('NoSur', inplace=True)
AllIn4SevFbase_Diag['Sur_Code'].fillna('NoSur', inplace=True)
AllIn4SevFbase_Diag['Sur_Name'].fillna('NoSur', inplace=True)
AllIn4SevFbase_Sur = AllIn4SevFbase_Sur.apply(pd.Series.explode).reset_index(drop=True)
AllIn4SevFbase_Sur = AllIn4SevFbase_Sur.drop_duplicates(['PT_No', 'In_Date', 'DRGNO', 'Severity', 'Sur_Date','Sur_Code', 'Sur_Name'], keep='first')
AllIn4SevFbase_OnlySur = AllIn4SevFbase_Sur[AllIn4SevFbase_Sur.Sur_Code != 'NoSur']
AllIn4SevFbase_OnlySur = AllIn4SevFbase_OnlySur[AllIn4SevFbase_OnlySur.Sur_Code != 'GroupPay_All']


##### another way to selcet most frequent value using groupby(Not Accurate in Certain Condition)#####
#scodename = AllIn4SevFbase_Sur.groupby(by='DRGNO', as_index=False).agg({'Sur_Code': lambda a: ' / '.join(pd.Series.mode(a)), 'Sur_Name' : lambda b: ' / '.join(pd.Series.mode(b))})


scode = AllIn4SevFbase_OnlySur.groupby(by='DRGNO', as_index=False)['Sur_Code'].agg(lambda x : x.value_counts().index[0])
sname = AllIn4SevFbase_OnlySur.groupby(by='Sur_Code', as_index=False)['Sur_Name'].agg(lambda x : x.value_counts().index[0])
scode.reset_index(drop=True, inplace=True)
sname.reset_index(drop=True, inplace=True)
scodename = scode.merge(sname, on='Sur_Code', how='left')
scodename.reset_index(drop=True, inplace=True)
sfreq = AllIn4SevFbase_OnlySur.groupby(by='DRGNO', as_index=False)['Sur_Code'].agg(lambda x : x.value_counts().head(1))
sfreq.rename(columns={'Sur_Code':'Sfreq'}, inplace=True)
sfreq['Sfreq'] = sfreq['Sfreq'].astype('int64')
sfreq.reset_index(drop=True, inplace=True)
stot = AllIn4SevFbase_OnlySur.groupby(by='DRGNO', as_index=False)['Sur_Code'].agg(lambda x : x.count())
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


AllInSevF_diagsur = bfreq.merge(bsev,on='DRGNO',how='left').merge(dcodenamefreq,on='DRGNO',how='left').merge(scodenamefreq,on='DRGNO',how='left')
AllInSevF_diagsur.drop(['Frequency', 'Ratio', 'Dtot', 'Stot'], axis=1, inplace=True)
AllInSevF_diagsur.reset_index(drop=True, inplace=True)
AllInSevF_diagsur['Dcode'].fillna('NoDiag', inplace=True)
AllInSevF_diagsur['Dname'].fillna('NoDiag', inplace=True)
AllInSevF_diagsur['Dfreq'].fillna(0, inplace=True)
AllInSevF_diagsur['Dratio'].fillna('0.0 %', inplace=True)
AllInSevF_diagsur['Scode'].fillna('NoSur', inplace=True)
AllInSevF_diagsur['Sname'].fillna('NoSur', inplace=True)
AllInSevF_diagsur['Sfreq'].fillna(0, inplace=True)
AllInSevF_diagsur['Sratio'].fillna('0.0 %', inplace=True)

with pd.option_context('display.max_columns', None):
    print (AllInSevF_diagsur)


print(AllInSevF_diagsur.columns)

AllInSevF_diagsur50 = AllInSevF_diagsur.loc[0:49, :]
'''


AllInSevF_base50.to_csv('./All/All 입원환자 중증도 빈도별 순위.csv', encoding='cp949', index=False)
AllInSevF_base50Sev.to_csv('./All/All 입원환자 중증도 빈도별 순위(전문상위50).csv', encoding='cp949', index=False)
AllInSevF_base50Norm.to_csv('./All/All 입원환자 중증도 빈도별 순위(일반상위50).csv', encoding='cp949', index=False)
AllInSevF_base50Simp.to_csv('./All/All 입원환자 중증도 빈도별 순위(단순상위50).csv', encoding='cp949', index=False)
#AllInSevF_diagsur50.to_csv('./All/AllInSevF_diagsur50.csv', index=False)


AllInSevF_base.to_csv('./All/(원)All 입원환자 중증도 빈도별 순위.csv', encoding='cp949', index=False)
AllInSevF_baseSev.to_csv('./All/(원)All 입원환자 중증도 빈도별 순위(전문상위50).csv', encoding='cp949', index=False)
AllInSevF_baseNorm.to_csv('./All/(원)All 입원환자 중증도 빈도별 순위(일반상위50).csv', encoding='cp949', index=False)
AllInSevF_baseSimp.to_csv('./All/(원)All 입원환자 중증도 빈도별 순위(단순상위50).csv', encoding='cp949', index=False)

#AllInSevF_inst = pd.DataFrame(columns=['Rank' ,'DRGNO', 'Severity', 'A_Ratio', 'B_Ratio', 'C_Ratio', 'D_Ratio', 'E_Ratio', 'F_Ratio', 'G_Ratio', 'H_Ratio'])
#AllInSevF_regn = pd.DataFrame(columns=['Rank' ,'DRGNO', 'Severity', 'fSeoul', 'fBusan', 'fDaegu', 'fGwangju', 'fDaejeon', 'fIncheon', 'fJeju', 'fSejong', 'fJeonnam', 'fJeonbuk',
#                                      'fGyeongnam', 'fGyeongbuk', 'fChungnam', 'fChungbuk', 'fGangwon', 'fGyeonggi'])



