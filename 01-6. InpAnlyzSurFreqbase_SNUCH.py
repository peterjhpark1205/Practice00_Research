'''
This script is written 4 anyalyzing SNUCH's Inpatients' Data based on 'Surgery Frequency and highest '.

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
DiagCN=DiagCN.drop_duplicates(['D_Code'], keep='first')

SurCN = SurCN.rename(columns={'Scode':'Sur_Code', 'Sname':'Sur_Name'})
SurCN=SurCN.drop_duplicates(['Sur_Code'], keep='first')

DRGCNmain5=DRGCNmain5.groupby(by='DRGNO', as_index=False).agg({'DRGname': lambda a: a.value_counts().index[0]})
DRGCNsub4=DRGCNsub4.groupby(by='DRGNO', as_index=False).agg({'DRGname': lambda a: a.value_counts().index[0]})

print(SNUCHIn.info())
print(SNUCHIn.columns)


############################# Analyzing #############################


## RAW DATA based one Surgery Frequency: append rows
SNUCHIn4SurFbase = SNUCHIn.copy()
SNUCHIn4SurFbase.Sur_Code = SNUCHIn4SurFbase.Sur_Code.str.split('/')
SNUCHIn4SurFbase = SNUCHIn4SurFbase.apply(pd.Series.explode).reset_index(drop=True)
SNUCHIn4SurFbase.drop(['Sur_Name'], axis=1, inplace=True)
SNUCHIn4SurFbase['Sur_Code'] = SNUCHIn4SurFbase['Sur_Code'].str[0:5]
SNUCHIn4SurFbase = SNUCHIn4SurFbase.merge(SurCN, on='Sur_Code', how='left')
SNUCHIn4SurFbase.Sur_Name.fillna('NoSur', inplace=True)
SNUCHIn4SurFbase.loc[SNUCHIn4SurFbase.Sur_Name == 'NoSur', 'Sur_Code'] = 'NoSur'
SNUCHIn4SurFbase = SNUCHIn4SurFbase[SNUCHIn4SurFbase.Sur_Code != 'NoSur']
SNUCHIn4SurFbase = SNUCHIn4SurFbase[SNUCHIn4SurFbase.Sur_Code != 'GroupPay_SNUCH']
print(SNUCHIn4SurFbase.info())
print(SNUCHIn4SurFbase.columns)



SNUCHIn4SurFbase = SNUCHIn4SurFbase.drop_duplicates(['PT_No', 'In_Date', 'Sur_Code', 'Sur_Name', 'Sur_Date'], keep='first')
print(SNUCHIn4SurFbase.info())


bfreq=SNUCHIn4SurFbase.groupby(by='Sur_Code', as_index=False).agg({'Sur_Name': pd.Series.count})
bfreq.rename(columns={'Sur_Name':'Frequency'}, inplace=True)
bfreq = bfreq[bfreq.Sur_Code != 'NoDRG']
bfreq.sort_values(by='Frequency', ascending=False, inplace=True)
bfreq.reset_index(drop=True, inplace=True)
bfreq.index += 1
bfreq = bfreq.rename_axis('Rank').reset_index()
bfreq['Ratio'] = (bfreq.Frequency / bfreq.Frequency.sum()) * 100
bfreq.Ratio = bfreq.Ratio.round(1)
bfreq['Ratio'] = bfreq['Ratio'].astype(str)
bfreq['Frequency'] = bfreq['Frequency'].astype(int)
print (bfreq)

bsur=SNUCHIn4SurFbase.groupby(by='Sur_Code', as_index=False)['Sur_Name'].agg(lambda x: x.value_counts().index[0])
bsur.reset_index(drop=True, inplace=True)
print (bsur)


## 01. BASIC INFORMATION by Surgery Frequency
pnum=SNUCHIn4SurFbase.groupby(by='Sur_Code', as_index=False).agg({'PT_No': pd.Series.nunique})
pnum.rename(columns={'PT_No':'P_Num'}, inplace=True)
pnum['P_Num'] = pnum['P_Num'].astype(int)
pnum.reset_index(drop=True, inplace=True)
print (pnum)

avage=SNUCHIn4SurFbase.groupby(by='Sur_Code', as_index=False).agg({'Age': pd.Series.mean})
avage.rename(columns={'Age':'AvAge'}, inplace=True)
avage.AvAge = avage.AvAge.round(1)
avage.reset_index(drop=True, inplace=True)
print (avage)

avinprd=SNUCHIn4SurFbase.groupby(by='Sur_Code', as_index=False).agg({'In_Prd': pd.Series.mean})
avinprd.rename(columns={'In_Prd':'AvInPrd'}, inplace=True)
avinprd.AvInPrd = avinprd.AvInPrd.round(1)
avinprd.reset_index(drop=True, inplace=True)
print (avinprd)

SNUCHIn4SurFbase_Diag = SNUCHIn4SurFbase.copy()
SNUCHIn4SurFbase_Diag.D_Code = SNUCHIn4SurFbase_Diag.D_Code.str.split('/')
SNUCHIn4SurFbase_Diag = SNUCHIn4SurFbase_Diag.apply(pd.Series.explode).reset_index(drop=True)
SNUCHIn4SurFbase_Diag['D_Code'].fillna('NoDiag', inplace=True)
SNUCHIn4SurFbase_Diag.drop(['D_Name'], axis=1, inplace=True)
SNUCHIn4SurFbase_Diag = SNUCHIn4SurFbase_Diag.merge(DiagCN, on='D_Code', how='left')
SNUCHIn4SurFbase_Diag.D_Name.fillna('NoDiag', inplace=True)
SNUCHIn4SurFbase_Diag.loc[SNUCHIn4SurFbase_Diag.D_Name == 'NoDiag', 'D_Code'] = 'NoDiag'
SNUCHIn4SurFbase_Diag = SNUCHIn4SurFbase_Diag.drop_duplicates(['PT_No', 'In_Date', 'Sur_Code', 'Sur_Name', 'Sur_Date','D_Code', 'D_Name'], keep='first') # Need D_Date after for proper drop
SNUCHIn4SurFbase_Diag = SNUCHIn4SurFbase_Diag[SNUCHIn4SurFbase_Diag.D_Code != 'NoDiag']

##### another way to selcet most frequent value using groupby(Not Accurate in Certain Condition)#####
#dcodename = SNUCHIn4SurFbase_Diag.groupby(by='DRGNO', as_index=False).agg({'D_Code': lambda a: ' / '.join(pd.Series.mode(a)), 'D_Name': lambda b: ' / '.join(pd.Series.mode(b))})

dcode = SNUCHIn4SurFbase_Diag.groupby(by='Sur_Code', as_index=False)['D_Code'].agg(lambda x:x.value_counts().index[0])
dname = SNUCHIn4SurFbase_Diag.groupby(by='D_Code', as_index=False).agg({'D_Name': lambda a: a.value_counts().index[0]})
dcode.reset_index(drop=True, inplace=True)
dname.reset_index(drop=True, inplace=True)
dcodename = dcode.merge(dname, on='D_Code', how='left')
dcodename.reset_index(drop=True, inplace=True)
dfreq = SNUCHIn4SurFbase_Diag.groupby(by='Sur_Code', as_index=False)['D_Code'].agg(lambda x: x.value_counts().head(1))
dfreq.rename(columns={'D_Code':'Dfreq'}, inplace=True)
dfreq['Dfreq'] = dfreq['Dfreq'].astype('int64')
dfreq.reset_index(drop=True, inplace=True)
dtot = SNUCHIn4SurFbase_Diag.groupby(by='Sur_Code', as_index=False)['D_Code'].agg(lambda x: x.count())
dtot.rename(columns={'D_Code':'Dtot'}, inplace=True)
dtot['Dtot'] = dtot['Dtot'].astype('int64')
dtot.reset_index(drop=True, inplace=True)
dcodenamefreq = dcodename.merge(dfreq,on='Sur_Code',how='left').merge(dtot,on='Sur_Code',how='left')
dcodenamefreq.rename(columns={'D_Code':'Dcode', 'D_Name':'Dname'}, inplace=True)
dcodenamefreq.reset_index(drop=True, inplace=True)
dcodenamefreq['Dratio'] = (dcodenamefreq.Dfreq / dcodenamefreq.Dtot) * 100
dcodenamefreq.Dratio = dcodenamefreq.Dratio.round(1)
dcodenamefreq['Dratio'] = dcodenamefreq['Dratio'].astype(str)
with pd.option_context('display.max_columns', None):
    print (dcodenamefreq)

SNUCHIn4SurFbase_Ins = SNUCHIn4SurFbase.copy()
SNUCHIn4SurFbase_Ins['IndPaidExp'] = SNUCHIn4SurFbase_Ins['Pay_InsSelf'] + SNUCHIn4SurFbase_Ins['Pay_NoIns'] + SNUCHIn4SurFbase_Ins['Pay_Sel']
inspaid = SNUCHIn4SurFbase_Ins.groupby(by='Sur_Code', as_index=False).agg({'IndPaidExp' : pd.Series.mean, 'Pay_InsCorp' : pd.Series.mean})
inspaid.rename(columns={'Pay_InsCorp':'CorpPaidExp'}, inplace=True)
inspaid['IndPaidExp'] = inspaid['IndPaidExp'].astype(int)
inspaid['CorpPaidExp'] = inspaid['CorpPaidExp'].astype(int)
inspaid['IndPaidExp'] = inspaid.apply(lambda x: "{:,}".format(x['IndPaidExp']), axis=1)
inspaid['CorpPaidExp'] = inspaid.apply(lambda x: "{:,}".format(x['CorpPaidExp']), axis=1)
inspaid.reset_index(drop=True, inplace=True)
print (inspaid)


SNUCHInSurF_base = bfreq.merge(bsur,on='Sur_Code',how='left').merge(pnum,on='Sur_Code',how='left').merge(avage,on='Sur_Code',how='left').merge(avinprd,on='Sur_Code',how='left').merge(dcodenamefreq,on='Sur_Code',how='left').merge(inspaid,on='Sur_Code',how='left')
SNUCHInSurF_base.rename(columns={'Sur_Code':'Scode', 'Sur_Name':'Sname'}, inplace=True)
SNUCHInSurF_base = SNUCHInSurF_base[['Rank', 'Scode','Sname', 'Frequency', 'Ratio', 'P_Num', 'AvAge', 'AvInPrd', 'Dcode', 'Dname', 'Dfreq', 'Dratio', 'IndPaidExp', 'CorpPaidExp']]
SNUCHInSurF_base['Frequency'] = SNUCHInSurF_base.apply(lambda x: "{:,}".format(x['Frequency']), axis=1)
SNUCHInSurF_base['Dfreq'] = SNUCHInSurF_base.apply(lambda x: "{:,}".format(x['Dfreq']), axis=1)
SNUCHInSurF_base.reset_index(drop=True, inplace=True)

SNUCHInSurF_base = SNUCHInSurF_base.rename(columns={'Rank':'순위', 'Scode':'수술코드','Sname':'수술명', 'Frequency':'빈도(건)', 'Ratio':'비율(%)', 'P_Num':'환자수(명)', 'AvAge':'평균연령(세)',
                                                    'AvInPrd':'평균재원기간(일)', 'Dcode':'진단코드', 'Dname':'진단명', 'Dfreq':'진단빈도(건)', 'Dratio':'진단비율(%)', 'IndPaidExp':'본인부담금(원)',
                                                    'CorpPaidExp':'공단부담금(원)'})


with pd.option_context('display.max_columns', None):
    print(SNUCHInSurF_base)
print(SNUCHInSurF_base.columns)

SNUCHInSurF_base50 = SNUCHInSurF_base.loc[0:49,:]

## 03. SEVERITY INFORMATION based on Diagnosis Frequency
SNUCHIn4SurFbase_Sev = SNUCHIn4SurFbase.copy()
#SNUCHIn4SurFbase_Sev.DRGNO = SNUCHIn4SurFbase_Sev.DRGNO.str.split('/')
#SNUCHIn4SurFbase_Sev.Severity = SNUCHIn4SurFbase_Sev.Severity.str.split('/')
#SNUCHIn4SurFbase_Sev = SNUCHIn4SurFbase_Sev.apply(pd.Series.explode).reset_index(drop=True)

severe = SNUCHIn4SurFbase_Sev.groupby('Sur_Code')['Severity'].apply(lambda x: (x=='Severe').sum()).reset_index(name='Sev_Freq')
severe = severe[severe.Sur_Code != 'NoSur']

normal = SNUCHIn4SurFbase_Sev.groupby('Sur_Code')['Severity'].apply(lambda x: (x=='Normal').sum()).reset_index(name='Norm_Freq')
normal = normal[normal.Sur_Code != 'NoSur']

simple = SNUCHIn4SurFbase_Sev.groupby('Sur_Code')['Severity'].apply(lambda x: (x=='Simple').sum()).reset_index(name='Simple_Freq')
simple = simple[simple.Sur_Code != 'NoSur']

sorterror = SNUCHIn4SurFbase_Sev.groupby('Sur_Code')['Severity'].apply(lambda x: (x=='SortError').sum()).reset_index(name='SrtErr_Freq')
sorterror = sorterror[sorterror.Sur_Code != 'NoSur']

nodrg = SNUCHIn4SurFbase_Sev.groupby('Sur_Code')['Severity'].apply(lambda x: (x=='NoDRG').sum()).reset_index(name='NoDRG_Freq')
nodrg = nodrg[nodrg.Sur_Code != 'NoSur']

SNUCHInSurF_svty = bfreq.merge(bsur,on='Sur_Code',how='left').merge(severe,on='Sur_Code',how='left').merge(normal,on='Sur_Code',how='left').merge(simple,on='Sur_Code',how='left').merge(sorterror,on='Sur_Code',how='left').merge(nodrg,on='Sur_Code',how='left')
SNUCHInSurF_svty.rename(columns={'Sur_Code':'Scode', 'Sur_Name':'Sname'}, inplace=True)
SNUCHInSurF_svty = SNUCHInSurF_svty[['Rank', 'Scode','Sname', 'Sev_Freq', 'Norm_Freq', 'Simple_Freq', 'SrtErr_Freq', 'NoDRG_Freq', 'Frequency', 'Ratio']]
SNUCHInSurF_svty.drop(['Frequency', 'Ratio'], axis=1, inplace=True)
SNUCHInSurF_svty.reset_index(drop=True, inplace=True)

SNUCHInSurF_svty['Sev_Ratio'] = (SNUCHInSurF_svty.Sev_Freq / (SNUCHInSurF_svty.Sev_Freq + SNUCHInSurF_svty.Norm_Freq + SNUCHInSurF_svty.Simple_Freq + SNUCHInSurF_svty.SrtErr_Freq + SNUCHInSurF_svty.NoDRG_Freq)) * 100
SNUCHInSurF_svty.Sev_Ratio = SNUCHInSurF_svty.Sev_Ratio.round(1)
SNUCHInSurF_svty['Sev_Ratio'] = SNUCHInSurF_svty['Sev_Ratio'].astype(str)

SNUCHInSurF_svty['Norm_Ratio'] = (SNUCHInSurF_svty.Norm_Freq / (SNUCHInSurF_svty.Sev_Freq + SNUCHInSurF_svty.Norm_Freq + SNUCHInSurF_svty.Simple_Freq + SNUCHInSurF_svty.SrtErr_Freq + SNUCHInSurF_svty.NoDRG_Freq)) * 100
SNUCHInSurF_svty.Norm_Ratio = SNUCHInSurF_svty.Norm_Ratio.round(1)
SNUCHInSurF_svty['Norm_Ratio'] = SNUCHInSurF_svty['Norm_Ratio'].astype(str)

SNUCHInSurF_svty['Simple_Ratio'] = (SNUCHInSurF_svty.Simple_Freq / (SNUCHInSurF_svty.Sev_Freq + SNUCHInSurF_svty.Norm_Freq + SNUCHInSurF_svty.Simple_Freq + SNUCHInSurF_svty.SrtErr_Freq + SNUCHInSurF_svty.NoDRG_Freq)) * 100
SNUCHInSurF_svty.Simple_Ratio = SNUCHInSurF_svty.Simple_Ratio.round(1)
SNUCHInSurF_svty['Simple_Ratio'] = SNUCHInSurF_svty['Simple_Ratio'].astype(str)

SNUCHInSurF_svty['SrtErr_Ratio'] = (SNUCHInSurF_svty.SrtErr_Freq / (SNUCHInSurF_svty.Sev_Freq + SNUCHInSurF_svty.Norm_Freq + SNUCHInSurF_svty.Simple_Freq + SNUCHInSurF_svty.SrtErr_Freq + SNUCHInSurF_svty.NoDRG_Freq)) * 100
SNUCHInSurF_svty.SrtErr_Ratio = SNUCHInSurF_svty.SrtErr_Ratio.round(1)
SNUCHInSurF_svty['SrtErr_Ratio'] = SNUCHInSurF_svty['SrtErr_Ratio'].astype(str)

SNUCHInSurF_svty['NoDRG_Ratio'] = (SNUCHInSurF_svty.NoDRG_Freq / (SNUCHInSurF_svty.Sev_Freq + SNUCHInSurF_svty.Norm_Freq + SNUCHInSurF_svty.Simple_Freq + SNUCHInSurF_svty.SrtErr_Freq + SNUCHInSurF_svty.NoDRG_Freq)) * 100
SNUCHInSurF_svty.NoDRG_Ratio = SNUCHInSurF_svty.NoDRG_Ratio.round(1)
SNUCHInSurF_svty['NoDRG_Ratio'] = SNUCHInSurF_svty['NoDRG_Ratio'].astype(str)

SNUCHInSurF_svty = SNUCHInSurF_svty[['Rank', 'Scode', 'Sname', 'Sev_Freq', 'Sev_Ratio', 'Norm_Freq', 'Norm_Ratio', 'Simple_Freq', 'Simple_Ratio', 'SrtErr_Freq', 'SrtErr_Ratio', 'NoDRG_Freq', 'NoDRG_Ratio']]

SNUCHInSurF_svty['Sev_Freq'] = SNUCHInSurF_svty.apply(lambda x: "{:,}".format(x['Sev_Freq']), axis=1)
SNUCHInSurF_svty['Norm_Freq'] = SNUCHInSurF_svty.apply(lambda x: "{:,}".format(x['Norm_Freq']), axis=1)
SNUCHInSurF_svty['Simple_Freq'] = SNUCHInSurF_svty.apply(lambda x: "{:,}".format(x['Simple_Freq']), axis=1)
SNUCHInSurF_svty['SrtErr_Freq'] = SNUCHInSurF_svty.apply(lambda x: "{:,}".format(x['SrtErr_Freq']), axis=1)
SNUCHInSurF_svty['NoDRG_Freq'] = SNUCHInSurF_svty.apply(lambda x: "{:,}".format(x['NoDRG_Freq']), axis=1)

SNUCHInSurF_svty = SNUCHInSurF_svty.rename(columns={'Rank':'순위', 'Scode':'수술코드', 'Sname':'수술명', 'Sev_Freq':'전문빈도(건)', 'Sev_Ratio':'전문비율(%)', 'Norm_Freq':'일반빈도(건)', 'Norm_Ratio':'일반비율(%)',
                                                    'Simple_Freq':'단순빈도(건)', 'Simple_Ratio':'단순비율(%)', 'SrtErr_Freq':'분류오류빈도(건)', 'SrtErr_Ratio':'분류오류비율(%)', 'NoDRG_Freq':'미분류빈도(건)',
                                                    'NoDRG_Ratio':'미분류비율(%)'})


SNUCHInSurF_svty50 = SNUCHInSurF_svty.loc[0:49, :]



SNUCHInSurF_base50.to_csv('./SNUCH/SNUCH 입원환자 수술 빈도별 순위 (기본 사항).csv', encoding='cp949', index=False)
SNUCHInSurF_svty50.to_csv('./SNUCH/SNUCH 입원환자 수술 빈도별 순위 (중증도 분류).csv', encoding='cp949', index=False)


SNUCHInSurF_base.to_csv('./SNUCH/(원)SNUCH 입원환자 수술 빈도별 순위 (기본 사항).csv', encoding='cp949', index=False)
SNUCHInSurF_svty.to_csv('./SNUCH/(원)SNUCH 입원환자 수술 빈도별 순위 (중증도 분류).csv', encoding='cp949', index=False)




