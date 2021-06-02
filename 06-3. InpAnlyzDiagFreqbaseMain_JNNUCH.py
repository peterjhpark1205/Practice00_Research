'''
This script is written 4 anyalyzing JNNUCH's Inpatients' Data based on 'Diagnosis Frequency'.

Written Date: 2019.12.11.
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

JNNUCHIn = pd.read_csv("./JNNUCH/JNNUCHInPMain_R4A.csv", encoding="utf-8")
SubIns_Cri = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018-2019SubIns_Cri.csv", encoding="utf-8", low_memory=False)
DiagCN = pd.read_csv("./master_Dcode&Dname.csv", encoding="utf-8")
SurCN = pd.read_csv("./master_Scode&Sname.csv", encoding="utf-8")
DRGCNmain5 = pd.read_csv("./SevCri_5digit(DRGname).csv", encoding="utf-8")
DRGCNsub4 = pd.read_csv("./SevCri_4digit(DRGname).csv", encoding="utf-8")
print(JNNUCHIn.info())
print(JNNUCHIn.columns)

DiagCN.drop(['Dname(ENG)'], axis=1, inplace=True)
DiagCN.rename(columns={'Dcode' : 'D_Code', 'Dname(KOR)' :'D_Name'}, inplace=True)
DiagCN=DiagCN.drop_duplicates(['D_Code'], keep='first')

SurCN = SurCN.rename(columns={'Scode':'Sur_Code', 'Sname':'Sur_Name'})
SurCN=SurCN.drop_duplicates(['Sur_Code'], keep='first')

'''
print(JNNUCHIn.head())
print("Total Patients : ", len(JNNUCHIn))
'''

############################# Analyzing #############################

## 01-1. DEMOGRAPHICS 4 RealP

JNNUCHIn4BDemo = JNNUCHIn.groupby('PT_No').agg({'Age': lambda a : a.value_counts().index[0], 'Gender': lambda b : b.value_counts().index[0], 'Address': lambda c : c.value_counts().index[0]})
bins = [0, 1, 7, 13, 18, np.inf]
labels = ['under1', '1to6', '7to12', '13to18', 'over18']
Rdemoage = JNNUCHIn4BDemo.groupby(pd.cut(JNNUCHIn4BDemo['Age'], bins=bins, labels=labels)).size().reset_index(name='count')
Rdemoage.rename(columns={'Age':' ', 'count':'Counts'}, inplace=True)
Rdemoage['Ratio'] = (Rdemoage.Counts / Rdemoage.Counts.sum()) * 100
Rdemoage.Ratio = Rdemoage.Ratio.round(1)
Rdemoage.Counts = Rdemoage.apply(lambda x: "{:,}".format(x['Counts']), axis=1)
Rdemoage['Ratio'] = Rdemoage['Ratio'].astype(str)
Rdemoage['Class'] = ' '
Rdemoage = Rdemoage[['Class', ' ', 'Counts', 'Ratio']]
Rdemoage = Rdemoage.append(pd.Series(['나이', ' ', ' ', ' '], index=Rdemoage.columns), ignore_index=True)
Rdemoage = Rdemoage.reindex([5, 0, 1, 2, 3, 4])
Rdemoage[' '] = Rdemoage[' '].map({' ' : ' ', 'under1' : '1세미만', '1to6' : '1세이상-6세이하', '7to12' : '7세이상-12세이하', '13to18' : '13세이상-18세이하', 'over18' : '18세초과'})
print (Rdemoage)


Rdemogender = JNNUCHIn4BDemo.groupby(by='Gender', as_index=False).size().reset_index(name='count')
Rdemogender.rename(columns={'Gender':' ', 'count':'Counts'}, inplace=True)
Rdemogender['Ratio'] = (Rdemogender.Counts / Rdemogender.Counts.sum()) * 100
Rdemogender.Ratio = Rdemogender.Ratio.round(1)
Rdemogender.Counts = Rdemogender.apply(lambda x: "{:,}".format(x['Counts']), axis=1)
Rdemogender['Ratio'] = Rdemogender['Ratio'].astype(str)
Rdemogender['Class'] = ' '
Rdemogender = Rdemogender[['Class', ' ', 'Counts', 'Ratio']]
Rdemogender = Rdemogender.append(pd.Series(['성별', ' ', ' ', ' '], index=Rdemogender.columns), ignore_index=True)
Rdemogender = Rdemogender.sort_index(ascending=False)
Rdemogender[' '] = Rdemogender[' '].map({' ' : ' ', 'Male' : '남', 'Female' : '여'})
print (Rdemogender)


JNNUCHIn4Rdemoreg = JNNUCHIn4BDemo.copy()
JNNUCHIn4Rdemoreg = JNNUCHIn4Rdemoreg[JNNUCHIn4Rdemoreg.Address != 'NoAdd']
Rdemoreg = JNNUCHIn4Rdemoreg.groupby(by='Address', as_index=False).size().reset_index(name='count')
Rdemoreg.rename(columns={'Address':' ', 'count':'Counts'}, inplace=True)
Rdemoreg['Ratio'] = (Rdemoreg.Counts / Rdemoreg.Counts.sum()) * 100
Rdemoreg.Ratio = Rdemoreg.Ratio.round(1)
Rdemoreg.Counts = Rdemoreg.apply(lambda x: "{:,}".format(x['Counts']), axis=1)
Rdemoreg['Ratio'] = Rdemoreg['Ratio'].astype(str)
Rdemoregsub = {' ' : ['seoul', 'busan', 'daegu', 'incheon', 'gwangju', 'daejeon', 'ulsan', 'sejong', 'gyeonggi', 'gangwon', 'chungbuk', 'chungnam', 'jeonbuk', 'jeonnam', 'gyeongbuk', 'gyeongnam', 'jeju']}
Rdemoregsub = pd.DataFrame(Rdemoregsub)
Rdemoreg = Rdemoregsub.merge(Rdemoreg, on=' ', how='left')
Rdemoreg.Counts.fillna('0', inplace=True)
Rdemoreg.Ratio.fillna('0.0', inplace=True)
Rdemoreg['Class'] = ' '
Rdemoreg = Rdemoreg[['Class', ' ', 'Counts', 'Ratio']]
Rdemoreg = Rdemoreg.append(pd.Series(['지역', ' ', ' ', ' '], index=Rdemoreg.columns), ignore_index=True)
Rdemoreg = Rdemoreg.reindex([17, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
Rdemoreg[' '] = Rdemoreg[' '].map({' ': ' ', 'seoul' : '서울특별시', 'busan' : '부산광역시', 'daegu' : '대구광역시', 'incheon' : '인천광역시', 'gwangju' : '광주광역시',
                                   'daejeon' : '대전광역시', 'ulsan' : '울산광역시', 'sejong' : '세종특별자치시', 'gyeonggi' : '경기도', 'gangwon' : '강원도', 'chungbuk' : '충청북도',
                                   'chungnam' : '충청남도', 'jeonbuk' : '전라북도', 'jeonnam' : '전라남도', 'gyeongbuk' : '경상북도', 'gyeongnam' : '경상남도', 'jeju' : '제주특별자치도'})
print (Rdemoreg)



# ('NHIS', 'MedCareT1', 'MedCareT2', 'MedCareDis', 'Others')
JNNUCHIn4RIns = JNNUCHIn.copy()
JNNUCHIn4RIns.drop_duplicates(['PT_No', 'Ins_Var'],inplace=True)
Rdemoins = JNNUCHIn4RIns.groupby(by='Ins_Var', as_index=False).size().reset_index(name='count')
Rdemoins.rename(columns={'Ins_Var':' ', 'count':'Counts'}, inplace=True)
Rdemoinstot = JNNUCHIn4BDemo.shape[0]
Rdemoins['Ratio'] = (Rdemoins.Counts / Rdemoins.Counts.sum()) * 100
Rdemoins.Ratio = Rdemoins.Ratio.round(1)
Rdemoins.Counts = Rdemoins.apply(lambda x: "{:,}".format(x['Counts']), axis=1)
Rdemoins['Ratio'] = Rdemoins['Ratio'].astype(str)
Rdemoinssub = {' ' : ['NHIS', 'MedCareT1', 'MedCareT2', 'MedCareDis', 'Others']}
Rdemoinssub = pd.DataFrame(Rdemoinssub)
Rdemoins = Rdemoinssub.merge(Rdemoins, on=' ', how='left')
Rdemoins.Counts.fillna('0', inplace=True)
Rdemoins.Ratio.fillna('0.0', inplace=True)
Rdemoins['Class'] = ' '
Rdemoins = Rdemoins[['Class', ' ', 'Counts', 'Ratio']]
Rdemoins = Rdemoins.append(pd.Series(['보험 급종', ' ', ' ', ' '], index=Rdemoins.columns), ignore_index=True)
Rdemoins = Rdemoins.reindex([5, 0, 1, 2, 3, 4])
Rdemoins[' '] = Rdemoins[' '].map({' ': ' ', 'NHIS' : '국민건강보험', 'MedCareT1' : '의료급여1종', 'MedCareT2' : '의료급여2종', 'MedCareDis' : '의료급여장애인', 'Others' : '기타'})
print (Rdemoins)


# ('Severe', 'Normal', 'Simple', 'SortError')
JNNUCHIn4Rdemosev = JNNUCHIn.copy()

JNNUCHIn4Rdemosev['Severity'] = JNNUCHIn4Rdemosev['Severity'].map({'Severe' : 4, 'Normal' : 3, 'Simple' : 2, 'SortError' : 1, 'NoDRG' : 0})

JNNUCHIn4Rdemosevsub = JNNUCHIn4Rdemosev.copy()
JNNUCHIn4Rdemosev = JNNUCHIn4Rdemosev.groupby(['PT_No'], as_index= False)['Severity'].agg(lambda x : x.max())
JNNUCHIn4Rdemosev = JNNUCHIn4Rdemosev.merge(JNNUCHIn4Rdemosevsub, on=['PT_No', 'Severity'], how='left')
JNNUCHIn4Rdemosev.drop_duplicates(subset =['PT_No', 'Severity'], inplace = True)
JNNUCHIn4Rdemosev.reset_index(drop=True, inplace=True)

JNNUCHIn4Rdemosev['Severity'] = JNNUCHIn4Rdemosev['Severity'].map({4 :'Severe', 3 :'Normal', 2 :'Simple', 1 :'SortError', 0 : 'NoDRG'})
#JNNUCHIn4Rdemosev.Severity = JNNUCHIn4Rdemosev.Severity.str.split('/')
#JNNUCHIn4Rdemosev.DRGNO = JNNUCHIn4Rdemosev.DRGNO.str.split('/')
#JNNUCHIn4Rdemosev = JNNUCHIn4Rdemosev.apply(pd.Series.explode).reset_index(drop=True)


JNNUCHIn4Rdemosev = JNNUCHIn4Rdemosev[JNNUCHIn4Rdemosev.DRGNO != 'NoDRG']
Rdemosev = JNNUCHIn4Rdemosev.groupby(by='Severity', as_index=False).size().reset_index(name='count')
Rdemosev.rename(columns={'Severity':' ', 'count':'Counts'}, inplace=True)
Rdemosev['Ratio'] = (Rdemosev.Counts / Rdemosev.Counts.sum()) * 100
Rdemosev.Ratio = Rdemosev.Ratio.round(1)
Rdemosev.Counts = Rdemosev.apply(lambda x: "{:,}".format(x['Counts']), axis=1)
Rdemosev['Ratio'] = Rdemosev['Ratio'].astype(str)
Rdemosevsub = {' ' : ['Severe', 'Normal', 'Simple', 'SortError']}
Rdemosevsub = pd.DataFrame(Rdemosevsub)
Rdemosev = Rdemosevsub.merge(Rdemosev, on=' ', how='left')
Rdemosev.Counts.fillna('0', inplace=True)
Rdemosev.Ratio.fillna('0.0', inplace=True)
Rdemosev['Class'] = ' '
Rdemosev = Rdemosev[['Class', ' ', 'Counts', 'Ratio']]
Rdemosev = Rdemosev.append(pd.Series(['중증도(KDRG 기준)', ' ', ' ', ' '], index=Rdemosev.columns), ignore_index=True)
Rdemosev = Rdemosev.reindex([4, 0, 1, 2, 3])
Rdemosev[' '] = Rdemosev[' '].map({' ': ' ', 'Severe' : '전문', 'Normal' : '일반', 'Simple' : '단순', 'SortError' : '분류오류'})
print (Rdemosev)


# ('Rare', 'SevIncure', ''ExtRare', 'OtherChrom', 'Mild')
#print(SubIns_Cri.columns)
SubIns_Cri.rename(columns={'SubIns':'Ins_Sub'}, inplace=True)
SubIns_Cri = SubIns_Cri[['Ins_Sub', 'Rarity']]
#print(SubIns_Cri.Rarity.unique())
SubIns_Cri['Rarity'] = SubIns_Cri['Rarity'].map({'희귀' : 'Rare', '중증난치' : 'SevIncure', '극희귀' : 'ExtRare', '기타염색체' : 'OtherChrom', '중증' : 'Severe','경증' : 'Mild'})
SubIns_Cri.dropna(inplace=True)
#print(SubIns_Cri)

JNNUCHIn4Rrare = JNNUCHIn.copy()
JNNUCHIn4Rrare.Ins_Sub = JNNUCHIn4Rrare.Ins_Sub.str.split('/')
JNNUCHIn4Rrare = JNNUCHIn4Rrare.apply(pd.Series.explode).reset_index(drop=True)
JNNUCHIn4Rrare = JNNUCHIn4Rrare.merge(SubIns_Cri, on='Ins_Sub', how='left')
JNNUCHIn4Rrare.Rarity.fillna('NoVCode',inplace=True)
JNNUCHIn4Rrare.Ins_Sub.fillna('NoVCode',inplace=True)
JNNUCHIn4Rrare.loc[JNNUCHIn4Rrare.Rarity == 'NoVCode', 'Ins_Sub'] = 'NoVCode'
JNNUCHIn4Rrare.drop_duplicates(['PT_No', 'Ins_Sub'],inplace=True)
#JNNUCHIn4Rrare = JNNUCHIn4Rrare[JNNUCHIn4Rrare.Rarity != 'NoVCode']

Rdemorare = JNNUCHIn4Rrare.groupby(by='Rarity', as_index=False).size().reset_index(name='count')
Rdemorare.rename(columns={'Rarity':' ', 'count':'Counts'}, inplace=True)
Rdemoraretot = JNNUCHIn4BDemo.shape[0]
Rdemorare['Ratio'] = (Rdemorare.Counts / Rdemoraretot) * 100
Rdemorare.Ratio = Rdemorare.Ratio.round(1)
Rdemorare.Counts = Rdemorare.apply(lambda x: "{:,}".format(x['Counts']), axis=1)
Rdemorare['Ratio'] = Rdemorare['Ratio'].astype(str)
Rdemoraresub = {' ' : ['Mild', 'Severe', 'SevIncure', 'Rare', 'ExtRare', 'OtherChrom', 'NoVCode']}
Rdemoraresub = pd.DataFrame(Rdemoraresub)
Rdemorare = Rdemoraresub.merge(Rdemorare, on=' ', how='left')
Rdemorare.Counts.fillna('0', inplace=True)
Rdemorare.Ratio.fillna('0.0', inplace=True)
Rdemorare['Class'] = ' '
Rdemorare = Rdemorare[['Class', ' ', 'Counts', 'Ratio']]
Rdemorare = Rdemorare.append(pd.Series(['희귀질환(산정특례기호 기준)', ' ', ' ', ' '], index=Rdemorare.columns), ignore_index=True)
Rdemorare = Rdemorare.reindex([7, 0, 1, 2, 3, 4, 5, 6])
Rdemorare[' '] = Rdemorare[' '].map({' ': ' ', 'Mild' : '경증질환', 'Severe' : '중증', 'SevIncure' : '중증난치질환', 'Rare' : '희귀질환', 'ExtRare' : '극희귀질환', 'OtherChrom' : '기타염색체질환', 'NoVCode' : '산정특례기호없음'})
print (Rdemorare)

JNNUCHInRDemo = pd.concat([Rdemoage, Rdemogender, Rdemoreg, Rdemoins, Rdemorare, Rdemosev], ignore_index=True)
RPNum = JNNUCHIn4BDemo.shape[0]
"{:,}".format(RPNum)
JNNUCHInRDemo = JNNUCHInRDemo.append(pd.Series(['전체 입원환자 수', ' ', RPNum, '100.0'], index=JNNUCHInRDemo.columns), ignore_index=True)
JNNUCHInRDemo = JNNUCHInRDemo.reindex([46, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
                                     30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45])
JNNUCHInRDemo = JNNUCHInRDemo.rename(columns={'Class':'구분', ' ':' ', 'Counts': '건수(건)', 'Ratio':'비율(%)'})
print(JNNUCHInRDemo)



## 01-2. DEMOGRAPHICS 4 AllP by Episodes

bins = [0, 1, 7, 13, 18, np.inf]
labels = ['under1', '1to6', '7to12', '13to18', 'over18']
demoage = JNNUCHIn.groupby(pd.cut(JNNUCHIn['Age'], bins=bins, labels=labels)).size().reset_index(name='count')
demoage.rename(columns={'Age':' ', 'count':'Counts'}, inplace=True)
demoage['Ratio'] = (demoage.Counts / demoage.Counts.sum()) * 100
demoage.Ratio = demoage.Ratio.round(1)
demoage.Counts = demoage.apply(lambda x: "{:,}".format(x['Counts']), axis=1)
demoage['Ratio'] = demoage['Ratio'].astype(str)
demoage['Class'] = ' '
demoage = demoage[['Class', ' ', 'Counts', 'Ratio']]
demoage = demoage.append(pd.Series(['나이', ' ', ' ', ' '], index=demoage.columns), ignore_index=True)
demoage = demoage.reindex([5, 0, 1, 2, 3, 4])
demoage[' '] = demoage[' '].map({' ' : ' ', 'under1' : '1세미만', '1to6' : '1세이상-6세이하', '7to12' : '7세이상-12세이하', '13to18' : '13세이상-18세이하', 'over18' : '18세초과'})
print (demoage)


demogender = JNNUCHIn.groupby(by='Gender', as_index=False).size().reset_index(name='count')
demogender.rename(columns={'Gender':' ', 'count':'Counts'}, inplace=True)
demogender['Ratio'] = (demogender.Counts / demogender.Counts.sum()) * 100
demogender.Ratio = demogender.Ratio.round(1)
demogender.Counts = demogender.apply(lambda x: "{:,}".format(x['Counts']), axis=1)
demogender['Ratio'] = demogender['Ratio'].astype(str)
demogender['Class'] = ' '
demogender = demogender[['Class', ' ', 'Counts', 'Ratio']]
demogender = demogender.append(pd.Series(['성별', ' ', ' ', ' '], index=demogender.columns), ignore_index=True)
demogender = demogender.sort_index(ascending=False)
demogender[' '] = demogender[' '].map({' ' : ' ', 'Male' : '남', 'Female' : '여'})
print (demogender)



JNNUCHIn4demoreg = JNNUCHIn.copy()
JNNUCHIn4demoreg = JNNUCHIn4demoreg[JNNUCHIn4demoreg.Address != 'NoAdd']
demoreg = JNNUCHIn4demoreg.groupby(by='Address', as_index=False).size().reset_index(name='count')
demoreg.rename(columns={'Address':' ', 'count':'Counts'}, inplace=True)
demoreg['Ratio'] = (demoreg.Counts / demoreg.Counts.sum()) * 100
demoreg.Ratio = demoreg.Ratio.round(1)
demoreg.Counts = demoreg.apply(lambda x: "{:,}".format(x['Counts']), axis=1)
demoreg['Ratio'] = demoreg['Ratio'].astype(str)
demoregsub = {' ' : ['seoul', 'busan', 'daegu', 'incheon', 'gwangju', 'daejeon', 'ulsan', 'sejong', 'gyeonggi', 'gangwon', 'chungbuk', 'chungnam', 'jeonbuk', 'jeonnam', 'gyeongbuk', 'gyeongnam', 'jeju']}
demoregsub = pd.DataFrame(demoregsub)
demoreg = demoregsub.merge(demoreg, on=' ', how='left')
demoreg.Counts.fillna('0', inplace=True)
demoreg.Ratio.fillna('0.0', inplace=True)
demoreg['Class'] = ' '
demoreg = demoreg[['Class', ' ', 'Counts', 'Ratio']]
demoreg = demoreg.append(pd.Series(['지역', ' ', ' ', ' '], index=demoreg.columns), ignore_index=True)
demoreg = demoreg.reindex([17, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
demoreg[' '] = demoreg[' '].map({' ': ' ', 'seoul' : '서울특별시', 'busan' : '부산광역시', 'daegu' : '대구광역시', 'incheon' : '인천광역시', 'gwangju' : '광주광역시',
                                   'daejeon' : '대전광역시', 'ulsan' : '울산광역시', 'sejong' : '세종특별자치시', 'gyeonggi' : '경기도', 'gangwon' : '강원도', 'chungbuk' : '충청북도',
                                   'chungnam' : '충청남도', 'jeonbuk' : '전라북도', 'jeonnam' : '전라남도', 'gyeongbuk' : '경상북도', 'gyeongnam' : '경상남도', 'jeju' : '제주특별자치도'})
print (demoreg)



# ('NHIS', 'MedCareT1', 'MedCareT2', 'MedCareDis', 'Others')
JNNUCHIn4Ins = JNNUCHIn.copy()
JNNUCHIn4Ins.drop_duplicates(['PT_No', 'In_Date', 'Ins_Var'],inplace=True)
demoins = JNNUCHIn4Ins.groupby(by='Ins_Var', as_index=False).size().reset_index(name='count')
demoins.rename(columns={'Ins_Var':' ', 'count':'Counts'}, inplace=True)
demoinstot = JNNUCHIn.shape[0]
demoins['Ratio'] = (demoins.Counts / demoins.Counts.sum()) * 100
demoins.Ratio = demoins.Ratio.round(1)
demoins.Counts = demoins.apply(lambda x: "{:,}".format(x['Counts']), axis=1)
demoins['Ratio'] = demoins['Ratio'].astype(str)
demoinssub = {' ' : ['NHIS', 'MedCareT1', 'MedCareT2', 'MedCareDis', 'Others']}
demoinssub = pd.DataFrame(demoinssub)
demoins = demoinssub.merge(demoins, on=' ', how='left')
demoins.Counts.fillna('0', inplace=True)
demoins.Ratio.fillna('0.0', inplace=True)
demoins['Class'] = ' '
demoins = demoins[['Class', ' ', 'Counts', 'Ratio']]
demoins = demoins.append(pd.Series(['보험 급종', ' ', ' ', ' '], index=demoins.columns), ignore_index=True)
demoins = demoins.reindex([5, 0, 1, 2, 3, 4])
demoins[' '] = demoins[' '].map({' ': ' ', 'NHIS' : '국민건강보험', 'MedCareT1' : '의료급여1종', 'MedCareT2' : '의료급여2종', 'MedCareDis' : '의료급여장애인', 'Others' : '기타'})
print (demoins)


# ('Severe', 'Normal', 'Simple', 'SortError')
JNNUCHIn4demosev = JNNUCHIn.copy()

JNNUCHIn4demosev['Severity'] = JNNUCHIn4demosev['Severity'].map({'Severe' : 4, 'Normal' : 3, 'Simple' : 2, 'SortError' : 1, 'NoDRG' : 0})

JNNUCHIn4demosevsub = JNNUCHIn4demosev.copy()
JNNUCHIn4demosev = JNNUCHIn4demosev.groupby(['PT_No', 'In_Date'], as_index= False)['Severity'].agg(lambda x : x.max())
JNNUCHIn4demosev = JNNUCHIn4demosev.merge(JNNUCHIn4demosevsub, on=['PT_No', 'In_Date', 'Severity'], how='left')
JNNUCHIn4demosev.drop_duplicates(subset =['PT_No', 'In_Date', 'Severity'], inplace = True)
JNNUCHIn4demosev.reset_index(drop=True, inplace=True)

JNNUCHIn4demosev['Severity'] = JNNUCHIn4demosev['Severity'].map({4 :'Severe', 3 :'Normal', 2 :'Simple', 1 :'SortError', 0 : 'NoDRG'})
#JNNUCHIn4demosev.Severity = JNNUCHIn4demosev.Severity.str.split('/')
#JNNUCHIn4demosev.DRGNO = JNNUCHIn4demosev.DRGNO.str.split('/')
#JNNUCHIn4demosev = JNNUCHIn4demosev.apply(pd.Series.explode).reset_index(drop=True)

JNNUCHIn4demosev = JNNUCHIn4demosev[JNNUCHIn4demosev.DRGNO != 'NoDRG']
demosev = JNNUCHIn4demosev.groupby(by='Severity', as_index=False).size().reset_index(name='count')
demosev.rename(columns={'Severity':' ', 'count':'Counts'}, inplace=True)
demosev['Ratio'] = (demosev.Counts / demosev.Counts.sum()) * 100
demosev.Ratio = demosev.Ratio.round(1)
demosev.Counts = demosev.apply(lambda x: "{:,}".format(x['Counts']), axis=1)
demosev['Ratio'] = demosev['Ratio'].astype(str)
demosevsub = {' ' : ['Severe', 'Normal', 'Simple', 'SortError']}
demosevsub = pd.DataFrame(demosevsub)
demosev = demosevsub.merge(demosev, on=' ', how='left')
demosev.Counts.fillna('0', inplace=True)
demosev.Ratio.fillna('0.0', inplace=True)
demosev['Class'] = ' '
demosev = demosev[['Class', ' ', 'Counts', 'Ratio']]
demosev = demosev.append(pd.Series(['중증도(KDRG 기준)', ' ', ' ', ' '], index=demosev.columns), ignore_index=True)
demosev = demosev.reindex([4, 0, 1, 2, 3])
demosev[' '] = demosev[' '].map({' ': ' ', 'Severe' : '전문', 'Normal' : '일반', 'Simple' : '단순', 'SortError' : '분류오류'})
print (demosev)


# ('Rare', 'SevIncure', ''ExtRare', 'OtherChrom', 'Mild')

JNNUCHIn4rare = JNNUCHIn.copy()
JNNUCHIn4rare.Ins_Sub = JNNUCHIn4rare.Ins_Sub.str.split('/')
JNNUCHIn4rare = JNNUCHIn4rare.apply(pd.Series.explode).reset_index(drop=True)
JNNUCHIn4rare = JNNUCHIn4rare.merge(SubIns_Cri, on='Ins_Sub', how='left')
JNNUCHIn4rare.Rarity.fillna('NoVCode',inplace=True)
JNNUCHIn4rare.Ins_Sub.fillna('NoVCode',inplace=True)
JNNUCHIn4rare.loc[JNNUCHIn4rare.Rarity == 'NoVCode', 'Ins_Sub'] = 'NoVCode'
JNNUCHIn4rare.drop_duplicates(['PT_No', 'In_Date', 'Ins_Sub'],inplace=True)
#JNNUCHIn4rare = JNNUCHIn4rare[JNNUCHIn4rare.Rarity != 'NoVCode']

demorare = JNNUCHIn4rare.groupby(by='Rarity', as_index=False).size().reset_index(name='count')
demorare.rename(columns={'Rarity':' ', 'count':'Counts'}, inplace=True)
demoraretot = JNNUCHIn.shape[0]
demorare['Ratio'] = (demorare.Counts / demoraretot) * 100
demorare.Ratio = demorare.Ratio.round(1)
demorare.Counts = demorare.apply(lambda x: "{:,}".format(x['Counts']), axis=1)
demorare['Ratio'] = demorare['Ratio'].astype(str)
demoraresub = {' ' : ['Mild', 'Severe', 'SevIncure', 'Rare', 'ExtRare', 'OtherChrom', 'NoVCode']}
demoraresub = pd.DataFrame(demoraresub)
demorare = demoraresub.merge(demorare, on=' ', how='left')
demorare.Counts.fillna('0', inplace=True)
demorare.Ratio.fillna('0.0', inplace=True)
demorare['Class'] = ' '
demorare = demorare[['Class', ' ', 'Counts', 'Ratio']]
demorare = demorare.append(pd.Series(['희귀질환(산정특례기호 기준)', ' ', ' ', ' '], index=demorare.columns), ignore_index=True)
demorare = demorare.reindex([7, 0, 1, 2, 3, 4, 5, 6])
demorare[' '] = demorare[' '].map({' ': ' ', 'Mild' : '경증질환', 'Severe' : '중증', 'SevIncure' : '중증난치질환', 'Rare' : '희귀질환', 'ExtRare' : '극희귀질환', 'OtherChrom' : '기타염색체질환', 'NoVCode' : '산정특례기호없음'})
print (demorare)


JNNUCHInDemo = pd.concat([demoage, demogender, demoreg, demoins, demorare, demosev], ignore_index=True)
InEpisode = JNNUCHIn.shape[0]
InEpisode = "{:,}".format(InEpisode)
JNNUCHInDemo = JNNUCHInDemo.append(pd.Series(['전체 입원에피소드 횟수', ' ', InEpisode, '100.0'], index=JNNUCHInDemo.columns), ignore_index=True)
JNNUCHInDemo = JNNUCHInDemo.reindex([46, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
                                     30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45])
JNNUCHInDemo = JNNUCHInDemo.rename(columns={'Class':'구분', ' ':' ', 'Counts': '건수(건)', 'Ratio':'비율(%)'})
print(JNNUCHInDemo)


#JNNUCHInDiagF_demo_age = pd.DataFrame(columns=['under1', '1to6', '7to12', '13to18', 'over18'])
#JNNUCHInDiagF_demo_gender = pd.DataFrame(columns=['Male', 'Female'])
#JNNUCHInDiagF_demo_region = pd.DataFrame(columns=['fSeoul', 'fBusan', 'fDaegu', 'fGwangju', 'fDaejeon', 'fIncheon', 'fJeju', 'fSejong', 'fJeonnam', 'fJeonbuk', 'fGyeongnam',
#                                              'fGyeongbuk', 'fChungnam', 'fChungbuk', 'fGangwon', 'fGyeonggi'])
#JNNUCHInDiagF_demo_sev = pd.DataFrame(columns=['Severe', 'Normal', 'Simple', 'SortError', 'NoSort'])
#JNNUCHInDiagF_demo_rare = pd.DataFrame(columns=['Rare', 'ElseSort']) - Sort Needed




## RAW DATA based one Diagnosis Frequency: append rows
JNNUCHIn4DiagFbase = JNNUCHIn.copy()
JNNUCHIn4DiagFbase.D_Code = JNNUCHIn4DiagFbase.D_Code.str.split('/')
JNNUCHIn4DiagFbase = JNNUCHIn4DiagFbase.apply(pd.Series.explode).reset_index(drop=True)
JNNUCHIn4DiagFbase.drop(['D_Name'], axis=1, inplace=True)
JNNUCHIn4DiagFbase = JNNUCHIn4DiagFbase.merge(DiagCN, on='D_Code', how='left')
JNNUCHIn4DiagFbase.D_Name.fillna('NoDiag', inplace=True)
JNNUCHIn4DiagFbase.loc[JNNUCHIn4DiagFbase.D_Name == 'NoDiag', 'D_Code'] = 'NoDiag'


print(JNNUCHIn4DiagFbase)
print(JNNUCHIn4DiagFbase.columns)
print(JNNUCHIn4DiagFbase.info())

JNNUCHIn4DiagFbase = JNNUCHIn4DiagFbase.drop_duplicates(['PT_No', 'In_Date', 'D_Code'], keep='first') # Need D_Date after for proper drop
JNNUCHIn4DiagFbase = JNNUCHIn4DiagFbase[JNNUCHIn4DiagFbase.D_Code != 'NoDiag']
print(JNNUCHIn4DiagFbase.info())
bfreq = JNNUCHIn4DiagFbase.groupby(by='D_Code', as_index=False).agg({'D_Name': pd.Series.count})
bfreq.rename(columns={'D_Name':'Frequency'}, inplace=True)
bfreq = bfreq[bfreq.D_Code != 'NoDiag']
bfreq.sort_values(by='Frequency', ascending=False, inplace=True)
bfreq.reset_index(drop=True, inplace=True)
bfreq.index += 1
bfreq = bfreq.rename_axis('Rank').reset_index()
bfreq['Ratio'] = (bfreq.Frequency / bfreq.Frequency.sum()) * 100
bfreq.Ratio = bfreq.Ratio.round(1)
bfreq.Frequency = bfreq.apply(lambda x: "{:,}".format(x['Frequency']), axis=1)
bfreq['Ratio'] = bfreq['Ratio'].astype(str)
bfreq['Frequency'] = bfreq['Frequency'].astype(str)
#print (bfreq)

bdname=JNNUCHIn4DiagFbase.groupby(by='D_Code', as_index=False).agg({'D_Name': lambda a: a.value_counts().index[0]})
bdname = bdname[bdname.D_Code != 'NoDiag']
bdname.reset_index(drop=True, inplace=True)
#print (dname)


## 02. BASIC INFORMATION based on Diagnosis Frequency
pnum=JNNUCHIn4DiagFbase.groupby(by='D_Code', as_index=False).agg({'PT_No': pd.Series.nunique})
pnum.rename(columns={'PT_No':'P_Num'}, inplace=True)
pnum = pnum[pnum.D_Code != 'NoDiag']
pnum['P_Num'] = pnum['P_Num'].astype(int)
pnum['P_Num'] = pnum.apply(lambda x: "{:,}".format(x['P_Num']), axis=1)
pnum.reset_index(drop=True, inplace=True)
#print (pnum)


avage=JNNUCHIn4DiagFbase.groupby(by='D_Code', as_index=False).agg({'Age': pd.Series.mean})
avage.rename(columns={'Age':'AvAge'}, inplace=True)
avage = avage[avage.D_Code != 'NoDiag']
avage.AvAge = avage.AvAge.round(1)
avage.AvAge = avage.AvAge.astype(str)
avage.reset_index(drop=True, inplace=True)
#print (avage)


avinprd=JNNUCHIn4DiagFbase.groupby(by='D_Code', as_index=False).agg({'In_Prd': pd.Series.mean})
avinprd.rename(columns={'In_Prd':'AvInPrd'}, inplace=True)
avinprd = avinprd[avinprd.D_Code != 'NoDiag']
avinprd.AvInPrd = avinprd.AvInPrd.round(1)
avinprd.AvInPrd = avinprd.AvInPrd.astype(str)
avinprd.reset_index(drop=True, inplace=True)
#print (avinprd)

'''
# No Surgery Info by Diagnosis
JNNUCHIn4DiagFbase_Sur = JNNUCHIn4DiagFbase.copy()
JNNUCHIn4DiagFbase_Sur.Sur_Code = JNNUCHIn4DiagFbase_Sur.Sur_Code.str.split('/')
JNNUCHIn4DiagFbase_Sur = JNNUCHIn4DiagFbase_Sur.apply(pd.Series.explode).reset_index(drop=True)
JNNUCHIn4DiagFbase_Sur = JNNUCHIn4DiagFbase_Sur.drop_duplicates(['PT_No', 'In_Date', 'D_Code', 'Sur_Code'], keep='first') # Need D_Date after for proper drop
JNNUCHIn4DiagFbase_Sur.drop(['Sur_Name'], axis=1, inplace=True)
JNNUCHIn4DiagFbase_Sur['Sur_Code'] = JNNUCHIn4DiagFbase_Sur['Sur_Code'].str[0:5]
JNNUCHIn4DiagFbase_Sur = JNNUCHIn4DiagFbase_Sur.merge(SurCN, on='Sur_Code', how='left')
JNNUCHIn4DiagFbase_Sur.Sur_Name.fillna('NoSur', inplace=True)
JNNUCHIn4DiagFbase_Sur.loc[JNNUCHIn4DiagFbase_Sur.Sur_Name == 'NoSur', 'Sur_Code'] = 'NoSur'
JNNUCHIn4DiagFbase_TotSur = JNNUCHIn4DiagFbase_Sur.copy()
JNNUCHIn4DiagFbase_OnlySur = JNNUCHIn4DiagFbase_Sur[JNNUCHIn4DiagFbase_Sur.Sur_Code != 'NoSur']
JNNUCHIn4DiagFbase_OnlySur = JNNUCHIn4DiagFbase_OnlySur[JNNUCHIn4DiagFbase_OnlySur.Sur_Code != 'GroupPay_JNNUCH']


with pd.option_context('display.max_columns', None):
    print(JNNUCHIn4DiagFbase_Sur)


##### another way to selcet most frequent value using groupby(Not Accurate in Certain Condition)#####

scode = JNNUCHIn4DiagFbase_OnlySur.groupby(by='D_Code', as_index=False)['Sur_Code'].agg(lambda x : x.value_counts().index[0])
scode.reset_index(drop=True, inplace=True)
sname = JNNUCHIn4DiagFbase_OnlySur.groupby(by='Sur_Code', as_index=False)['Sur_Name'].agg(lambda x : x.value_counts().index[0])
sname.reset_index(drop=True, inplace=True)
scodename = scode.merge(sname, on='Sur_Code', how='left')
scodename.reset_index(drop=True, inplace=True)
sfreq = JNNUCHIn4DiagFbase_OnlySur.groupby(by='D_Code', as_index=False)['Sur_Code'].agg(lambda x : x.value_counts().head(1))
sfreq.rename(columns={'Sur_Code':'Sfreq'}, inplace=True)
sfreq['Sfreq'] = sfreq['Sfreq'].astype('int64')
sfreq.reset_index(drop=True, inplace=True)
stot = JNNUCHIn4DiagFbase_TotSur.groupby(by='D_Code', as_index=False)['Sur_Code'].agg(lambda x : x.count())
stot.rename(columns={'Sur_Code':'Stot'}, inplace=True)
stot['Stot'] = stot['Stot'].astype('int64')
stot.reset_index(drop=True, inplace=True)
scodenamefreq = scodename.merge(sfreq,on='D_Code',how='left').merge(stot,on='D_Code',how='left')
scodenamefreq.rename(columns={'Sur_Code':'Scode', 'Sur_Name':'Sname'}, inplace=True)
scodenamefreq = scodenamefreq[scodenamefreq.D_Code != 'NoDiag']
scodenamefreq.reset_index(drop=True, inplace=True)
scodenamefreq['Sratio'] = (scodenamefreq.Sfreq / scodenamefreq.Stot) * 100
scodenamefreq.Sratio = scodenamefreq.Sratio.round(1)
scodenamefreq['Sratio'] = scodenamefreq['Sratio'].astype(str) + ' %'
'''

JNNUCHIn4DiagFbase_Ins = JNNUCHIn4DiagFbase.copy()
JNNUCHIn4DiagFbase_Ins['IndPaidExp'] = JNNUCHIn4DiagFbase_Ins['Pay_InsSelf'] + JNNUCHIn4DiagFbase_Ins['Pay_NoIns'] + JNNUCHIn4DiagFbase_Ins['Pay_Sel']
inspaid = JNNUCHIn4DiagFbase_Ins.groupby(by='D_Code', as_index=False).agg({'IndPaidExp' : pd.Series.mean, 'Pay_InsCorp' : pd.Series.mean})
inspaid.rename(columns={'Pay_InsCorp':'CorpPaidExp'}, inplace=True)
inspaid = inspaid[inspaid.D_Code != 'NoDiag']
inspaid['IndPaidExp'] = inspaid['IndPaidExp'].astype(int)
inspaid['CorpPaidExp'] = inspaid['CorpPaidExp'].astype(int)
inspaid['IndPaidExp'] = inspaid.apply(lambda x: "{:,}".format(x['IndPaidExp']), axis=1)
inspaid['CorpPaidExp'] = inspaid.apply(lambda x: "{:,}".format(x['CorpPaidExp']), axis=1)
inspaid.reset_index(drop=True, inplace=True)
#print (inspaid)

JNNUCHInDiagF_base = bfreq.merge(bdname,on='D_Code',how='left').merge(pnum,on='D_Code',how='left').merge(avage,on='D_Code',how='left').merge(avinprd,on='D_Code',how='left').merge(inspaid,on='D_Code',how='left')
JNNUCHInDiagF_base.rename(columns={'D_Code':'Dcode', 'D_Name':'Dname'}, inplace=True)
JNNUCHInDiagF_base = JNNUCHInDiagF_base[['Rank' ,'Dcode', 'Dname', 'Frequency', 'Ratio', 'P_Num', 'AvAge', 'AvInPrd', 'IndPaidExp', 'CorpPaidExp']]
JNNUCHInDiagF_base = JNNUCHInDiagF_base.rename(columns={'Rank' : '순위' ,'Dcode' : '진단코드', 'Dname' : '진단명', 'Frequency' : '입원 빈도(건)', 'Ratio' : '비율', 'P_Num' : '환자수(명)',
                                                      'AvAge' : '평균연령(세)', 'AvInPrd' : '평균재원기간(일)', 'IndPaidExp' : '본인부담금(원)', 'CorpPaidExp' : '공단부담금(원)'})
JNNUCHInDiagF_base.reset_index(drop=True, inplace=True)
#JNNUCHInDiagF_base['Scode'].fillna('NoSur', inplace=True)
#JNNUCHInDiagF_base['Sname'].fillna('NoSur', inplace=True)
#JNNUCHInDiagF_base['Sfreq'].fillna(0, inplace=True)
#JNNUCHInDiagF_base['Sratio'].fillna('0.0 %', inplace=True)

print(JNNUCHInDiagF_base)
print(JNNUCHInDiagF_base.columns)

JNNUCHInDiagF_base50 = JNNUCHInDiagF_base.loc[0:49, :]


## 03. SEVERITY INFORMATION based on Diagnosis Frequency
JNNUCHIn4DiagFbase_Sev = JNNUCHIn4DiagFbase.copy()
#JNNUCHIn4DiagFbase_Sev.DRGNO = JNNUCHIn4DiagFbase_Sev.DRGNO.str.split('/')
#JNNUCHIn4DiagFbase_Sev.Severity = JNNUCHIn4DiagFbase_Sev.Severity.str.split('/')
#JNNUCHIn4DiagFbase_Sev = JNNUCHIn4DiagFbase_Sev.apply(pd.Series.explode).reset_index(drop=True)

severe = JNNUCHIn4DiagFbase_Sev.groupby('D_Code')['Severity'].apply(lambda x: (x=='Severe').sum()).reset_index(name='Sev_Freq')
severe = severe[severe.D_Code != 'NoDiag']

normal = JNNUCHIn4DiagFbase_Sev.groupby('D_Code')['Severity'].apply(lambda x: (x=='Normal').sum()).reset_index(name='Norm_Freq')
normal = normal[normal.D_Code != 'NoDiag']

simple = JNNUCHIn4DiagFbase_Sev.groupby('D_Code')['Severity'].apply(lambda x: (x=='Simple').sum()).reset_index(name='Simple_Freq')
simple = simple[simple.D_Code != 'NoDiag']

sorterror = JNNUCHIn4DiagFbase_Sev.groupby('D_Code')['Severity'].apply(lambda x: (x=='SortError').sum()).reset_index(name='SrtErr_Freq')
sorterror = sorterror[sorterror.D_Code != 'NoDiag']

nodrg = JNNUCHIn4DiagFbase_Sev.groupby('D_Code')['Severity'].apply(lambda x: (x=='NoDRG').sum()).reset_index(name='NoDRG_Freq')
nodrg = nodrg[nodrg.D_Code != 'NoDiag']

JNNUCHInDiagF_svty = bfreq.merge(bdname,on='D_Code',how='left').merge(severe,on='D_Code',how='left').merge(normal,on='D_Code',how='left').merge(simple,on='D_Code',how='left').merge(sorterror,on='D_Code',how='left').merge(nodrg,on='D_Code',how='left')
JNNUCHInDiagF_svty.rename(columns={'D_Code':'Dcode', 'D_Name':'Dname'}, inplace=True)
JNNUCHInDiagF_svty = JNNUCHInDiagF_svty[['Rank', 'Dcode','Dname', 'Sev_Freq', 'Norm_Freq', 'Simple_Freq', 'SrtErr_Freq', 'NoDRG_Freq', 'Frequency', 'Ratio']]
JNNUCHInDiagF_svty.drop(['Frequency', 'Ratio'], axis=1, inplace=True)
JNNUCHInDiagF_svty.reset_index(drop=True, inplace=True)

JNNUCHInDiagF_svty['Sev_Ratio'] = (JNNUCHInDiagF_svty.Sev_Freq / (JNNUCHInDiagF_svty.Sev_Freq + JNNUCHInDiagF_svty.Norm_Freq + JNNUCHInDiagF_svty.Simple_Freq + JNNUCHInDiagF_svty.SrtErr_Freq + JNNUCHInDiagF_svty.NoDRG_Freq)) * 100
JNNUCHInDiagF_svty.Sev_Ratio = JNNUCHInDiagF_svty.Sev_Ratio.round(1)
JNNUCHInDiagF_svty['Sev_Ratio'] = JNNUCHInDiagF_svty['Sev_Ratio'].astype(str)

JNNUCHInDiagF_svty['Norm_Ratio'] = (JNNUCHInDiagF_svty.Norm_Freq / (JNNUCHInDiagF_svty.Sev_Freq + JNNUCHInDiagF_svty.Norm_Freq + JNNUCHInDiagF_svty.Simple_Freq + JNNUCHInDiagF_svty.SrtErr_Freq + JNNUCHInDiagF_svty.NoDRG_Freq)) * 100
JNNUCHInDiagF_svty.Norm_Ratio = JNNUCHInDiagF_svty.Norm_Ratio.round(1)
JNNUCHInDiagF_svty['Norm_Ratio'] = JNNUCHInDiagF_svty['Norm_Ratio'].astype(str)

JNNUCHInDiagF_svty['Simple_Ratio'] = (JNNUCHInDiagF_svty.Simple_Freq / (JNNUCHInDiagF_svty.Sev_Freq + JNNUCHInDiagF_svty.Norm_Freq + JNNUCHInDiagF_svty.Simple_Freq + JNNUCHInDiagF_svty.SrtErr_Freq + JNNUCHInDiagF_svty.NoDRG_Freq)) * 100
JNNUCHInDiagF_svty.Simple_Ratio = JNNUCHInDiagF_svty.Simple_Ratio.round(1)
JNNUCHInDiagF_svty['Simple_Ratio'] = JNNUCHInDiagF_svty['Simple_Ratio'].astype(str)

JNNUCHInDiagF_svty['SrtErr_Ratio'] = (JNNUCHInDiagF_svty.SrtErr_Freq / (JNNUCHInDiagF_svty.Sev_Freq + JNNUCHInDiagF_svty.Norm_Freq + JNNUCHInDiagF_svty.Simple_Freq + JNNUCHInDiagF_svty.SrtErr_Freq + JNNUCHInDiagF_svty.NoDRG_Freq)) * 100
JNNUCHInDiagF_svty.SrtErr_Ratio = JNNUCHInDiagF_svty.SrtErr_Ratio.round(1)
JNNUCHInDiagF_svty['SrtErr_Ratio'] = JNNUCHInDiagF_svty['SrtErr_Ratio'].astype(str)

JNNUCHInDiagF_svty['NoDRG_Ratio'] = (JNNUCHInDiagF_svty.NoDRG_Freq / (JNNUCHInDiagF_svty.Sev_Freq + JNNUCHInDiagF_svty.Norm_Freq + JNNUCHInDiagF_svty.Simple_Freq + JNNUCHInDiagF_svty.SrtErr_Freq + JNNUCHInDiagF_svty.NoDRG_Freq)) * 100
JNNUCHInDiagF_svty.NoDRG_Ratio = JNNUCHInDiagF_svty.NoDRG_Ratio.round(1)
JNNUCHInDiagF_svty['NoDRG_Ratio'] = JNNUCHInDiagF_svty['NoDRG_Ratio'].astype(str)

JNNUCHInDiagF_svty = JNNUCHInDiagF_svty[['Rank', 'Dcode', 'Dname', 'Sev_Freq', 'Sev_Ratio', 'Norm_Freq', 'Norm_Ratio', 'Simple_Freq', 'Simple_Ratio', 'SrtErr_Freq', 'SrtErr_Ratio', 'NoDRG_Freq', 'NoDRG_Ratio']]

JNNUCHInDiagF_svty['Sev_Freq'] = JNNUCHInDiagF_svty.apply(lambda x: "{:,}".format(x['Sev_Freq']), axis=1)
JNNUCHInDiagF_svty['Norm_Freq'] = JNNUCHInDiagF_svty.apply(lambda x: "{:,}".format(x['Norm_Freq']), axis=1)
JNNUCHInDiagF_svty['Simple_Freq'] = JNNUCHInDiagF_svty.apply(lambda x: "{:,}".format(x['Simple_Freq']), axis=1)
JNNUCHInDiagF_svty['SrtErr_Freq'] = JNNUCHInDiagF_svty.apply(lambda x: "{:,}".format(x['SrtErr_Freq']), axis=1)
JNNUCHInDiagF_svty['NoDRG_Freq'] = JNNUCHInDiagF_svty.apply(lambda x: "{:,}".format(x['NoDRG_Freq']), axis=1)

JNNUCHInDiagF_svty = JNNUCHInDiagF_svty.rename(columns={'Rank':'순위', 'Dcode':'진단코드', 'Dname':'진단명', 'Sev_Freq':'전문빈도(건)', 'Sev_Ratio':'전문비율(%)', 'Norm_Freq':'일반빈도(건)', 'Norm_Ratio':'일반비율(%)',
                                                      'Simple_Freq':'단순빈도(건)', 'Simple_Ratio':'단순비율(%)', 'SrtErr_Freq':'분류오류빈도(건)', 'SrtErr_Ratio':'분류오류비율(%)', 'NoDRG_Freq':'미분류빈도(건)',
                                                      'NoDRG_Ratio':'미분류비율(%)'})

JNNUCHInDiagF_svty50 = JNNUCHInDiagF_svty.loc[0:49, :]

#JNNUCHInDiagF_inst = pd.DataFrame(columns=['Rank' ,'Dcode', 'Dname', 'A_Ratio', 'B_Ratio', 'C_Ratio', 'D_Ratio', 'E_Ratio', 'F_Ratio', 'G_Ratio', 'H_Ratio'])
#JNNUCHInDiagF_regn = pd.DataFrame(columns=['Rank' ,'Dcode', 'Dname', 'fSeoul', 'fBusan', 'fDaegu', 'fGwangju', 'fDaejeon', 'fIncheon', 'fJeju', 'fSejong', 'fJeonnam', 'fJeonbuk',
#                                      'fGyeongnam', 'fGyeongbuk', 'fChungnam', 'fChungbuk', 'fGangwon', 'fGyeonggi'])




#JNNUCHInRDemo.to_csv('./JNNUCH/JNNUCH 입원환자 기본 인적사항(실환자 기준).csv', encoding='cp949', index=False)
#JNNUCHInDemo.to_csv('./JNNUCH/JNNUCH 입원환자 기본 인적사항(연환자 기준).csv', encoding='cp949', index=False)
JNNUCHInDiagF_base50.to_csv('./JNNUCH/JNNUCH 입원환자 주진단 빈도별 순위 (기본 사항).csv', encoding='cp949', index=False)
JNNUCHInDiagF_svty50.to_csv('./JNNUCH/JNNUCH 입원환자 주진단 빈도별 순위 (중증도 분류).csv', encoding='cp949', index=False)

JNNUCHInDiagF_base.to_csv('./JNNUCH/(원)JNNUCH 입원환자 주진단 빈도별 순위 (기본 사항).csv', encoding='cp949', index=False)
JNNUCHInDiagF_svty.to_csv('./JNNUCH/(원)JNNUCH 입원환자 주진단 빈도별 순위 (중증도 분류).csv', encoding='cp949', index=False)



'''
# visualize diag_count
dcount_snuh30 = dcount_snuh.loc[0:29, :] # cut top 30

print(dcount_snuh30)


fig, ax1 = plt.subplots()

color1 = 'darkgreen'
ax1.set_xlabel('Dcode')
ax1.set_ylabel('Frequency', color=color1)
ax1.bar(dcount_snuh30.Dcode, dcount_snuh30.Frequency, color=color1)
ax1.tick_params(axis='y', labelcolor=color1)


ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

color2 = 'darkred'
ax2.set_ylabel('Ratio', color=color2)  # we already handled the x-label with ax1
ax2.plot(dcount_snuh30.Dcode, dcount_snuh30.Ratio, color=color2)
ax2.tick_params(axis='y', labelcolor=color2)


fig.tight_layout()  # otherwise the right y-label is slightly clipped
'''
#plt.show()
'''
'''

