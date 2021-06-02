'''
This script is written 4 preprocessing KBNUCH's Inpatients' Data.

Written Date: 2020.01.07
Written By: Peter JH Park

'''

### Import modules in needs

import os, sys, csv
import pandas as pd
import numpy as np
import datetime, time

print("\n Current Working Directory is: ", os.getcwd())

### READ Files & Check
KBNUCHIn = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018KBNUCHIn_Prep.csv", encoding="utf-8", low_memory=False)
SevPri = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018-2019SevCri_5digitPri.csv", encoding="utf-8", low_memory=False)
SevSub = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018-2019SevCri_4digitSub.csv", encoding="utf-8", low_memory=False)


### Preprocessing


## 01. Renaming Columns
KBNUCHIn.reset_index(drop=True, inplace=True)
print(KBNUCHIn.columns)

'''
Index(['환자ID', '생년월일', '성별', '광역시/도', '시/군/구', '보험급종', '산정특례 특정기호', '입원일',
       '입원진료과', '퇴원일', '퇴원진료과', '재원일수', 'KDRG번호', '주진단코드1', '주진단코드명1',
       '부진단코드2', '부진단코드명2', '부진단코드3', '부진단코드명3', '부진단코드4', '부진단코드명4', '부진단코드5',
       '부진단코드명5', '부진단코드6', '부진단코드명6', '수술일1', '수술\n보험코드', '수술명',
       '수술 집도의\n소속진료과', '수술일2', '수술\n보험코드.1', '수술명.1', '수술 집도의\n소속진료과.1',
       '일부본인부담\n본인부담금', '일부본인부담\n공단부담금', '전액본인부담', '비급여'],
      dtype='object')
'''

'''
result: Index(['InstName', 'PT_No', 'Birth', 'Age', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'In_Date', 'In_Dep', 'Dis_Date', 'Dis_Dep', 'In_Prd', 'Pay_InsSelf',
                   'Pay_InsCorp', 'Pay_NoIns', 'Pay_Sel', 'D_Code', 'D_Name', 'DRGNO', 'Severity', 'Sur_Date', 'Sur_Code', 'Sur_Name'])
'''

KBNUCHIn.rename(columns={KBNUCHIn.columns[0] : 'PT_No', KBNUCHIn.columns[1] : 'Birth', KBNUCHIn.columns[2] : 'Gender', KBNUCHIn.columns[3] : 'Address', KBNUCHIn.columns[5] : 'Ins_Var',
                         KBNUCHIn.columns[6] : 'Ins_Sub', KBNUCHIn.columns[7] : 'In_Date', KBNUCHIn.columns[8] : 'In_Dep', KBNUCHIn.columns[9] : 'Dis_Date', KBNUCHIn.columns[10] : 'Dis_Dep',
                         KBNUCHIn.columns[11] : 'In_Prd', KBNUCHIn.columns[12] : 'DRGNO', KBNUCHIn.columns[13] : 'D_Code1', KBNUCHIn.columns[14] : 'D_Name1', KBNUCHIn.columns[15] : 'D_Code2',
                         KBNUCHIn.columns[16] : 'D_Name2', KBNUCHIn.columns[17] : 'D_Code3', KBNUCHIn.columns[18] : 'D_Name3', KBNUCHIn.columns[19] : 'D_Code4', KBNUCHIn.columns[20] : 'D_Name4',
                         KBNUCHIn.columns[21] : 'D_Code5', KBNUCHIn.columns[22] : 'D_Name5', KBNUCHIn.columns[23] : 'D_Code6', KBNUCHIn.columns[24] : 'D_Name6',
                         KBNUCHIn.columns[25] : 'Sur_Date1', KBNUCHIn.columns[26] : 'Sur_Code1', KBNUCHIn.columns[27] : 'Sur_Name1', KBNUCHIn.columns[28] : 'Sur_Dep1',
                         KBNUCHIn.columns[29] : 'Sur_Date2', KBNUCHIn.columns[30] : 'Sur_Code2', KBNUCHIn.columns[31] : 'Sur_Name2', KBNUCHIn.columns[32] : 'Sur_Dep2', KBNUCHIn.columns[33] : 'Pay_InsSelf',
                         KBNUCHIn.columns[34] : 'Pay_InsCorp', KBNUCHIn.columns[35] : 'Pay_Sel', KBNUCHIn.columns[36] : 'Pay_NoIns'}, inplace=True)


'''
print(KBNUCHIn.columns)
'''
KBNUCHIn.dropna(how='all', axis=0, inplace=True)
print(KBNUCHIn)

## 02. Deleting Unnecessary Columns
'''
print(KBNUCHIn.loc[0:5])
'''

del KBNUCHIn['시/군/구']

# Diagnosis
KBNUCHIn['D_Code1'] = KBNUCHIn['D_Code1'].fillna('NoDiag')
KBNUCHIn['D_Code1'] = KBNUCHIn['D_Code1'].astype(str); KBNUCHIn['D_Code1'] = KBNUCHIn['D_Code1'].str.replace('/',',')
KBNUCHIn['D_Name1'] = KBNUCHIn['D_Name1'].fillna('NoDiag')
KBNUCHIn['D_Name1'] = KBNUCHIn['D_Name1'].astype(str); KBNUCHIn['D_Name1'] = KBNUCHIn['D_Name1'].str.replace('/',',')
KBNUCHIn['D_Code2'] = KBNUCHIn['D_Code2'].fillna('NoDiag')
KBNUCHIn['D_Code2'] = KBNUCHIn['D_Code2'].astype(str); KBNUCHIn['D_Code2'] = KBNUCHIn['D_Code2'].str.replace('/',',')
KBNUCHIn['D_Name2'] = KBNUCHIn['D_Name2'].fillna('NoDiag')
KBNUCHIn['D_Name2'] = KBNUCHIn['D_Name2'].astype(str); KBNUCHIn['D_Name2'] = KBNUCHIn['D_Name2'].str.replace('/',',')
KBNUCHIn['D_Code3'] = KBNUCHIn['D_Code3'].fillna('NoDiag')
KBNUCHIn['D_Code3'] = KBNUCHIn['D_Code3'].astype(str); KBNUCHIn['D_Code3'] = KBNUCHIn['D_Code3'].str.replace('/',',')
KBNUCHIn['D_Name3'] = KBNUCHIn['D_Name3'].fillna('NoDiag')
KBNUCHIn['D_Name3'] = KBNUCHIn['D_Name3'].astype(str); KBNUCHIn['D_Name3'] = KBNUCHIn['D_Name3'].str.replace('/',',')
KBNUCHIn['D_Code4'] = KBNUCHIn['D_Code4'].fillna('NoDiag')
KBNUCHIn['D_Code4'] = KBNUCHIn['D_Code4'].astype(str); KBNUCHIn['D_Code4'] = KBNUCHIn['D_Code4'].str.replace('/',',')
KBNUCHIn['D_Name4'] = KBNUCHIn['D_Name4'].fillna('NoDiag')
KBNUCHIn['D_Name4'] = KBNUCHIn['D_Name4'].astype(str); KBNUCHIn['D_Name4'] = KBNUCHIn['D_Name4'].str.replace('/',',')
KBNUCHIn['D_Code5'] = KBNUCHIn['D_Code5'].fillna('NoDiag')
KBNUCHIn['D_Code5'] = KBNUCHIn['D_Code5'].astype(str); KBNUCHIn['D_Code5'] = KBNUCHIn['D_Code5'].str.replace('/',',')
KBNUCHIn['D_Name5'] = KBNUCHIn['D_Name5'].fillna('NoDiag')
KBNUCHIn['D_Name5'] = KBNUCHIn['D_Name5'].astype(str); KBNUCHIn['D_Name5'] = KBNUCHIn['D_Name5'].str.replace('/',',')
KBNUCHIn['D_Code6'] = KBNUCHIn['D_Code6'].fillna('NoDiag')
KBNUCHIn['D_Code6'] = KBNUCHIn['D_Code6'].astype(str); KBNUCHIn['D_Code6'] = KBNUCHIn['D_Code6'].str.replace('/',',')
KBNUCHIn['D_Name6'] = KBNUCHIn['D_Name6'].fillna('NoDiag')
KBNUCHIn['D_Name6'] = KBNUCHIn['D_Name6'].astype(str); KBNUCHIn['D_Name6'] = KBNUCHIn['D_Name6'].str.replace('/',',')

KBNUCHIn.loc[KBNUCHIn.D_Code1 != 'NoDiag', 'D_Code1'] = KBNUCHIn.D_Code1.str[0:4]
KBNUCHIn.loc[KBNUCHIn.D_Code2 != 'NoDiag', 'D_Code2'] = KBNUCHIn.D_Code2.str[0:4]
KBNUCHIn.loc[KBNUCHIn.D_Code3 != 'NoDiag', 'D_Code3'] = KBNUCHIn.D_Code3.str[0:4]
KBNUCHIn.loc[KBNUCHIn.D_Code4 != 'NoDiag', 'D_Code4'] = KBNUCHIn.D_Code4.str[0:4]
KBNUCHIn.loc[KBNUCHIn.D_Code5 != 'NoDiag', 'D_Code5'] = KBNUCHIn.D_Code5.str[0:4]
KBNUCHIn.loc[KBNUCHIn.D_Code6 != 'NoDiag', 'D_Code6'] = KBNUCHIn.D_Code6.str[0:4]

KBNUCHIn['D_Code'] = KBNUCHIn['D_Code1']+'/'+KBNUCHIn['D_Code2']+'/'+KBNUCHIn['D_Code3']+'/'+KBNUCHIn['D_Code4']+'/'+KBNUCHIn['D_Code5']+'/'+KBNUCHIn['D_Code6']
KBNUCHIn['D_Name'] = KBNUCHIn['D_Name1']+'/'+KBNUCHIn['D_Name2']+'/'+KBNUCHIn['D_Name3']+'/'+KBNUCHIn['D_Name4']+'/'+KBNUCHIn['D_Name5']+'/'+KBNUCHIn['D_Name6']

del KBNUCHIn['D_Code1']; del KBNUCHIn['D_Code2']; del KBNUCHIn['D_Code3']; del KBNUCHIn['D_Code4']; del KBNUCHIn['D_Code5']; del KBNUCHIn['D_Code6']
del KBNUCHIn['D_Name1']; del KBNUCHIn['D_Name2']; del KBNUCHIn['D_Name3']; del KBNUCHIn['D_Name4']; del KBNUCHIn['D_Name5']; del KBNUCHIn['D_Name6']


# Surgery
KBNUCHIn['Sur_Code1'] = KBNUCHIn['Sur_Code1'].fillna('NoSur')
KBNUCHIn['Sur_Code1'] = KBNUCHIn['Sur_Code1'].astype(str); KBNUCHIn['Sur_Code1'] = KBNUCHIn['Sur_Code1'].str.replace('/',',')
KBNUCHIn['Sur_Name1'] = KBNUCHIn['Sur_Name1'].fillna('NoSur')
KBNUCHIn['Sur_Name1'] = KBNUCHIn['Sur_Name1'].astype(str); KBNUCHIn['Sur_Name1'] = KBNUCHIn['Sur_Name1'].str.replace('/',',')
print(KBNUCHIn['Sur_Date1'])
KBNUCHIn['Sur_Date1'] = KBNUCHIn['Sur_Date1'].fillna('NoSur')
KBNUCHIn['Sur_Date1'] = KBNUCHIn['Sur_Date1'].astype(str); KBNUCHIn['Sur_Date1'] = KBNUCHIn['Sur_Date1'].str.replace('.',''); KBNUCHIn['Sur_Date1'] = KBNUCHIn['Sur_Date1'].astype(str).str[:-1]
print(KBNUCHIn['Sur_Date1'])
KBNUCHIn['Sur_Code2'] = KBNUCHIn['Sur_Code2'].fillna('NoSur')
KBNUCHIn['Sur_Code2'] = KBNUCHIn['Sur_Code2'].astype(str); KBNUCHIn['Sur_Code2'] = KBNUCHIn['Sur_Code2'].str.replace('/',',')
KBNUCHIn['Sur_Name2'] = KBNUCHIn['Sur_Name2'].fillna('NoSur')
KBNUCHIn['Sur_Name2'] = KBNUCHIn['Sur_Name2'].astype(str); KBNUCHIn['Sur_Name2'] = KBNUCHIn['Sur_Name2'].str.replace('/',',')
print(KBNUCHIn['Sur_Date2'])
KBNUCHIn['Sur_Date2'] = KBNUCHIn['Sur_Date2'].fillna('NoSur')
KBNUCHIn['Sur_Date2'] = KBNUCHIn['Sur_Date2'].astype(str); KBNUCHIn['Sur_Date2'] = KBNUCHIn['Sur_Date2'].str.replace('.',''); KBNUCHIn['Sur_Date2'] = KBNUCHIn['Sur_Date2'].astype(str).str[:-1]
print(KBNUCHIn['Sur_Date2'])

KBNUCHIn['Sur_Code'] = KBNUCHIn['Sur_Code1']+'/'+KBNUCHIn['Sur_Code2']
KBNUCHIn['Sur_Name'] = KBNUCHIn['Sur_Name1']+'/'+KBNUCHIn['Sur_Name2']
KBNUCHIn['Sur_Date'] = KBNUCHIn['Sur_Date1']+'/'+KBNUCHIn['Sur_Date2']

del KBNUCHIn['Sur_Code1']; del KBNUCHIn['Sur_Code2']
del KBNUCHIn['Sur_Name1']; del KBNUCHIn['Sur_Name2']
del KBNUCHIn['Sur_Date1']; del KBNUCHIn['Sur_Date2']
del KBNUCHIn['Sur_Dep1']; del KBNUCHIn['Sur_Dep2']


# Severity
print(SevPri.columns)
SevPri.rename(columns={SevPri.columns[0] : 'DRGNO01', SevPri.columns[1] : 'Severity01'}, inplace=True)
SevPri['Severity01'].fillna('분류오류', inplace=True)
print(SevPri)
KBNUCHIn['DRGNO01'] = KBNUCHIn['DRGNO'].astype(str).str[0:5]
KBNUCHIn['DRGNO01'].fillna('NoDRG', inplace=True)

print(SevSub.columns)
SevSub.rename(columns={SevSub.columns[0] : 'DRGNO02', SevSub.columns[1] : 'Severity02'}, inplace=True)
print(SevSub)
KBNUCHIn['DRGNO02'] = KBNUCHIn['DRGNO'].astype(str).str[0:4]
KBNUCHIn['DRGNO02'].fillna('NoDRG', inplace=True)

KBNUCHIn = KBNUCHIn.merge(SevPri, on='DRGNO01', how='left').merge(SevSub, on='DRGNO02', how='left')

KBNUCHIn.drop_duplicates(['PT_No', 'Birth', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'In_Date', 'In_Dep', 'Dis_Date', 'Dis_Dep', 'In_Prd', 'DRGNO', 'Pay_InsSelf',
                          'Pay_InsCorp', 'Pay_Sel', 'Pay_NoIns', 'D_Code', 'D_Name', 'Sur_Code', 'Sur_Name', 'Sur_Date', 'DRGNO01', 'DRGNO02', 'Severity01', 'Severity02'], inplace=True)

KBNUCHIn.reset_index(drop=True, inplace=True)


KBNUCHIn['Severity01'] = KBNUCHIn['Severity01'].map({'전문' : 'Severe', '일반' : 'Normal', '단순' : 'Simple', '분류오류' : 'SortError'})
KBNUCHIn['Severity02'] = KBNUCHIn['Severity02'].map({'전문' : 'Severe', '일반' : 'Normal', '단순' : 'Simple', '분류오류' : 'SortError'})

KBNUCHIn['Severity01'] = KBNUCHIn['Severity01'].map({'Severe' : 4, 'Normal' : 3, 'Simple' : 2, 'SortError' : 1})
KBNUCHIn['Severity01'] = KBNUCHIn['Severity01'].fillna(0)
KBNUCHIn['Severity02'] = KBNUCHIn['Severity02'].map({'Severe' : 4, 'Normal' : 3, 'Simple' : 2, 'SortError' : 1})
KBNUCHIn['Severity02'] = KBNUCHIn['Severity02'].fillna(0)

KBNUCHIn.loc[(KBNUCHIn['Severity01']  >= KBNUCHIn['Severity02']), 'Severity'] = KBNUCHIn['Severity01']
KBNUCHIn.loc[(KBNUCHIn['Severity01']  < KBNUCHIn['Severity02']), 'Severity'] = KBNUCHIn['Severity02']

KBNUCHIn['Severity01'] = KBNUCHIn['Severity01'].map({4 :'Severe', 3 :'Normal', 2 :'Simple', 1 :'SortError', 0 :'NoDRG'})
KBNUCHIn['Severity02'] = KBNUCHIn['Severity02'].map({4 :'Severe', 3 :'Normal', 2 :'Simple', 1 :'SortError', 0 :'NoDRG'})
KBNUCHIn['Severity'] = KBNUCHIn['Severity'].map({4 :'Severe', 3 :'Normal', 2 :'Simple', 1 :'SortError', 0 :'NoDRG'})

KBNUCHIn.loc[(KBNUCHIn['Severity']  == 'NoDRG'), 'DRGNO'] = 'NoDRG'

print(KBNUCHIn.columns)
print(KBNUCHIn)



## 03. Refine KBNUCHIn

KBNUCHIn.drop_duplicates(['PT_No', 'In_Date'], inplace=True)
KBNUCHIn.reset_index(drop=True, inplace=True)

KBNUCHIn['DRGNO'] = KBNUCHIn['DRGNO'].fillna('NoDRG')
KBNUCHIn['Severity'] = KBNUCHIn['Severity'].fillna('NoDRG')

KBNUCHIn['Sur_Date'] = KBNUCHIn['Sur_Date'].fillna('NoSur')
KBNUCHIn['Sur_Code'] = KBNUCHIn['Sur_Code'].fillna('NoSur')
KBNUCHIn['Sur_Name'] = KBNUCHIn['Sur_Name'].fillna('NoSur')


KBNUCHIn['Address'] = KBNUCHIn['Address'].fillna('NoAdd')
KBNUCHIn['Ins_Var'] = KBNUCHIn['Ins_Var'].fillna('NoInsVar')
KBNUCHIn['Ins_Sub'] = KBNUCHIn['Ins_Sub'].fillna('NoVCode')
KBNUCHIn['In_Prd'] = KBNUCHIn['In_Prd'].fillna(0)
KBNUCHIn['Pay_InsSelf'] = KBNUCHIn['Pay_InsSelf'].fillna(0)
KBNUCHIn['Pay_InsCorp'] = KBNUCHIn['Pay_InsCorp'].fillna(0)
KBNUCHIn['Pay_NoIns'] = KBNUCHIn['Pay_NoIns'].fillna(0)
KBNUCHIn['Pay_Sel'] = KBNUCHIn['Pay_Sel'].fillna(0)

KBNUCHIn.loc[KBNUCHIn['Ins_Sub'] != 'NoVCode', 'Ins_Sub'] = KBNUCHIn.Ins_Sub.str[0:4]

KBNUCHIn.dropna(how='any')
'''
print(len(KBNUCHIn))
'''

print(KBNUCHIn)


KBNUCHIn['PT_No'] = KBNUCHIn['PT_No'].astype(str)
KBNUCHIn['Birth'] = KBNUCHIn['Birth'].astype(int)
KBNUCHIn['Gender'] = KBNUCHIn['Gender'].astype(str)
KBNUCHIn['Address'] = KBNUCHIn['Address'].astype(str)
KBNUCHIn['Ins_Var'] = KBNUCHIn['Ins_Var'].astype(str)
KBNUCHIn['Ins_Sub'] = KBNUCHIn['Ins_Sub'].astype(str)
KBNUCHIn['In_Date'] = KBNUCHIn['In_Date'].astype(int)
KBNUCHIn['In_Dep'] = KBNUCHIn['In_Dep'].astype(str)
KBNUCHIn['Dis_Date'] = KBNUCHIn['Dis_Date'].astype(int)
KBNUCHIn['Dis_Dep'] = KBNUCHIn['Dis_Dep'].astype(str)
KBNUCHIn['In_Prd'] = KBNUCHIn['In_Prd'].astype(int)
KBNUCHIn['Pay_InsSelf'] = KBNUCHIn['Pay_InsSelf'].astype(str); KBNUCHIn['Pay_InsSelf'] = KBNUCHIn['Pay_InsSelf'].str.replace(',',''); KBNUCHIn['Pay_InsSelf'] = KBNUCHIn['Pay_InsSelf'].str.replace(' ','')
KBNUCHIn['Pay_InsSelf'] = KBNUCHIn['Pay_InsSelf'].astype(int)
KBNUCHIn['Pay_InsCorp'] = KBNUCHIn['Pay_InsCorp'].astype(str); KBNUCHIn['Pay_InsCorp'] = KBNUCHIn['Pay_InsCorp'].str.replace(',',''); KBNUCHIn['Pay_InsCorp'] = KBNUCHIn['Pay_InsCorp'].str.replace(' ','')
KBNUCHIn['Pay_InsCorp'] = KBNUCHIn['Pay_InsCorp'].astype(int)
KBNUCHIn['Pay_NoIns'] = KBNUCHIn['Pay_NoIns'].astype(str); KBNUCHIn['Pay_NoIns'] = KBNUCHIn['Pay_NoIns'].str.replace(',',''); KBNUCHIn['Pay_NoIns'] = KBNUCHIn['Pay_NoIns'].str.replace(' ','')
KBNUCHIn['Pay_NoIns'] = KBNUCHIn['Pay_NoIns'].astype(int)
KBNUCHIn['Pay_Sel'] = KBNUCHIn['Pay_Sel'].astype(str); KBNUCHIn['Pay_Sel'] = KBNUCHIn['Pay_Sel'].str.replace(',',''); KBNUCHIn['Pay_Sel'] = KBNUCHIn['Pay_Sel'].str.replace(' ','')
KBNUCHIn['Pay_Sel'] = KBNUCHIn['Pay_Sel'].astype(int)
KBNUCHIn['D_Code'] = KBNUCHIn['D_Code'].astype(str)
KBNUCHIn['D_Name'] = KBNUCHIn['D_Name'].astype(str)
KBNUCHIn['DRGNO'] = KBNUCHIn['DRGNO'].astype(str)
KBNUCHIn['Severity'] = KBNUCHIn['Severity'].astype(str)
KBNUCHIn['Sur_Date'] = KBNUCHIn['Sur_Date'].astype(str)
KBNUCHIn['Sur_Code'] = KBNUCHIn['Sur_Code'].astype(str)
KBNUCHIn['Sur_Name'] = KBNUCHIn['Sur_Name'].astype(str)



# 04-1) Change values in 'Gender' Column ( '1' : Male, '2' : Female)
KBNUCHIn.Gender.replace(['1.0', '2.0'], ['Male', 'Female'], inplace=True)

# 04-2) Create 'Age' Column
# df.loc[df['column name'] condition, 'new column name'] = 'value if condition is met'
KBNUCHIn.loc[(KBNUCHIn['In_Date'] % 10000 <= KBNUCHIn['Birth'] % 10000) & (KBNUCHIn['In_Date'] // 10000 >= KBNUCHIn['Birth'] // 10000), 'Age'] = (KBNUCHIn['In_Date'] - KBNUCHIn['Birth'])// 10000
KBNUCHIn.loc[(KBNUCHIn['In_Date'] % 10000 > KBNUCHIn['Birth'] % 10000) & (KBNUCHIn['In_Date'] // 10000 == KBNUCHIn['Birth'] // 10000), 'Age'] = (KBNUCHIn['In_Date'] - KBNUCHIn['Birth'])// 10000
KBNUCHIn.loc[(KBNUCHIn['In_Date'] % 10000 > KBNUCHIn['Birth'] % 10000) & (KBNUCHIn['In_Date'] // 10000 > KBNUCHIn['Birth'] // 10000), 'Age'] = (KBNUCHIn['In_Date'] - KBNUCHIn['Birth'])// 10000 - 1


KBNUCHIn['Age'] = KBNUCHIn['Age'].astype(int)


print(KBNUCHIn.columns)
print(KBNUCHIn['Age'])



# 04-3) Rephrase 'Address' Values
class dict_partial(dict):
    def __getitem__(self, value):
        for k in self.keys():
            if k in value:
                return self.get(k)
        else:
            return self.get(None)

address_map = dict_partial({'서울': 'seoul', '부산': 'busan', '울산': 'ulsan', '대구': 'daegu', '광주': 'gwangju', '대전': 'daejeon', '제주': 'jeju', '세종': 'sejong', '인천': 'incheon',
                            '전남': 'jeonnam', '전라남도': 'jeonnam', '전북': 'jeonbuk', '전라북도' : 'jeonbuk', '경남': 'gyeongnam', '경상남도': 'gyeongnam', '경북': 'gyeongbuk', '경상북도': 'gyeongbuk',
                            '충남': 'chungnam', '충청남도': 'chungnam', '충북': 'chungbuk', '충청북도' : 'chungbuk', '강원': 'gangwon', '경기': 'gyeonggi'})

KBNUCHIn['Address'] = KBNUCHIn['Address'].apply(lambda x: address_map[x])


KBNUCHIn['Address'] = KBNUCHIn['Address'].fillna('NoAdd')




# 04-4) Rephrase 'Ins_Var' Values

KBNUCHIn['Ins_Var'] = KBNUCHIn['Ins_Var'].map({'1.0' : 'NHIS', '2.0' : 'MedCareT1', '3.0' : 'MedCareT2', '4.0' : 'MedCareDis'})
KBNUCHIn['Ins_Var'] = KBNUCHIn['Ins_Var'].fillna('Others')


# 04-5) Change Order of Columns
KBNUCHIn['InstName'] = np.nan
KBNUCHIn['InstName'] = KBNUCHIn['InstName'].fillna('KBNUCH')
KBNUCHIn = KBNUCHIn[['InstName', 'PT_No', 'Birth', 'Age', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'In_Date', 'In_Dep', 'Dis_Date', 'Dis_Dep', 'In_Prd', 'Pay_InsSelf',
                     'Pay_InsCorp', 'Pay_NoIns', 'Pay_Sel', 'D_Code', 'D_Name', 'DRGNO', 'Severity', 'Sur_Date', 'Sur_Code', 'Sur_Name']]


print(KBNUCHIn.columns)

with pd.option_context('display.max_columns', None):
    print(KBNUCHIn.loc[316:320, : ])




### Save as CSV in '/Users/peterpark/Desktop/PY_START/SNUH_Project01'

KBNUCHIn.to_csv('./KBNUCH/KBNUCHInP_R4A.csv', index=False)

