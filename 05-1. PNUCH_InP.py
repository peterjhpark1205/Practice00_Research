'''
This script is written 4 preprocessing PNUCH's Inpatients' Data.

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
PNUCHIn = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018PNUCHIn_Prep.csv", encoding="utf-8", low_memory=False)
SevPri = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018-2019SevCri_5digitPri.csv", encoding="utf-8", low_memory=False)
SevSub = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018-2019SevCri_4digitSub.csv", encoding="utf-8", low_memory=False)


### Preprocessing


## 01. Renaming Columns
PNUCHIn.reset_index(drop=True, inplace=True)
print(PNUCHIn.columns)

'''
['환자ID', '생년월일', '성별', '주소', '시/군/구', '보험급종', '산정특례 특정기호', '입원일',
       '입원진료과', '퇴원일', '퇴원진료과', '재원일수', 'KDRG번호', '주진단코드1', '주진단코드명1',
       '부진단코드2', '부진단코드명2', '부진단코드3', '부진단코드명3', '부진단코드4', '부진단코드명4', '부진단코드5',
       '부진단코드명5', '수술일1', '수술\n보험코드', '수술명', '수술 집도의\n소속진료과', '수술일2',
       '수술\n보험코드.1', '수술명.1', '수술 집도의\n소속진료과.1', '일부본인부담\n본인부담금',
       '일부본인부담\n공단부담금', '전액본인부담', '비급여']
'''

'''
result: Index(['InstName', 'PT_No', 'Birth', 'Age', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'In_Date', 'In_Dep', 'Dis_Date', 'Dis_Dep', 'In_Prd', 'Pay_InsSelf',
                   'Pay_InsCorp', 'Pay_NoIns', 'Pay_Sel', 'D_Code', 'D_Name', 'DRGNO', 'Severity', 'Sur_Date', 'Sur_Code', 'Sur_Name'])
'''

PNUCHIn.rename(columns={PNUCHIn.columns[0] : 'PT_No', PNUCHIn.columns[1] : 'Birth', PNUCHIn.columns[2] : 'Gender', PNUCHIn.columns[3] : 'Address', PNUCHIn.columns[5] : 'Ins_Var',
                        PNUCHIn.columns[6] : 'Ins_Sub', PNUCHIn.columns[7] : 'In_Date', PNUCHIn.columns[8] : 'In_Dep', PNUCHIn.columns[9] : 'Dis_Date', PNUCHIn.columns[10] : 'Dis_Dep',
                        PNUCHIn.columns[11] : 'In_Prd', PNUCHIn.columns[12] : 'DRGNO', PNUCHIn.columns[13] : 'D_Code1', PNUCHIn.columns[14] : 'D_Name1', PNUCHIn.columns[15] : 'D_Code2',
                        PNUCHIn.columns[16] : 'D_Name2', PNUCHIn.columns[17] : 'D_Code3', PNUCHIn.columns[18] : 'D_Name3', PNUCHIn.columns[19] : 'D_Code4', PNUCHIn.columns[20] : 'D_Name4',
                        PNUCHIn.columns[21] : 'D_Code5', PNUCHIn.columns[22] : 'D_Name5', PNUCHIn.columns[23] : 'Sur_Date1', PNUCHIn.columns[24] : 'Sur_Code1', PNUCHIn.columns[25] : 'Sur_Name1',
                        PNUCHIn.columns[26] : 'Sur_Dep1', PNUCHIn.columns[27] : 'Sur_Date2', PNUCHIn.columns[28] : 'Sur_Code2', PNUCHIn.columns[29] : 'Sur_Name2', PNUCHIn.columns[30] : 'Sur_Dep2',
                        PNUCHIn.columns[31] : 'Pay_InsSelf', PNUCHIn.columns[32] : 'Pay_InsCorp', PNUCHIn.columns[33] : 'Pay_Sel', PNUCHIn.columns[34] : 'Pay_NoIns'}, inplace=True)


'''
print(PNUCHIn.columns)
'''
PNUCHIn.dropna(how='all', axis=0, inplace=True)
print(PNUCHIn)

## 02. Deleting Unnecessary Columns
'''
print(PNUCHIn.loc[0:5])
'''

del PNUCHIn['시/군/구']

# Diagnosis
PNUCHIn['D_Code1'] = PNUCHIn['D_Code1'].fillna('NoDiag')
PNUCHIn['D_Code1'] = PNUCHIn['D_Code1'].astype(str); PNUCHIn['D_Code1'] = PNUCHIn['D_Code1'].str.replace('/',',')
PNUCHIn['D_Name1'] = PNUCHIn['D_Name1'].fillna('NoDiag')
PNUCHIn['D_Name1'] = PNUCHIn['D_Name1'].astype(str); PNUCHIn['D_Name1'] = PNUCHIn['D_Name1'].str.replace('/',',')
PNUCHIn['D_Code2'] = PNUCHIn['D_Code2'].fillna('NoDiag')
PNUCHIn['D_Code2'] = PNUCHIn['D_Code2'].astype(str); PNUCHIn['D_Code2'] = PNUCHIn['D_Code2'].str.replace('/',',')
PNUCHIn['D_Name2'] = PNUCHIn['D_Name2'].fillna('NoDiag')
PNUCHIn['D_Name2'] = PNUCHIn['D_Name2'].astype(str); PNUCHIn['D_Name2'] = PNUCHIn['D_Name2'].str.replace('/',',')
PNUCHIn['D_Code3'] = PNUCHIn['D_Code3'].fillna('NoDiag')
PNUCHIn['D_Code3'] = PNUCHIn['D_Code3'].astype(str); PNUCHIn['D_Code3'] = PNUCHIn['D_Code3'].str.replace('/',',')
PNUCHIn['D_Name3'] = PNUCHIn['D_Name3'].fillna('NoDiag')
PNUCHIn['D_Name3'] = PNUCHIn['D_Name3'].astype(str); PNUCHIn['D_Name3'] = PNUCHIn['D_Name3'].str.replace('/',',')
PNUCHIn['D_Code4'] = PNUCHIn['D_Code4'].fillna('NoDiag')
PNUCHIn['D_Code4'] = PNUCHIn['D_Code4'].astype(str); PNUCHIn['D_Code4'] = PNUCHIn['D_Code4'].str.replace('/',',')
PNUCHIn['D_Name4'] = PNUCHIn['D_Name4'].fillna('NoDiag')
PNUCHIn['D_Name4'] = PNUCHIn['D_Name4'].astype(str); PNUCHIn['D_Name4'] = PNUCHIn['D_Name4'].str.replace('/',',')
PNUCHIn['D_Code5'] = PNUCHIn['D_Code5'].fillna('NoDiag')
PNUCHIn['D_Code5'] = PNUCHIn['D_Code5'].astype(str); PNUCHIn['D_Code5'] = PNUCHIn['D_Code5'].str.replace('/',',')
PNUCHIn['D_Name5'] = PNUCHIn['D_Name5'].fillna('NoDiag')
PNUCHIn['D_Name5'] = PNUCHIn['D_Name5'].astype(str); PNUCHIn['D_Name5'] = PNUCHIn['D_Name5'].str.replace('/',',')

PNUCHIn.loc[PNUCHIn.D_Code1 != 'NoDiag', 'D_Code1'] = PNUCHIn.D_Code1.str[0:4]
PNUCHIn.loc[PNUCHIn.D_Code2 != 'NoDiag', 'D_Code2'] = PNUCHIn.D_Code2.str[0:4]
PNUCHIn.loc[PNUCHIn.D_Code3 != 'NoDiag', 'D_Code3'] = PNUCHIn.D_Code3.str[0:4]
PNUCHIn.loc[PNUCHIn.D_Code4 != 'NoDiag', 'D_Code4'] = PNUCHIn.D_Code4.str[0:4]
PNUCHIn.loc[PNUCHIn.D_Code5 != 'NoDiag', 'D_Code5'] = PNUCHIn.D_Code5.str[0:4]

PNUCHIn['D_Code'] = PNUCHIn['D_Code1']+'/'+PNUCHIn['D_Code2']+'/'+PNUCHIn['D_Code3']+'/'+PNUCHIn['D_Code4']+'/'+PNUCHIn['D_Code5']
PNUCHIn['D_Name'] = PNUCHIn['D_Name1']+'/'+PNUCHIn['D_Name2']+'/'+PNUCHIn['D_Name3']+'/'+PNUCHIn['D_Name4']+'/'+PNUCHIn['D_Name5']

del PNUCHIn['D_Code1']; del PNUCHIn['D_Code2']; del PNUCHIn['D_Code3']; del PNUCHIn['D_Code4']; del PNUCHIn['D_Code5']
del PNUCHIn['D_Name1']; del PNUCHIn['D_Name2']; del PNUCHIn['D_Name3']; del PNUCHIn['D_Name4']; del PNUCHIn['D_Name5']


# Surgery
PNUCHIn['Sur_Code1'] = PNUCHIn['Sur_Code1'].fillna('NoSur')
PNUCHIn['Sur_Code1'] = PNUCHIn['Sur_Code1'].astype(str); PNUCHIn['Sur_Code1'] = PNUCHIn['Sur_Code1'].str.replace('/',',')
PNUCHIn['Sur_Name1'] = PNUCHIn['Sur_Name1'].fillna('NoSur')
PNUCHIn['Sur_Name1'] = PNUCHIn['Sur_Name1'].astype(str); PNUCHIn['Sur_Name1'] = PNUCHIn['Sur_Name1'].str.replace('/',',')
print(PNUCHIn['Sur_Date1'])
PNUCHIn['Sur_Date1'] = PNUCHIn['Sur_Date1'].fillna('NoSur')
PNUCHIn['Sur_Date1'] = PNUCHIn['Sur_Date1'].astype(str); PNUCHIn['Sur_Date1'] = PNUCHIn['Sur_Date1'].str.replace('.',''); PNUCHIn['Sur_Date1'] = PNUCHIn['Sur_Date1'].astype(str).str[0:8]
print(PNUCHIn['Sur_Date1'])
PNUCHIn['Sur_Code2'] = PNUCHIn['Sur_Code2'].fillna('NoSur')
PNUCHIn['Sur_Code2'] = PNUCHIn['Sur_Code2'].astype(str); PNUCHIn['Sur_Code2'] = PNUCHIn['Sur_Code2'].str.replace('/',',')
PNUCHIn['Sur_Name2'] = PNUCHIn['Sur_Name2'].fillna('NoSur')
PNUCHIn['Sur_Name2'] = PNUCHIn['Sur_Name2'].astype(str); PNUCHIn['Sur_Name2'] = PNUCHIn['Sur_Name2'].str.replace('/',',')
print(PNUCHIn['Sur_Date2'])
PNUCHIn['Sur_Date2'] = PNUCHIn['Sur_Date2'].fillna('NoSur')
PNUCHIn['Sur_Date2'] = PNUCHIn['Sur_Date2'].astype(str); PNUCHIn['Sur_Date2'] = PNUCHIn['Sur_Date2'].str.replace('.',''); PNUCHIn['Sur_Date2'] = PNUCHIn['Sur_Date2'].astype(str).str[0:8]
print(PNUCHIn['Sur_Date2'])

PNUCHIn['Sur_Code'] = PNUCHIn['Sur_Code1']+'/'+PNUCHIn['Sur_Code2']
PNUCHIn['Sur_Name'] = PNUCHIn['Sur_Name1']+'/'+PNUCHIn['Sur_Name2']
PNUCHIn['Sur_Date'] = PNUCHIn['Sur_Date1']+'/'+PNUCHIn['Sur_Date2']

del PNUCHIn['Sur_Code1']; del PNUCHIn['Sur_Code2']
del PNUCHIn['Sur_Name1']; del PNUCHIn['Sur_Name2']
del PNUCHIn['Sur_Date1']; del PNUCHIn['Sur_Date2']
del PNUCHIn['Sur_Dep1']; del PNUCHIn['Sur_Dep2']


# Severity
print(SevPri.columns)
SevPri.rename(columns={SevPri.columns[0] : 'DRGNO01', SevPri.columns[1] : 'Severity01'}, inplace=True)
SevPri['Severity01'].fillna('분류오류', inplace=True)
print(SevPri)
SevPri['DRGNO01'] = SevPri['DRGNO01'].astype(str).str[0:4]
PNUCHIn['DRGNO01'] = PNUCHIn['DRGNO'].astype(str).str[0:4]
PNUCHIn['DRGNO01'].fillna('NoDRG', inplace=True)

print(SevSub.columns)
SevSub.rename(columns={SevSub.columns[0] : 'DRGNO02', SevSub.columns[1] : 'Severity02'}, inplace=True)
print(SevSub)
PNUCHIn['DRGNO02'] = PNUCHIn['DRGNO'].astype(str).str[0:4]
PNUCHIn['DRGNO02'].fillna('NoDRG', inplace=True)

PNUCHIn = PNUCHIn.merge(SevPri, on='DRGNO01', how='left').merge(SevSub, on='DRGNO02', how='left')

PNUCHIn.drop_duplicates(['PT_No', 'Birth', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'In_Date', 'In_Dep', 'Dis_Date', 'Dis_Dep', 'In_Prd', 'DRGNO', 'Pay_InsSelf',
                          'Pay_InsCorp', 'Pay_Sel', 'Pay_NoIns', 'D_Code', 'D_Name', 'Sur_Code', 'Sur_Name', 'Sur_Date', 'DRGNO01', 'DRGNO02', 'Severity01', 'Severity02'], inplace=True)

PNUCHIn.reset_index(drop=True, inplace=True)


PNUCHIn['Severity01'] = PNUCHIn['Severity01'].map({'전문' : 'Severe', '일반' : 'Normal', '단순' : 'Simple', '분류오류' : 'SortError'})
PNUCHIn['Severity02'] = PNUCHIn['Severity02'].map({'전문' : 'Severe', '일반' : 'Normal', '단순' : 'Simple', '분류오류' : 'SortError'})

PNUCHIn['Severity01'] = PNUCHIn['Severity01'].map({'Severe' : 4, 'Normal' : 3, 'Simple' : 2, 'SortError' : 1})
PNUCHIn['Severity01'] = PNUCHIn['Severity01'].fillna(0)
PNUCHIn['Severity02'] = PNUCHIn['Severity02'].map({'Severe' : 4, 'Normal' : 3, 'Simple' : 2, 'SortError' : 1})
PNUCHIn['Severity02'] = PNUCHIn['Severity02'].fillna(0)

PNUCHIn.loc[(PNUCHIn['Severity01']  >= PNUCHIn['Severity02']), 'Severity'] = PNUCHIn['Severity01']
PNUCHIn.loc[(PNUCHIn['Severity01']  < PNUCHIn['Severity02']), 'Severity'] = PNUCHIn['Severity02']

PNUCHIn['Severity01'] = PNUCHIn['Severity01'].map({4 :'Severe', 3 :'Normal', 2 :'Simple', 1 :'SortError', 0 :'NoDRG'})
PNUCHIn['Severity02'] = PNUCHIn['Severity02'].map({4 :'Severe', 3 :'Normal', 2 :'Simple', 1 :'SortError', 0 :'NoDRG'})
PNUCHIn['Severity'] = PNUCHIn['Severity'].map({4 :'Severe', 3 :'Normal', 2 :'Simple', 1 :'SortError', 0 :'NoDRG'})

PNUCHIn.loc[(PNUCHIn['Severity']  == 'NoDRG'), 'DRGNO'] = 'NoDRG'

print(PNUCHIn.columns)
print(PNUCHIn)



## 03. Refine PNUCHIn

PNUCHIn.drop_duplicates(['PT_No', 'In_Date'], inplace=True)
PNUCHIn.reset_index(drop=True, inplace=True)

PNUCHIn['DRGNO'] = PNUCHIn['DRGNO'].fillna('NoDRG')
PNUCHIn['Severity'] = PNUCHIn['Severity'].fillna('NoDRG')

PNUCHIn['Sur_Date'] = PNUCHIn['Sur_Date'].fillna('NoSur')
PNUCHIn['Sur_Code'] = PNUCHIn['Sur_Code'].fillna('NoSur')
PNUCHIn['Sur_Name'] = PNUCHIn['Sur_Name'].fillna('NoSur')


PNUCHIn['Address'] = PNUCHIn['Address'].fillna('NoAdd')
PNUCHIn['Ins_Var'] = PNUCHIn['Ins_Var'].fillna('NoInsVar')
PNUCHIn['Ins_Sub'] = PNUCHIn['Ins_Sub'].fillna('NoVCode')
PNUCHIn['In_Prd'] = PNUCHIn['In_Prd'].fillna(0)
PNUCHIn['Pay_InsSelf'] = PNUCHIn['Pay_InsSelf'].fillna(0)
PNUCHIn['Pay_InsCorp'] = PNUCHIn['Pay_InsCorp'].fillna(0)
PNUCHIn['Pay_NoIns'] = PNUCHIn['Pay_NoIns'].fillna(0)
PNUCHIn['Pay_Sel'] = PNUCHIn['Pay_Sel'].fillna(0)


PNUCHIn.dropna(how='any')
'''
print(len(PNUCHIn))
'''

print(PNUCHIn)


PNUCHIn['PT_No'] = PNUCHIn['PT_No'].astype(str)
PNUCHIn['Birth'] = PNUCHIn['Birth'].astype(str); PNUCHIn['Birth'] = PNUCHIn['Birth'].str.replace('-',''); PNUCHIn['Birth'] = PNUCHIn['Birth'].astype(int)
PNUCHIn['Gender'] = PNUCHIn['Gender'].astype(str)
PNUCHIn['Address'] = PNUCHIn['Address'].astype(str)
PNUCHIn['Ins_Var'] = PNUCHIn['Ins_Var'].astype(str)
PNUCHIn['Ins_Sub'] = PNUCHIn['Ins_Sub'].astype(str)
PNUCHIn['In_Date'] = PNUCHIn['In_Date'].astype(str); PNUCHIn['In_Date'] = PNUCHIn['In_Date'].str.replace('-',''); PNUCHIn['In_Date'] = PNUCHIn['In_Date'].astype(int)
PNUCHIn['In_Dep'] = PNUCHIn['In_Dep'].astype(str)
PNUCHIn['Dis_Date'] = PNUCHIn['Dis_Date'].astype(str); PNUCHIn['Dis_Date'] = PNUCHIn['Dis_Date'].str.replace('-',''); PNUCHIn['Dis_Date'] = PNUCHIn['Dis_Date'].astype(int)
PNUCHIn['Dis_Dep'] = PNUCHIn['Dis_Dep'].astype(str)
PNUCHIn['In_Prd'] = PNUCHIn['In_Prd'].astype(int)
PNUCHIn['Pay_InsSelf'] = PNUCHIn['Pay_InsSelf'].astype(str); PNUCHIn['Pay_InsSelf'] = PNUCHIn['Pay_InsSelf'].str.replace('.',''); PNUCHIn['Pay_InsSelf'] = PNUCHIn['Pay_InsSelf'].astype(str).str[:-1]
PNUCHIn['Pay_InsSelf'] = PNUCHIn['Pay_InsSelf'].astype(int)
PNUCHIn['Pay_InsCorp'] = PNUCHIn['Pay_InsCorp'].astype(str); PNUCHIn['Pay_InsCorp'] = PNUCHIn['Pay_InsCorp'].str.replace('.',''); PNUCHIn['Pay_InsCorp'] = PNUCHIn['Pay_InsCorp'].astype(str).str[:-1]
PNUCHIn['Pay_InsCorp'] = PNUCHIn['Pay_InsCorp'].astype(int)
PNUCHIn['Pay_NoIns'] = PNUCHIn['Pay_NoIns'].astype(str); PNUCHIn['Pay_NoIns'] = PNUCHIn['Pay_NoIns'].str.replace('.',''); PNUCHIn['Pay_NoIns'] = PNUCHIn['Pay_NoIns'].astype(str).str[:-1]
PNUCHIn['Pay_NoIns'] = PNUCHIn['Pay_NoIns'].astype(int)
PNUCHIn['Pay_Sel'] = PNUCHIn['Pay_Sel'].astype(str); PNUCHIn['Pay_Sel'] = PNUCHIn['Pay_Sel'].str.replace('.',''); PNUCHIn['Pay_Sel'] = PNUCHIn['Pay_Sel'].astype(str).str[:-1]
PNUCHIn['Pay_Sel'] = PNUCHIn['Pay_Sel'].astype(int)
PNUCHIn['D_Code'] = PNUCHIn['D_Code'].astype(str)
PNUCHIn['D_Name'] = PNUCHIn['D_Name'].astype(str)
PNUCHIn['DRGNO'] = PNUCHIn['DRGNO'].astype(str)
PNUCHIn['Severity'] = PNUCHIn['Severity'].astype(str)
PNUCHIn['Sur_Date'] = PNUCHIn['Sur_Date'].astype(str)
PNUCHIn['Sur_Code'] = PNUCHIn['Sur_Code'].astype(str)
PNUCHIn['Sur_Name'] = PNUCHIn['Sur_Name'].astype(str)



# 04-1) Change values in 'Gender' Column ( '1' : Male, '2' : Female)
PNUCHIn.Gender.replace(['1.0', '2.0'], ['Male', 'Female'], inplace=True)

# 04-2) Create 'Age' Column
# df.loc[df['column name'] condition, 'new column name'] = 'value if condition is met'
PNUCHIn.loc[(PNUCHIn['In_Date'] % 10000 <= PNUCHIn['Birth'] % 10000) & (PNUCHIn['In_Date'] // 10000 >= PNUCHIn['Birth'] // 10000), 'Age'] = (PNUCHIn['In_Date'] - PNUCHIn['Birth'])// 10000
PNUCHIn.loc[(PNUCHIn['In_Date'] % 10000 > PNUCHIn['Birth'] % 10000) & (PNUCHIn['In_Date'] // 10000 == PNUCHIn['Birth'] // 10000), 'Age'] = (PNUCHIn['In_Date'] - PNUCHIn['Birth'])// 10000
PNUCHIn.loc[(PNUCHIn['In_Date'] % 10000 > PNUCHIn['Birth'] % 10000) & (PNUCHIn['In_Date'] // 10000 > PNUCHIn['Birth'] // 10000), 'Age'] = (PNUCHIn['In_Date'] - PNUCHIn['Birth'])// 10000 - 1


PNUCHIn['Age'] = PNUCHIn['Age'].astype(int)


print(PNUCHIn.columns)
print(PNUCHIn['Age'])



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

PNUCHIn['Address'] = PNUCHIn['Address'].apply(lambda x: address_map[x])


PNUCHIn['Address'] = PNUCHIn['Address'].fillna('NoAdd')




# 04-4) Rephrase 'Ins_Var' Values
PNUCHIn['Ins_Var'] = PNUCHIn['Ins_Var'].map({'1.0' : 'NHIS', '2.0' : 'MedCareT1', '3.0' : 'MedCareT2', '4.0' : 'MedCareDis'})
PNUCHIn['Ins_Var'] = PNUCHIn['Ins_Var'].fillna('Others')


# 04-5) Refine 'Ins_Sub' Values
PNUCHIn.loc[(PNUCHIn.Ins_Sub.str.contains('F')) & (~PNUCHIn.Ins_Sub.str.contains(',')), 'Ins_Sub'] = 'NoVCode'
PNUCHIn.Ins_Sub = PNUCHIn.Ins_Sub.str.split(',')
PNUCHIn = PNUCHIn.apply(pd.Series.explode).reset_index(drop=True)

PNUCHIn= PNUCHIn[~PNUCHIn.Ins_Sub.str.contains('F')]

# 04-6) Change Order of Columns
PNUCHIn['InstName'] = np.nan
PNUCHIn['InstName'] = PNUCHIn['InstName'].fillna('PNUCH')
PNUCHIn = PNUCHIn[['InstName', 'PT_No', 'Birth', 'Age', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'In_Date', 'In_Dep', 'Dis_Date', 'Dis_Dep', 'In_Prd', 'Pay_InsSelf',
                     'Pay_InsCorp', 'Pay_NoIns', 'Pay_Sel', 'D_Code', 'D_Name', 'DRGNO', 'Severity', 'Sur_Date', 'Sur_Code', 'Sur_Name']]


print(PNUCHIn.columns)
with pd.option_context('display.max_columns', None):
    print(PNUCHIn)



### Save as CSV in '/Users/peterpark/Desktop/PY_START/SNUH_Project01'

PNUCHIn.to_csv('./PNUCH/PNUCHInP_R4A.csv', index=False)

