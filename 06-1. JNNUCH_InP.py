'''
This script is written 4 preprocessing JNNUCH's Inpatients' Data.

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
JNNUCHIn = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018JNNUCHIn_Prep.csv", encoding="utf-8", low_memory=False)
SevPri = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018-2019SevCri_5digitPri.csv", encoding="utf-8", low_memory=False)
SevSub = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018-2019SevCri_4digitSub.csv", encoding="utf-8", low_memory=False)


### Preprocessing


## 01. Renaming Columns
JNNUCHIn.reset_index(drop=True, inplace=True)
print(JNNUCHIn.columns)

'''
['신설ID', '나이', '성별', '주소', '보험\n급종', '산정특례 \n특정기호', '입원일', '입원\n진료과',
       '퇴원일', '퇴원\n진료과', '재원\n일수', 'KDRG번호', '진단코드', '진단명', '수술일', '수술코드',
       '수술명', '소속\n진료과', '일부본인부담\n본인부담금', '일부본인부담\n보험자부담금', '전액\n본인부담', '비급여']
'''

'''
result: Index(['InstName', 'PT_No', 'Age', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'In_Date', 'In_Dep', 'Dis_Date', 'Dis_Dep', 'In_Prd', 'Pay_InsSelf',
                   'Pay_InsCorp', 'Pay_NoIns', 'Pay_Sel', 'D_Code', 'D_Name', 'DRGNO', 'Severity', 'Sur_Date', 'Sur_Code', 'Sur_Name'])
'''

JNNUCHIn.rename(columns={JNNUCHIn.columns[0] : 'PT_No', JNNUCHIn.columns[1] : 'Age', JNNUCHIn.columns[2] : 'Gender', JNNUCHIn.columns[3] : 'Address', JNNUCHIn.columns[4] : 'Ins_Var',
                         JNNUCHIn.columns[5] : 'Ins_Sub', JNNUCHIn.columns[6] : 'In_Date', JNNUCHIn.columns[7] : 'In_Dep', JNNUCHIn.columns[8] : 'Dis_Date', JNNUCHIn.columns[9] : 'Dis_Dep',
                         JNNUCHIn.columns[10] : 'In_Prd', JNNUCHIn.columns[11] : 'DRGNO', JNNUCHIn.columns[12] : 'D_Code', JNNUCHIn.columns[13] : 'D_Name',
                         JNNUCHIn.columns[14] : 'Sur_Date', JNNUCHIn.columns[15] : 'Sur_Code', JNNUCHIn.columns[16] : 'Sur_Name', JNNUCHIn.columns[17] : 'Sur_Dep',
                         JNNUCHIn.columns[18] : 'Pay_InsSelf', JNNUCHIn.columns[19] : 'Pay_InsCorp', JNNUCHIn.columns[20] : 'Pay_Sel', JNNUCHIn.columns[21] : 'Pay_NoIns'}, inplace=True)


'''
print(JNNUCHIn.columns)
'''
JNNUCHIn.dropna(how='all', axis=0, inplace=True)
print(JNNUCHIn)

## 02. Deleting Unnecessary Columns
'''
print(JNNUCHIn.loc[0:5])
'''

# Diagnosis
JNNUCHIn['D_Code'] = JNNUCHIn['D_Code'].fillna('NoDiag')
JNNUCHIn['D_Code'] = JNNUCHIn['D_Code'].astype(str); JNNUCHIn['D_Code'] = JNNUCHIn['D_Code'].str.replace('/',','); JNNUCHIn['D_Code'] = JNNUCHIn['D_Code'].str.replace('.','').str[0:4]
JNNUCHIn['D_Name'] = JNNUCHIn['D_Name'].fillna('NoDiag')
JNNUCHIn['D_Name'] = JNNUCHIn['D_Name'].astype(str); JNNUCHIn['D_Name'] = JNNUCHIn['D_Name'].str.replace('/',',')


# Surgery
JNNUCHIn['Sur_Code'] = JNNUCHIn['Sur_Code'].fillna('NoSur')
JNNUCHIn['Sur_Code'] = JNNUCHIn['Sur_Code'].astype(str); JNNUCHIn['Sur_Code'] = JNNUCHIn['Sur_Code'].str.replace('/',',')
JNNUCHIn['Sur_Name'] = JNNUCHIn['Sur_Name'].fillna('NoSur')
JNNUCHIn['Sur_Name'] = JNNUCHIn['Sur_Name'].astype(str); JNNUCHIn['Sur_Name'] = JNNUCHIn['Sur_Name'].str.replace('/',',')
print(JNNUCHIn['Sur_Date'])
JNNUCHIn['Sur_Date'] = JNNUCHIn['Sur_Date'].fillna('NoSur')
JNNUCHIn['Sur_Date'] = JNNUCHIn['Sur_Date'].astype(str); JNNUCHIn['Sur_Date'] = JNNUCHIn['Sur_Date'].str.replace('-',''); JNNUCHIn['Sur_Date'] = JNNUCHIn['Sur_Date'].astype(str)
print(JNNUCHIn['Sur_Date'])


# Severity
print(SevPri.columns)
SevPri.rename(columns={SevPri.columns[0] : 'DRGNO01', SevPri.columns[1] : 'Severity01'}, inplace=True)
SevPri['Severity01'].fillna('분류오류', inplace=True)
print(SevPri)
JNNUCHIn['DRGNO01'] = JNNUCHIn['DRGNO'].astype(str).str[0:5]
JNNUCHIn['DRGNO01'].fillna('NoDRG', inplace=True)

print(SevSub.columns)
SevSub.rename(columns={SevSub.columns[0] : 'DRGNO02', SevSub.columns[1] : 'Severity02'}, inplace=True)
print(SevSub)
JNNUCHIn['DRGNO02'] = JNNUCHIn['DRGNO'].astype(str).str[0:4]
JNNUCHIn['DRGNO02'].fillna('NoDRG', inplace=True)

JNNUCHIn = JNNUCHIn.merge(SevPri, on='DRGNO01', how='left').merge(SevSub, on='DRGNO02', how='left')

JNNUCHIn.drop_duplicates(['PT_No', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'In_Date', 'In_Dep', 'Dis_Date', 'Dis_Dep', 'In_Prd', 'DRGNO', 'Pay_InsSelf',
                          'Pay_InsCorp', 'Pay_Sel', 'Pay_NoIns', 'D_Code', 'D_Name', 'Sur_Code', 'Sur_Name', 'Sur_Date', 'DRGNO01', 'DRGNO02', 'Severity01', 'Severity02'], inplace=True)

JNNUCHIn.reset_index(drop=True, inplace=True)


JNNUCHIn['Severity01'] = JNNUCHIn['Severity01'].map({'전문' : 'Severe', '일반' : 'Normal', '단순' : 'Simple', '분류오류' : 'SortError'})
JNNUCHIn['Severity02'] = JNNUCHIn['Severity02'].map({'전문' : 'Severe', '일반' : 'Normal', '단순' : 'Simple', '분류오류' : 'SortError'})

JNNUCHIn['Severity01'] = JNNUCHIn['Severity01'].map({'Severe' : 4, 'Normal' : 3, 'Simple' : 2, 'SortError' : 1})
JNNUCHIn['Severity01'] = JNNUCHIn['Severity01'].fillna(0)
JNNUCHIn['Severity02'] = JNNUCHIn['Severity02'].map({'Severe' : 4, 'Normal' : 3, 'Simple' : 2, 'SortError' : 1})
JNNUCHIn['Severity02'] = JNNUCHIn['Severity02'].fillna(0)

JNNUCHIn.loc[(JNNUCHIn['Severity01']  >= JNNUCHIn['Severity02']), 'Severity'] = JNNUCHIn['Severity01']
JNNUCHIn.loc[(JNNUCHIn['Severity01']  < JNNUCHIn['Severity02']), 'Severity'] = JNNUCHIn['Severity02']

JNNUCHIn['Severity01'] = JNNUCHIn['Severity01'].map({4 :'Severe', 3 :'Normal', 2 :'Simple', 1 :'SortError', 0 :'NoDRG'})
JNNUCHIn['Severity02'] = JNNUCHIn['Severity02'].map({4 :'Severe', 3 :'Normal', 2 :'Simple', 1 :'SortError', 0 :'NoDRG'})
JNNUCHIn['Severity'] = JNNUCHIn['Severity'].map({4 :'Severe', 3 :'Normal', 2 :'Simple', 1 :'SortError', 0 :'NoDRG'})

JNNUCHIn.loc[(JNNUCHIn['Severity']  == 'NoDRG'), 'DRGNO'] = 'NoDRG'

print(JNNUCHIn.columns)
print(JNNUCHIn)



## 03. Refine JNNUCHIn

JNNUCHIn['DRGNO'] = JNNUCHIn['DRGNO'].fillna('NoDRG')
JNNUCHIn['Severity'] = JNNUCHIn['Severity'].fillna('NoDRG')

JNNUCHIn['Sur_Date'] = JNNUCHIn['Sur_Date'].fillna('NoSur')
JNNUCHIn['Sur_Code'] = JNNUCHIn['Sur_Code'].fillna('NoSur')
JNNUCHIn['Sur_Name'] = JNNUCHIn['Sur_Name'].fillna('NoSur')


JNNUCHIn['Address'] = JNNUCHIn['Address'].fillna('NoAdd')
JNNUCHIn['Ins_Var'] = JNNUCHIn['Ins_Var'].fillna('NoInsVar')
JNNUCHIn['Ins_Sub'] = JNNUCHIn['Ins_Sub'].fillna('NoVCode'); JNNUCHIn['Ins_Sub'] = JNNUCHIn['Ins_Sub'].replace('-', 'NoVCode')
JNNUCHIn['In_Prd'] = JNNUCHIn['In_Prd'].fillna(0)
JNNUCHIn['Pay_InsSelf'] = JNNUCHIn['Pay_InsSelf'].fillna(0)
JNNUCHIn['Pay_InsCorp'] = JNNUCHIn['Pay_InsCorp'].fillna(0)
JNNUCHIn['Pay_NoIns'] = JNNUCHIn['Pay_NoIns'].fillna(0)
JNNUCHIn['Pay_Sel'] = JNNUCHIn['Pay_Sel'].fillna(0)


JNNUCHIn.dropna(how='any')
'''
print(len(JNNUCHIn))
'''

JNNUCHIn['PT_No'] = JNNUCHIn['PT_No'].astype(str)
JNNUCHIn['Age'] = JNNUCHIn['Age'].astype(int)
JNNUCHIn['Gender'] = JNNUCHIn['Gender'].astype(int)
JNNUCHIn['Address'] = JNNUCHIn['Address'].astype(str)
JNNUCHIn['Ins_Var'] = JNNUCHIn['Ins_Var'].astype(int)
JNNUCHIn['Ins_Sub'] = JNNUCHIn['Ins_Sub'].astype(str)
JNNUCHIn['In_Date'] = JNNUCHIn['In_Date'].astype(str); JNNUCHIn['In_Date'] = JNNUCHIn['In_Date'].str.replace('-',''); JNNUCHIn['In_Date'] = JNNUCHIn['In_Date'].str.replace(' ','del')
JNNUCHIn = JNNUCHIn[~JNNUCHIn.In_Date.str.contains('del')]
JNNUCHIn['In_Date'] = JNNUCHIn['In_Date'].astype(int)
JNNUCHIn['In_Dep'] = JNNUCHIn['In_Dep'].astype(str)
JNNUCHIn['Dis_Date'] = JNNUCHIn['Dis_Date'].astype(str); JNNUCHIn['Dis_Date'] = JNNUCHIn['Dis_Date'].str.replace('-','')
JNNUCHIn['Dis_Date'] = JNNUCHIn['Dis_Date'].astype(int)
JNNUCHIn['Dis_Dep'] = JNNUCHIn['Dis_Dep'].astype(str)
JNNUCHIn['In_Prd'] = JNNUCHIn['In_Prd'].astype(int)
JNNUCHIn['Pay_InsSelf'] = JNNUCHIn['Pay_InsSelf'].astype(str); JNNUCHIn['Pay_InsSelf'] = JNNUCHIn['Pay_InsSelf'].str.replace(',',''); JNNUCHIn['Pay_InsSelf'] = JNNUCHIn['Pay_InsSelf'].str.replace(' ','')
JNNUCHIn['Pay_InsSelf'] = JNNUCHIn['Pay_InsSelf'].astype(int)
JNNUCHIn['Pay_InsCorp'] = JNNUCHIn['Pay_InsCorp'].astype(str); JNNUCHIn['Pay_InsCorp'] = JNNUCHIn['Pay_InsCorp'].str.replace(',',''); JNNUCHIn['Pay_InsCorp'] = JNNUCHIn['Pay_InsCorp'].str.replace(' ','')
JNNUCHIn['Pay_InsCorp'] = JNNUCHIn['Pay_InsCorp'].astype(int)
JNNUCHIn['Pay_NoIns'] = JNNUCHIn['Pay_NoIns'].astype(str); JNNUCHIn['Pay_NoIns'] = JNNUCHIn['Pay_NoIns'].str.replace(',',''); JNNUCHIn['Pay_NoIns'] = JNNUCHIn['Pay_NoIns'].str.replace(' ','')
JNNUCHIn['Pay_NoIns'] = JNNUCHIn['Pay_NoIns'].astype(int)
JNNUCHIn['Pay_Sel'] = JNNUCHIn['Pay_Sel'].astype(str); JNNUCHIn['Pay_Sel'] = JNNUCHIn['Pay_Sel'].str.replace(',',''); JNNUCHIn['Pay_Sel'] = JNNUCHIn['Pay_Sel'].str.replace(' ','')
JNNUCHIn['Pay_Sel'] = JNNUCHIn['Pay_Sel'].astype(int)
JNNUCHIn['D_Code'] = JNNUCHIn['D_Code'].astype(str)
JNNUCHIn['D_Name'] = JNNUCHIn['D_Name'].astype(str)
JNNUCHIn['DRGNO'] = JNNUCHIn['DRGNO'].astype(str)
JNNUCHIn['Severity'] = JNNUCHIn['Severity'].astype(str)
JNNUCHIn['Sur_Date'] = JNNUCHIn['Sur_Date'].astype(str)
JNNUCHIn['Sur_Code'] = JNNUCHIn['Sur_Code'].astype(str)
JNNUCHIn['Sur_Name'] = JNNUCHIn['Sur_Name'].astype(str)

print(JNNUCHIn)

# 04-1) Change values in 'Gender' Column ( '1' : Male, '2' : Female)
JNNUCHIn.Gender.replace([1, 2], ['Male', 'Female'], inplace=True)


# 04-2) Rephrase 'Address' Values
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

JNNUCHIn['Address'] = JNNUCHIn['Address'].apply(lambda x: address_map[x])


JNNUCHIn['Address'] = JNNUCHIn['Address'].fillna('NoAdd')




# 04-3) Rephrase 'Ins_Var' Values

JNNUCHIn['Ins_Var'] = JNNUCHIn['Ins_Var'].map({1 : 'NHIS', 2 : 'MedCareT1', 3 : 'MedCareT2', 4 : 'MedCareDis'})
JNNUCHIn['Ins_Var'] = JNNUCHIn['Ins_Var'].fillna('Others')


# 07-4) Change Order of Columns

JNNUCHIn = JNNUCHIn.groupby(by=['PT_No', 'In_Date'], as_index=False).agg({'Age' : pd.Series.unique,
                                                                          'Gender' : pd.Series.unique,
                                                                          'Address' : pd.Series.unique,
                                                                          'Ins_Var' : pd.Series.unique,
                                                                          'Ins_Sub' : lambda a: '/'.join(pd.unique(a)),
                                                                          'In_Dep' : pd.Series.unique,
                                                                          'Dis_Date' : pd.Series.unique,
                                                                          'Dis_Dep' : pd.Series.unique,
                                                                          'In_Prd' : pd.Series.unique,
                                                                          'Pay_InsSelf' : pd.Series.unique,
                                                                          'Pay_InsCorp' : pd.Series.unique,
                                                                          'Pay_NoIns' : pd.Series.unique,
                                                                          'Pay_Sel' : pd.Series.unique,
                                                                          'D_Code' : lambda a: '/'.join(pd.unique(a)),
                                                                          'D_Name' : lambda a: '/'.join(pd.unique(a)),
                                                                          'DRGNO' : pd.Series.unique,
                                                                          'Severity' : pd.Series.unique,
                                                                          'Sur_Date' : lambda a: '/'.join(pd.unique(a)),
                                                                          'Sur_Code' : lambda a: '/'.join(pd.unique(a)),
                                                                          'Sur_Name' : lambda a: '/'.join(pd.unique(a))})

JNNUCHIn.loc[JNNUCHIn.Sur_Code == 'NoSur', 'Sur_Date'] = 'NoSur'
JNNUCHIn.loc[JNNUCHIn.Sur_Code == 'NoSur', 'Sur_Name'] = 'NoSur'


JNNUCHIn.drop_duplicates(['PT_No', 'In_Date'], inplace=True)
JNNUCHIn.reset_index(drop=True, inplace=True)

JNNUCHIn['InstName'] = np.nan
JNNUCHIn['InstName'] = JNNUCHIn['InstName'].fillna('JNNUCH')
JNNUCHIn = JNNUCHIn[['InstName', 'PT_No', 'Age', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'In_Date', 'In_Dep', 'Dis_Date', 'Dis_Dep', 'In_Prd', 'Pay_InsSelf',
                     'Pay_InsCorp', 'Pay_NoIns', 'Pay_Sel', 'D_Code', 'D_Name', 'DRGNO', 'Severity', 'Sur_Date', 'Sur_Code', 'Sur_Name']]


print(JNNUCHIn.columns)
with pd.option_context('display.max_columns', None):
    print(JNNUCHIn)



### Save as CSV in '/Users/peterpark/Desktop/PY_START/SNUH_Project01'

JNNUCHIn.to_csv('./JNNUCH/JNNUCHInP_R4A.csv', index=False)

