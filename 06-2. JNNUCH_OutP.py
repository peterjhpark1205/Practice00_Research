'''
This script is written 4 preprocessing JNNUCH's Outpatients' Data.

Written Date: 2019.12.19
Written By: Peter JH Park

'''

### Import modules in needs

import os, sys, csv
import pandas as pd
import numpy as np
import datetime, time

print("\n Current Working Directory is: ", os.getcwd())

### READ Files & Check

JNNUCHOut= pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018JNNUCHOut_Prep.csv", low_memory=False, encoding="utf-8")



print(JNNUCHOut.columns)
'''
['신설ID', '나이', '성별', '주소', '진료일', '진료과', '진단코드', '진단명', '보험급종', '산정특례 특정기호']
'''

'''
['InstName', 'PT_No', 'Age', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'Out_Date', 'Out_Dep', 'D_Code', 'D_Name']
'''

JNNUCHOut.rename(columns={JNNUCHOut.columns[0] : 'PT_No', JNNUCHOut.columns[1] : 'Age', JNNUCHOut.columns[2] : 'Gender', JNNUCHOut.columns[3] : 'Address',
                          JNNUCHOut.columns[4] : 'Out_Date', JNNUCHOut.columns[5] : 'Out_Dep', JNNUCHOut.columns[6] : 'D_Code', JNNUCHOut.columns[7] : 'D_Name',
                          JNNUCHOut.columns[8] : 'Ins_Var', JNNUCHOut.columns[9] : 'Ins_Sub'}, inplace=True)


## 02. Deleting Unnecessary Columns

print(JNNUCHOut.loc[0:5])


JNNUCHOut['D_Code'] = JNNUCHOut['D_Code'].fillna('NoDiag')
JNNUCHOut['D_Code'] = JNNUCHOut['D_Code'].astype(str); JNNUCHOut['D_Code'] = JNNUCHOut['D_Code'].str.replace('/',','); JNNUCHOut['D_Code'] = JNNUCHOut['D_Code'].str.replace('.','').str[0:4]
JNNUCHOut['D_Name'] = JNNUCHOut['D_Name'].fillna('NoDiag')
JNNUCHOut['D_Name'] = JNNUCHOut['D_Name'].astype(str); JNNUCHOut['D_Name'] = JNNUCHOut['D_Name'].str.replace('/',',')


JNNUCHOut['Address'] = JNNUCHOut['Address'].fillna('NoAdd')
JNNUCHOut['Ins_Var'] = JNNUCHOut['Ins_Var'].fillna('NoInsVar')
JNNUCHOut['Ins_Sub'] = JNNUCHOut['Ins_Sub'].fillna('NoVCode')
JNNUCHOut['Ins_Sub'] = JNNUCHOut['Ins_Sub'].str.replace('-', 'NoVCode')

JNNUCHOut.dropna(how='any')


JNNUCHOut['PT_No'] = JNNUCHOut['PT_No'].astype(str)
JNNUCHOut['Age'] = JNNUCHOut['Age'].astype(int)
JNNUCHOut['Gender'] = JNNUCHOut['Gender'].astype(int)
JNNUCHOut['Address'] = JNNUCHOut['Address'].astype(str)
JNNUCHOut['Ins_Var'] = JNNUCHOut['Ins_Var'].astype(str)
JNNUCHOut['Ins_Sub'] = JNNUCHOut['Ins_Sub'].astype(str)
JNNUCHOut['Out_Date'] = JNNUCHOut['Out_Date'].astype(str); JNNUCHOut['Out_Date'] = JNNUCHOut['Out_Date'].str.replace('-',''); JNNUCHOut['Out_Date'] = JNNUCHOut['Out_Date'].astype(int)
JNNUCHOut['Out_Dep'] = JNNUCHOut['Out_Dep'].astype(str)
JNNUCHOut['D_Code'] = JNNUCHOut['D_Code'].astype(str)
JNNUCHOut['D_Name'] = JNNUCHOut['D_Name'].astype(str)


# 03-1) Change values in 'Gender' Column ( '1' : Male, '2' : Female)
print(JNNUCHOut.Gender.unique())
JNNUCHOut.Gender.replace([1, 2], ['Male', 'Female'], inplace=True)


# 03-2) Rephrase 'Address' Values
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

JNNUCHOut['Address'] = JNNUCHOut['Address'].apply(lambda x: address_map[x])


JNNUCHOut['Address'] = JNNUCHOut['Address'].fillna('NoAdd')

'''
print(JNNUCHOut['Address'].value_counts())
print(JNNUCHOut['Address'])
'''


# 03-3) Rephrase 'Ins_Var' Values
'''
print(JNNUCHOut['Ins_Var'].value_counts())
'''

JNNUCHOut['Ins_Var'] = JNNUCHOut['Ins_Var'].map({'1' : 'NHIS', '2' : 'MedCareT1', '3' : 'MedCareT2', '4' : 'MedCareDis'})
JNNUCHOut['Ins_Var'] = JNNUCHOut['Ins_Var'].fillna('Others')


'''
print(JNNUCHOut['Ins_Var'].value_counts())
print(JNNUCHOut['Ins_Var'])
'''


# 03-4) Rephrase 'Ins_Sub' Values (Not Executed)
'''
print(JNNUCHOut['Ins_Sub'].value_counts())
'''

JNNUCHOut['Ins_Sub'] = JNNUCHOut.Ins_Sub.str.split(" ", expand=True)[0]

'''
print(JNNUCHOut['Ins_Sub'].value_counts())
print(JNNUCHOut['Ins_Sub'])
'''


# 03-5) Group by 'PT_No' & 'Out_Date'
JNNUCHOut = JNNUCHOut.groupby(by=['PT_No', 'Out_Date'], as_index=False).agg({'Age' : pd.Series.unique,
                                                                             'Gender' : pd.Series.unique,
                                                                             'Address' : pd.Series.unique,
                                                                             'Ins_Var' : pd.Series.unique,
                                                                             'Ins_Sub' : lambda a: '/'.join(pd.unique(a)),
                                                                             'Out_Dep' : pd.Series.unique,
                                                                             'D_Code' : lambda a: '/'.join(pd.unique(a)),
                                                                             'D_Name' : lambda a: '/'.join(pd.unique(a))})



# 03-6) Change Order of Columns
JNNUCHOut.drop_duplicates(['PT_No', 'Out_Date'], inplace=True)
JNNUCHOut.reset_index(drop=True, inplace=True)

JNNUCHOut['InstName'] = np.nan
JNNUCHOut['InstName'] = JNNUCHOut['InstName'].fillna('JNNUCH')
#print(JNNUCHOut.columns)

JNNUCHOut = JNNUCHOut[['InstName', 'PT_No', 'Age', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'Out_Date', 'Out_Dep', 'D_Code', 'D_Name']]


print(JNNUCHOut.columns)
with pd.option_context('display.max_columns', None):
    print(JNNUCHOut)


### Save as CSV in '/Users/peterpark/Desktop/PY_START/SNUH_Project01'


JNNUCHOut.to_csv('./JNNUCH/JNNUCHOutP_R4A.csv', index=False)


