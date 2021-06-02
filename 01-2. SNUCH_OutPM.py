'''
This script is written 4 preprocessing SNUCH's Outpatients' Data.

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

SNUCHOut= pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018SNUCHOut_Prep.csv", low_memory=False, encoding="utf-8")
OutV = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018SNUCHOutRare_Prep.csv", encoding="utf-8", low_memory=False)



OutV.rename(columns={OutV.columns[0] : "PACT_ID", OutV.columns[1] : "PT_No", OutV.columns[2] : "Diag_ID", OutV.columns[3] : "SevCode", OutV.columns[4] : "VCode", OutV.columns[5] : "VCodeSub"}, inplace=True)

SNUCHOut.rename(columns={SNUCHOut.columns[0] : 'PT_No', SNUCHOut.columns[8] : 'D_Code', SNUCHOut.columns[9] : 'D_Name', SNUCHOut.columns[10] : 'D_Code2', SNUCHOut.columns[11] : 'D_Name2', SNUCHOut.columns[12] : 'D_Code3', SNUCHOut.columns[13] : 'D_Name3',
                         SNUCHOut.columns[14] : 'D_Code4', SNUCHOut.columns[15] : 'D_Name4', SNUCHOut.columns[16] : 'D_Code5', SNUCHOut.columns[17] : 'D_Name5', SNUCHOut.columns[18] : 'D_Code6', SNUCHOut.columns[19] : 'D_Name6'}, inplace=True)


## 02. Deleting Unnecessary Columns

print(SNUCHOut.loc[0:5])


SNUCHOut['D_Code'] = SNUCHOut['D_Code'].fillna('NoDiag')
SNUCHOut['D_Code'] = SNUCHOut['D_Code'].astype(str); SNUCHOut['D_Code'] = SNUCHOut['D_Code'].str.replace('/',',')
SNUCHOut['D_Name'] = SNUCHOut['D_Name'].fillna('NoDiag')
SNUCHOut['D_Name'] = SNUCHOut['D_Name'].astype(str); SNUCHOut['D_Name'] = SNUCHOut['D_Name'].str.replace('/',',')


SNUCHOut.loc[SNUCHOut.D_Code != 'NoDiag', 'D_Code'] = SNUCHOut.D_Code.str[0:4]

del SNUCHOut['D_Code2']; del SNUCHOut['D_Code3']; del SNUCHOut['D_Code4']; del SNUCHOut['D_Code5']; del SNUCHOut['D_Code6']
del SNUCHOut['D_Name2']; del SNUCHOut['D_Name3']; del SNUCHOut['D_Name4']; del SNUCHOut['D_Name5']; del SNUCHOut['D_Name6']
del SNUCHOut['Ins_Sub']


## 03. Combine duplicated from InR(Criteria: ['PT_No', 'In_Date'])
print(OutV)

print(OutV.columns)

OutV.fillna('', inplace=True)
OutV.loc[(OutV.SevCode == 'V193'), 'VCode'] = 'V193'
OutV.loc[(OutV.VCode == OutV.VCodeSub), 'VCodeSub'] = ''
OutV.loc[(OutV.SevCode == 'V193'), 'SevCode'] = ''
OutV.loc[(OutV.VCode == ''), 'VCode'] = OutV.VCodeSub

del OutV['PACT_ID']; del OutV['Diag_ID']; del OutV['SevCode']; del OutV['VCodeSub']

OutV.drop_duplicates(['PT_No','VCode'], inplace=True)
OutV = OutV.groupby('PT_No').agg({'VCode': lambda a: '/'.join(a)}).reset_index()

OutV.rename(columns={'VCode':'Ins_Sub'}, inplace=True)

print(OutV)

SNUCHOut['PT_No'] = SNUCHOut['PT_No'].astype(str)
OutV['PT_No'] = OutV['PT_No'].astype(str)
SNUCHOut = pd.merge(SNUCHOut, OutV, how='left', on='PT_No')

# 04. Refining
SNUCHOut.drop_duplicates(['PT_No', 'Out_Date'], inplace=True)

SNUCHOut['Address'] = SNUCHOut['Address'].fillna('NoAdd')
SNUCHOut['Ins_Var'] = SNUCHOut['Ins_Var'].fillna('NoInsVar')
SNUCHOut['Ins_Sub'] = SNUCHOut['Ins_Sub'].fillna('NoVCode')

SNUCHOut.dropna(how='any')


SNUCHOut['PT_No'] = SNUCHOut['PT_No'].astype(str)
SNUCHOut['Birth'] = SNUCHOut['Birth'].astype(str); SNUCHOut['Birth'] = SNUCHOut['Birth'].str.replace('-',''); SNUCHOut['Birth'] = SNUCHOut['Birth'].astype(int)
SNUCHOut['Gender'] = SNUCHOut['Gender'].astype(str)
SNUCHOut['Address'] = SNUCHOut['Address'].astype(str)
SNUCHOut['Ins_Var'] = SNUCHOut['Ins_Var'].astype(str)
SNUCHOut['Ins_Sub'] = SNUCHOut['Ins_Sub'].astype(str)
SNUCHOut['Out_Date'] = SNUCHOut['Out_Date'].astype(str); SNUCHOut['Out_Date'] = SNUCHOut['Out_Date'].str.replace('-',''); SNUCHOut['Out_Date'] = SNUCHOut['Out_Date'].astype(int)
SNUCHOut['Out_Dep'] = SNUCHOut['Out_Dep'].astype(str)
SNUCHOut['D_Code'] = SNUCHOut['D_Code'].astype(str)
SNUCHOut['D_Name'] = SNUCHOut['D_Name'].astype(str)


# 07-1) Change values in 'Gender' Column ( '1' : Male, '2' : Female)
print(SNUCHOut.Gender.unique())
SNUCHOut.Gender.replace(['남성', '여성'], ['Male', 'Female'], inplace=True)

# 07-2) Create 'Age' Column
# df.loc[df['column name'] condition, 'new column name'] = 'value if condition is met'
SNUCHOut.loc[(SNUCHOut['Out_Date'] % 10000 <= SNUCHOut['Birth'] % 10000) & (SNUCHOut['Out_Date'] // 10000 >= SNUCHOut['Birth'] // 10000), 'Age'] = (SNUCHOut['Out_Date'] - SNUCHOut['Birth'])// 10000
SNUCHOut.loc[(SNUCHOut['Out_Date'] % 10000 > SNUCHOut['Birth'] % 10000) & (SNUCHOut['Out_Date'] // 10000 == SNUCHOut['Birth'] // 10000), 'Age'] = (SNUCHOut['Out_Date'] - SNUCHOut['Birth'])// 10000
SNUCHOut.loc[(SNUCHOut['Out_Date'] % 10000 > SNUCHOut['Birth'] % 10000) & (SNUCHOut['Out_Date'] // 10000 > SNUCHOut['Birth'] // 10000), 'Age'] = (SNUCHOut['Out_Date'] - SNUCHOut['Birth'])// 10000 - 1


SNUCHOut['Age'] = SNUCHOut['Age'].astype(int)


print(SNUCHOut.columns)
print(SNUCHOut['Age'])



# 07-3) Rephrase 'Address' Values
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

SNUCHOut['Address'] = SNUCHOut['Address'].apply(lambda x: address_map[x])


SNUCHOut['Address'] = SNUCHOut['Address'].fillna('NoAdd')

'''
print(SNUCHOut['Address'].value_counts())
print(SNUCHOut['Address'])
'''


# 07-4) Rephrase 'Ins_Var' Values
'''
print(SNUCHOut['Ins_Var'].value_counts())
'''

SNUCHOut['Ins_Var'] = SNUCHOut['Ins_Var'].map({'국민건강보험공단' : 'NHIS', '의료급여1종' : 'MedCareT1', '의료급여2종' : 'MedCareT2', '의료급여장애인' : 'MedCareDis'})
SNUCHOut['Ins_Var'] = SNUCHOut['Ins_Var'].fillna('Others')


'''
print(SNUCHOut['Ins_Var'].value_counts())
print(SNUCHOut['Ins_Var'])
'''



# 07-6) Change Order of Columns


SNUCHOut['InstName'] = np.nan
SNUCHOut['InstName'] = SNUCHOut['InstName'].fillna('SNUCH')
#print(SNUCHOut.columns)

SNUCHOut = SNUCHOut[['InstName', 'PT_No', 'Age', 'Birth', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'Out_Date', 'Out_Dep', 'D_Code', 'D_Name']]


SNUCHOut.to_csv('./SNUCH/SNUCHOutPMain_R4A.csv', index=False)


