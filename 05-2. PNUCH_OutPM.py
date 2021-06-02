'''
This script is written 4 preprocessing PNUCH's Outpatients' Data.

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

PNUCHOut= pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018PNUCHOut_Prep.csv", low_memory=False, encoding="utf-8")



print(PNUCHOut.columns)
'''
['환자ID', '생년월일', '성별', '주소', '외래진료일1', '외래진료과1', '주진단코드', '주진단코드명',
       '부진단코드', '부진단코드명', '부진단코드2', '부진단코드명2', '부진단코드3', '부진단코드명3', '부진단코드4',
       '부진단코드명4', '부진단코드5', '부진단코드명5', '보험급종', '산정특례 특정기호']
'''

'''
['InstName', 'PT_No', 'Age', 'Birth', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'Out_Date', 'Out_Dep', 'D_Code', 'D_Name']
'''

PNUCHOut.rename(columns={PNUCHOut.columns[0] : 'PT_No', PNUCHOut.columns[1] : 'Birth', PNUCHOut.columns[2] : 'Gender', PNUCHOut.columns[3] : 'Address',
                          PNUCHOut.columns[4] : 'Out_Date', PNUCHOut.columns[5] : 'Out_Dep', PNUCHOut.columns[6] : 'D_Code', PNUCHOut.columns[7] : 'D_Name',
                          PNUCHOut.columns[8] : 'D_Code2', PNUCHOut.columns[9] : 'D_Name2', PNUCHOut.columns[10] : 'D_Code3', PNUCHOut.columns[11] : 'D_Name3',
                          PNUCHOut.columns[12] : 'D_Code4', PNUCHOut.columns[13] : 'D_Name4', PNUCHOut.columns[14] : 'D_Code5', PNUCHOut.columns[15] : 'D_Name5',
                          PNUCHOut.columns[16] : 'D_Code6', PNUCHOut.columns[17] : 'D_Name6', PNUCHOut.columns[18] : 'Ins_Var', PNUCHOut.columns[19] : 'Ins_Sub'}, inplace=True)


## 02. Deleting Unnecessary Columns

print(PNUCHOut)


PNUCHOut['D_Code'] = PNUCHOut['D_Code'].fillna('NoDiag')
PNUCHOut['D_Code'] = PNUCHOut['D_Code'].astype(str); PNUCHOut['D_Code'] = PNUCHOut['D_Code'].str.replace('/',',')
PNUCHOut['D_Name'] = PNUCHOut['D_Name'].fillna('NoDiag')
PNUCHOut['D_Name'] = PNUCHOut['D_Name'].astype(str); PNUCHOut['D_Name'] = PNUCHOut['D_Name'].str.replace('/',',')
PNUCHOut['D_Code2'] = PNUCHOut['D_Code2'].fillna('NoDiag')
PNUCHOut['D_Code2'] = PNUCHOut['D_Code2'].astype(str); PNUCHOut['D_Code2'] = PNUCHOut['D_Code2'].str.replace('/',',')
PNUCHOut['D_Name2'] = PNUCHOut['D_Name2'].fillna('NoDiag')
PNUCHOut['D_Name2'] = PNUCHOut['D_Name2'].astype(str); PNUCHOut['D_Name2'] = PNUCHOut['D_Name2'].str.replace('/',',')
PNUCHOut['D_Code3'] = PNUCHOut['D_Code3'].fillna('NoDiag')
PNUCHOut['D_Code3'] = PNUCHOut['D_Code3'].astype(str); PNUCHOut['D_Code3'] = PNUCHOut['D_Code3'].str.replace('/',',')
PNUCHOut['D_Name3'] = PNUCHOut['D_Name3'].fillna('NoDiag')
PNUCHOut['D_Name3'] = PNUCHOut['D_Name3'].astype(str); PNUCHOut['D_Name3'] = PNUCHOut['D_Name3'].str.replace('/',',')
PNUCHOut['D_Code4'] = PNUCHOut['D_Code4'].fillna('NoDiag')
PNUCHOut['D_Code4'] = PNUCHOut['D_Code4'].astype(str); PNUCHOut['D_Code4'] = PNUCHOut['D_Code4'].str.replace('/',',')
PNUCHOut['D_Name4'] = PNUCHOut['D_Name4'].fillna('NoDiag')
PNUCHOut['D_Name4'] = PNUCHOut['D_Name4'].astype(str); PNUCHOut['D_Name4'] = PNUCHOut['D_Name4'].str.replace('/',',')
PNUCHOut['D_Code5'] = PNUCHOut['D_Code5'].fillna('NoDiag')
PNUCHOut['D_Code5'] = PNUCHOut['D_Code5'].astype(str); PNUCHOut['D_Code5'] = PNUCHOut['D_Code5'].str.replace('/',',')
PNUCHOut['D_Name5'] = PNUCHOut['D_Name5'].fillna('NoDiag')
PNUCHOut['D_Name5'] = PNUCHOut['D_Name5'].astype(str); PNUCHOut['D_Name5'] = PNUCHOut['D_Name5'].str.replace('/',',')
PNUCHOut['D_Code6'] = PNUCHOut['D_Code6'].fillna('NoDiag')
PNUCHOut['D_Code6'] = PNUCHOut['D_Code6'].astype(str); PNUCHOut['D_Code6'] = PNUCHOut['D_Code6'].str.replace('/',',')
PNUCHOut['D_Name6'] = PNUCHOut['D_Name6'].fillna('NoDiag')
PNUCHOut['D_Name6'] = PNUCHOut['D_Name6'].astype(str); PNUCHOut['D_Name6'] = PNUCHOut['D_Name6'].str.replace('/',',')

PNUCHOut.loc[PNUCHOut.D_Code != 'NoDiag', 'D_Code'] = PNUCHOut.D_Code.str[0:4]

del PNUCHOut['D_Code2']; del PNUCHOut['D_Code3']; del PNUCHOut['D_Code4']; del PNUCHOut['D_Code5']; del PNUCHOut['D_Code6']
del PNUCHOut['D_Name2']; del PNUCHOut['D_Name3']; del PNUCHOut['D_Name4']; del PNUCHOut['D_Name5']; del PNUCHOut['D_Name6']

PNUCHOut.drop_duplicates(['PT_No', 'Out_Date'], inplace=True)

PNUCHOut['Address'] = PNUCHOut['Address'].fillna('NoAdd')
PNUCHOut['Ins_Var'] = PNUCHOut['Ins_Var'].fillna('NoInsVar')
PNUCHOut['Ins_Sub'] = PNUCHOut['Ins_Sub'].fillna('NoVCode')


PNUCHOut.dropna(how='any')


PNUCHOut['PT_No'] = PNUCHOut['PT_No'].astype(str)
PNUCHOut['Birth'] = PNUCHOut['Birth'].astype(str); PNUCHOut['Birth'] = PNUCHOut['Birth'].str.replace('-',''); PNUCHOut['Birth'] = PNUCHOut['Birth'].astype(int)
PNUCHOut['Gender'] = PNUCHOut['Gender'].astype(int)
PNUCHOut['Address'] = PNUCHOut['Address'].astype(str)
PNUCHOut['Ins_Var'] = PNUCHOut['Ins_Var'].astype(str)
PNUCHOut['Ins_Sub'] = PNUCHOut['Ins_Sub'].astype(str)
PNUCHOut['Out_Date'] = PNUCHOut['Out_Date'].astype(str); PNUCHOut['Out_Date'] = PNUCHOut['Out_Date'].str.replace('-',''); PNUCHOut['Out_Date'] = PNUCHOut['Out_Date'].astype(int)
PNUCHOut['Out_Dep'] = PNUCHOut['Out_Dep'].astype(str)
PNUCHOut['D_Code'] = PNUCHOut['D_Code'].astype(str)
PNUCHOut['D_Name'] = PNUCHOut['D_Name'].astype(str)


# 07-1) Change values in 'Gender' Column ( '1' : Male, '2' : Female)
print(PNUCHOut.Gender.unique())
PNUCHOut.Gender.replace([1, 2], ['Male', 'Female'], inplace=True)

# 07-2) Create 'Age' Column
# df.loc[df['column name'] condition, 'new column name'] = 'value if condition is met'
PNUCHOut.loc[(PNUCHOut['Out_Date'] % 10000 <= PNUCHOut['Birth'] % 10000) & (PNUCHOut['Out_Date'] // 10000 >= PNUCHOut['Birth'] // 10000), 'Age'] = (PNUCHOut['Out_Date'] - PNUCHOut['Birth'])// 10000
PNUCHOut.loc[(PNUCHOut['Out_Date'] % 10000 > PNUCHOut['Birth'] % 10000) & (PNUCHOut['Out_Date'] // 10000 == PNUCHOut['Birth'] // 10000), 'Age'] = (PNUCHOut['Out_Date'] - PNUCHOut['Birth'])// 10000
PNUCHOut.loc[(PNUCHOut['Out_Date'] % 10000 > PNUCHOut['Birth'] % 10000) & (PNUCHOut['Out_Date'] // 10000 > PNUCHOut['Birth'] // 10000), 'Age'] = (PNUCHOut['Out_Date'] - PNUCHOut['Birth'])// 10000 - 1


PNUCHOut['Age'] = PNUCHOut['Age'].astype(int)


print(PNUCHOut.columns)
print(PNUCHOut['Age'])



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

PNUCHOut['Address'] = PNUCHOut['Address'].apply(lambda x: address_map[x])


PNUCHOut['Address'] = PNUCHOut['Address'].fillna('NoAdd')

'''
print(PNUCHOut['Address'].value_counts())
print(PNUCHOut['Address'])
'''


# 07-4) Rephrase 'Ins_Var' Values
'''
print(PNUCHOut['Ins_Var'].value_counts())
'''

PNUCHOut['Ins_Var'] = PNUCHOut['Ins_Var'].map({'1' : 'NHIS', '2' : 'MedCareT1', '3' : 'MedCareT2', '4' : 'MedCareDis'})
PNUCHOut['Ins_Var'] = PNUCHOut['Ins_Var'].fillna('Others')


'''
print(PNUCHOut['Ins_Var'].value_counts())
print(PNUCHOut['Ins_Var'])
'''


# 07-5) Rephrase 'Ins_Sub' Values (Not Executed)
'''
print(PNUCHOut['Ins_Sub'].value_counts())
'''

PNUCHOut['Ins_Sub'] = PNUCHOut.Ins_Sub.str.split(" ", expand=True)[0]

'''
print(PNUCHOut['Ins_Sub'].value_counts())
print(PNUCHOut['Ins_Sub'])
'''


# 07-6) Change Order of Columns


PNUCHOut['InstName'] = np.nan
PNUCHOut['InstName'] = PNUCHOut['InstName'].fillna('PNUCH')
#print(PNUCHOut.columns)

PNUCHOut = PNUCHOut[['InstName', 'PT_No', 'Age', 'Birth', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'Out_Date', 'Out_Dep', 'D_Code', 'D_Name']]


print(PNUCHOut.columns)
with pd.option_context('display.max_columns', None):
    print(PNUCHOut)


### Save as CSV in '/Users/peterpark/Desktop/PY_START/SNUH_Project01'


PNUCHOut.to_csv('./PNUCH/PNUCHOutPMain_R4A.csv', index=False)


