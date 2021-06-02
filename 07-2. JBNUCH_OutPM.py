'''
This script is written 4 preprocessing JBNUCH's Outpatients' Data.

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

JBNUCHOut= pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018JBNUCHOut_Prep.csv", low_memory=False, encoding="utf-8")



print(JBNUCHOut.columns)
'''
['환자ID', '생년월일', '성별', '주소', '외래진료일1', '외래진료과1', '주진단코드', '주진단코드명',
       '부진단코드', '부진단코드명', '부진단코드2', '부진단코드명2', '부진단코드3', '부진단코드명3', '부진단코드4',
       '부진단코드명4', '부진단코드5', '부진단코드명5', '보험급종', '산정특례 특정기호']
'''

'''
['InstName', 'PT_No', 'Age', 'Birth', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'Out_Date', 'Out_Dep', 'D_Code', 'D_Name']
'''

JBNUCHOut.rename(columns={JBNUCHOut.columns[0] : 'PT_No', JBNUCHOut.columns[1] : 'Birth', JBNUCHOut.columns[2] : 'Gender', JBNUCHOut.columns[3] : 'Address',
                          JBNUCHOut.columns[4] : 'Out_Date', JBNUCHOut.columns[5] : 'Out_Dep', JBNUCHOut.columns[6] : 'D_Code', JBNUCHOut.columns[7] : 'D_Name',
                          JBNUCHOut.columns[8] : 'D_Code2', JBNUCHOut.columns[9] : 'D_Name2', JBNUCHOut.columns[10] : 'D_Code3', JBNUCHOut.columns[11] : 'D_Name3',
                          JBNUCHOut.columns[12] : 'D_Code4', JBNUCHOut.columns[13] : 'D_Name4', JBNUCHOut.columns[14] : 'D_Code5', JBNUCHOut.columns[15] : 'D_Name5',
                          JBNUCHOut.columns[16] : 'D_Code6', JBNUCHOut.columns[17] : 'D_Name6', JBNUCHOut.columns[18] : 'Ins_Var', JBNUCHOut.columns[19] : 'Ins_Sub'}, inplace=True)


## 02. Deleting Unnecessary Columns

print(JBNUCHOut.loc[0:5])


JBNUCHOut['D_Code'] = JBNUCHOut['D_Code'].replace('없음', 'NoDiag')
JBNUCHOut['D_Code'] = JBNUCHOut['D_Code'].astype(str); JBNUCHOut['D_Code'] = JBNUCHOut['D_Code'].str.replace('/',',')
JBNUCHOut['D_Name'] = JBNUCHOut['D_Name'].replace('없음', 'NoDiag')
JBNUCHOut['D_Name'] = JBNUCHOut['D_Name'].astype(str); JBNUCHOut['D_Name'] = JBNUCHOut['D_Name'].str.replace('/',',')


JBNUCHOut.loc[JBNUCHOut.D_Code != 'NoDiag', 'D_Code'] = JBNUCHOut.D_Code.str[0:4]

del JBNUCHOut['D_Code2']; del JBNUCHOut['D_Code3']; del JBNUCHOut['D_Code4']; del JBNUCHOut['D_Code5']; del JBNUCHOut['D_Code6']
del JBNUCHOut['D_Name2']; del JBNUCHOut['D_Name3']; del JBNUCHOut['D_Name4']; del JBNUCHOut['D_Name5']; del JBNUCHOut['D_Name6']

JBNUCHOut.drop_duplicates(['PT_No', 'Out_Date'], inplace=True)

JBNUCHOut['Address'] = JBNUCHOut['Address'].fillna('NoAdd')
JBNUCHOut['Ins_Var'] = JBNUCHOut['Ins_Var'].fillna('NoInsVar')
JBNUCHOut['Ins_Sub'] = JBNUCHOut['Ins_Sub'].fillna('NoVCode')

JBNUCHOut.dropna(how='any')


JBNUCHOut['PT_No'] = JBNUCHOut['PT_No'].astype(str)
JBNUCHOut['Birth'] = JBNUCHOut['Birth'].astype(int)
JBNUCHOut['Gender'] = JBNUCHOut['Gender'].astype(str)
JBNUCHOut['Address'] = JBNUCHOut['Address'].astype(str)
JBNUCHOut['Ins_Var'] = JBNUCHOut['Ins_Var'].astype(str)
JBNUCHOut['Ins_Sub'] = JBNUCHOut['Ins_Sub'].astype(str)
JBNUCHOut['Out_Date'] = JBNUCHOut['Out_Date'].astype(int)
JBNUCHOut['Out_Dep'] = JBNUCHOut['Out_Dep'].astype(str)
JBNUCHOut['D_Code'] = JBNUCHOut['D_Code'].astype(str)
JBNUCHOut['D_Name'] = JBNUCHOut['D_Name'].astype(str)


# 07-1) Change values in 'Gender' Column ( '1' : Male, '2' : Female)
print(JBNUCHOut.Gender.unique())
JBNUCHOut.Gender.replace(['M', 'F'], ['Male', 'Female'], inplace=True)

# 07-2) Create 'Age' Column
# df.loc[df['column name'] condition, 'new column name'] = 'value if condition is met'
JBNUCHOut.loc[(JBNUCHOut['Out_Date'] % 10000 <= JBNUCHOut['Birth'] % 10000) & (JBNUCHOut['Out_Date'] // 10000 >= JBNUCHOut['Birth'] // 10000), 'Age'] = (JBNUCHOut['Out_Date'] - JBNUCHOut['Birth'])// 10000
JBNUCHOut.loc[(JBNUCHOut['Out_Date'] % 10000 > JBNUCHOut['Birth'] % 10000) & (JBNUCHOut['Out_Date'] // 10000 == JBNUCHOut['Birth'] // 10000), 'Age'] = (JBNUCHOut['Out_Date'] - JBNUCHOut['Birth'])// 10000
JBNUCHOut.loc[(JBNUCHOut['Out_Date'] % 10000 > JBNUCHOut['Birth'] % 10000) & (JBNUCHOut['Out_Date'] // 10000 > JBNUCHOut['Birth'] // 10000), 'Age'] = (JBNUCHOut['Out_Date'] - JBNUCHOut['Birth'])// 10000 - 1


JBNUCHOut['Age'] = JBNUCHOut['Age'].astype(int)


print(JBNUCHOut.columns)
print(JBNUCHOut['Age'])



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

JBNUCHOut['Address'] = JBNUCHOut['Address'].apply(lambda x: address_map[x])


JBNUCHOut['Address'] = JBNUCHOut['Address'].fillna('NoAdd')

'''
print(JBNUCHOut['Address'].value_counts())
print(JBNUCHOut['Address'])
'''


# 07-4) Rephrase 'Ins_Var' Values
'''
print(JBNUCHOut['Ins_Var'].value_counts())
'''

JBNUCHOut['Ins_Var'] = JBNUCHOut['Ins_Var'].map({'건강보험' : 'NHIS', '의료급여1종' : 'MedCareT1', '의료급여2종' : 'MedCareT2', '의료급여 2종 장애인' : 'MedCareDis'})
JBNUCHOut['Ins_Var'] = JBNUCHOut['Ins_Var'].fillna('Others')


'''
print(JBNUCHOut['Ins_Var'].value_counts())
print(JBNUCHOut['Ins_Var'])
'''


# 07-5) Rephrase 'Ins_Sub' Values (Not Executed)
'''
print(JBNUCHOut['Ins_Sub'].value_counts())
'''

JBNUCHOut['Ins_Sub'] = JBNUCHOut.Ins_Sub.str.split(" ", expand=True)[0]

'''
print(JBNUCHOut['Ins_Sub'].value_counts())
print(JBNUCHOut['Ins_Sub'])
'''


# 07-6) Change Order of Columns


JBNUCHOut['InstName'] = np.nan
JBNUCHOut['InstName'] = JBNUCHOut['InstName'].fillna('JBNUCH')
#print(JBNUCHOut.columns)

JBNUCHOut = JBNUCHOut[['InstName', 'PT_No', 'Age', 'Birth', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'Out_Date', 'Out_Dep', 'D_Code', 'D_Name']]


print(JBNUCHOut.columns)
with pd.option_context('display.max_columns', None):
    print(JBNUCHOut)


### Save as CSV in '/Users/peterpark/Desktop/PY_START/SNUH_Project01'


JBNUCHOut.to_csv('./JBNUCH/JBNUCHOutPMain_R4A.csv', index=False)


