'''
This script is written 4 preprocessing KBNUCH's Outpatients' Data.

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

KBNUCHOut= pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018KBNUCHOut_Prep.csv", low_memory=False, encoding="utf-8")



print(KBNUCHOut.columns)
'''
['환자ID', '생년월일', '성별', '광역시/도', '시/군/구', '외래진료일1', '외래진료과1', '주진단코드',
       '주진단코드명', '부진단코드', '부진단코드명', '부진단코드2', '부진단코드명2', '부진단코드3', '부진단코드명3',
       '부진단코드4', '부진단코드명4', '부진단코드5', '부진단코드명5', '보험급종', '산정특례 특정기호']
'''

'''
['InstName', 'PT_No', 'Age', 'Birth', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'Out_Date', 'Out_Dep', 'D_Code', 'D_Name']
'''

KBNUCHOut.rename(columns={KBNUCHOut.columns[0] : 'PT_No', KBNUCHOut.columns[1] : 'Birth', KBNUCHOut.columns[2] : 'Gender', KBNUCHOut.columns[3] : 'Address',
                          KBNUCHOut.columns[5] : 'Out_Date', KBNUCHOut.columns[6] : 'Out_Dep', KBNUCHOut.columns[7] : 'D_Code1', KBNUCHOut.columns[8] : 'D_Name1',
                          KBNUCHOut.columns[9] : 'D_Code2', KBNUCHOut.columns[10] : 'D_Name2', KBNUCHOut.columns[11] : 'D_Code3', KBNUCHOut.columns[12] : 'D_Name3',
                          KBNUCHOut.columns[13] : 'D_Code4', KBNUCHOut.columns[14] : 'D_Name4', KBNUCHOut.columns[15] : 'D_Code5', KBNUCHOut.columns[16] : 'D_Name5',
                          KBNUCHOut.columns[17] : 'D_Code6', KBNUCHOut.columns[18] : 'D_Name6', KBNUCHOut.columns[19] : 'Ins_Var', KBNUCHOut.columns[20] : 'Ins_Sub'}, inplace=True)


## 02. Deleting Unnecessary Columns

print(KBNUCHOut.loc[0:5])


KBNUCHOut['D_Code1'] = KBNUCHOut['D_Code1'].fillna('NoDiag')
KBNUCHOut['D_Code1'] = KBNUCHOut['D_Code1'].astype(str); KBNUCHOut['D_Code1'] = KBNUCHOut['D_Code1'].str.replace('/',',')
KBNUCHOut['D_Name1'] = KBNUCHOut['D_Name1'].fillna('NoDiag')
KBNUCHOut['D_Name1'] = KBNUCHOut['D_Name1'].astype(str); KBNUCHOut['D_Name1'] = KBNUCHOut['D_Name1'].str.replace('/',',')
KBNUCHOut['D_Code2'] = KBNUCHOut['D_Code2'].fillna('NoDiag')
KBNUCHOut['D_Code2'] = KBNUCHOut['D_Code2'].astype(str); KBNUCHOut['D_Code2'] = KBNUCHOut['D_Code2'].str.replace('/',',')
KBNUCHOut['D_Name2'] = KBNUCHOut['D_Name2'].fillna('NoDiag')
KBNUCHOut['D_Name2'] = KBNUCHOut['D_Name2'].astype(str); KBNUCHOut['D_Name2'] = KBNUCHOut['D_Name2'].str.replace('/',',')
KBNUCHOut['D_Code3'] = KBNUCHOut['D_Code3'].fillna('NoDiag')
KBNUCHOut['D_Code3'] = KBNUCHOut['D_Code3'].astype(str); KBNUCHOut['D_Code3'] = KBNUCHOut['D_Code3'].str.replace('/',',')
KBNUCHOut['D_Name3'] = KBNUCHOut['D_Name3'].fillna('NoDiag')
KBNUCHOut['D_Name3'] = KBNUCHOut['D_Name3'].astype(str); KBNUCHOut['D_Name3'] = KBNUCHOut['D_Name3'].str.replace('/',',')
KBNUCHOut['D_Code4'] = KBNUCHOut['D_Code4'].fillna('NoDiag')
KBNUCHOut['D_Code4'] = KBNUCHOut['D_Code4'].astype(str); KBNUCHOut['D_Code4'] = KBNUCHOut['D_Code4'].str.replace('/',',')
KBNUCHOut['D_Name4'] = KBNUCHOut['D_Name4'].fillna('NoDiag')
KBNUCHOut['D_Name4'] = KBNUCHOut['D_Name4'].astype(str); KBNUCHOut['D_Name4'] = KBNUCHOut['D_Name4'].str.replace('/',',')
KBNUCHOut['D_Code5'] = KBNUCHOut['D_Code5'].fillna('NoDiag')
KBNUCHOut['D_Code5'] = KBNUCHOut['D_Code5'].astype(str); KBNUCHOut['D_Code5'] = KBNUCHOut['D_Code5'].str.replace('/',',')
KBNUCHOut['D_Name5'] = KBNUCHOut['D_Name5'].fillna('NoDiag')
KBNUCHOut['D_Name5'] = KBNUCHOut['D_Name5'].astype(str); KBNUCHOut['D_Name5'] = KBNUCHOut['D_Name5'].str.replace('/',',')
KBNUCHOut['D_Code6'] = KBNUCHOut['D_Code6'].fillna('NoDiag')
KBNUCHOut['D_Code6'] = KBNUCHOut['D_Code6'].astype(str); KBNUCHOut['D_Code6'] = KBNUCHOut['D_Code6'].str.replace('/',',')
KBNUCHOut['D_Name6'] = KBNUCHOut['D_Name6'].fillna('NoDiag')
KBNUCHOut['D_Name6'] = KBNUCHOut['D_Name6'].astype(str); KBNUCHOut['D_Name6'] = KBNUCHOut['D_Name6'].str.replace('/',',')

KBNUCHOut.loc[KBNUCHOut.D_Code1 != 'NoDiag', 'D_Code1'] = KBNUCHOut.D_Code1.str[0:4]
KBNUCHOut.loc[KBNUCHOut.D_Code2 != 'NoDiag', 'D_Code2'] = KBNUCHOut.D_Code2.str[0:4]
KBNUCHOut.loc[KBNUCHOut.D_Code3 != 'NoDiag', 'D_Code3'] = KBNUCHOut.D_Code3.str[0:4]
KBNUCHOut.loc[KBNUCHOut.D_Code4 != 'NoDiag', 'D_Code4'] = KBNUCHOut.D_Code4.str[0:4]
KBNUCHOut.loc[KBNUCHOut.D_Code5 != 'NoDiag', 'D_Code5'] = KBNUCHOut.D_Code5.str[0:4]
KBNUCHOut.loc[KBNUCHOut.D_Code6 != 'NoDiag', 'D_Code6'] = KBNUCHOut.D_Code6.str[0:4]

KBNUCHOut['D_Code'] = KBNUCHOut['D_Code1']+'/'+KBNUCHOut['D_Code2']+'/'+KBNUCHOut['D_Code3']+'/'+KBNUCHOut['D_Code4']+'/'+KBNUCHOut['D_Code5']+'/'+KBNUCHOut['D_Code6']
KBNUCHOut['D_Name'] = KBNUCHOut['D_Name1']+'/'+KBNUCHOut['D_Name2']+'/'+KBNUCHOut['D_Name3']+'/'+KBNUCHOut['D_Name4']+'/'+KBNUCHOut['D_Name5']+'/'+KBNUCHOut['D_Name6']

del KBNUCHOut['D_Code1']; del KBNUCHOut['D_Code2']; del KBNUCHOut['D_Code3']; del KBNUCHOut['D_Code4']; del KBNUCHOut['D_Code5']; del KBNUCHOut['D_Code6']
del KBNUCHOut['D_Name1']; del KBNUCHOut['D_Name2']; del KBNUCHOut['D_Name3']; del KBNUCHOut['D_Name4']; del KBNUCHOut['D_Name5']; del KBNUCHOut['D_Name6']

KBNUCHOut.drop_duplicates(['PT_No', 'Out_Date'], inplace=True)

KBNUCHOut['Address'] = KBNUCHOut['Address'].fillna('NoAdd')
KBNUCHOut['Ins_Var'] = KBNUCHOut['Ins_Var'].fillna('NoInsVar')
KBNUCHOut['Ins_Sub'] = KBNUCHOut['Ins_Sub'].fillna('NoVCode')
KBNUCHOut['Ins_Sub'] = KBNUCHOut['Ins_Sub'].str.replace('-', 'NoVCode')

KBNUCHOut.dropna(how='any')


KBNUCHOut['PT_No'] = KBNUCHOut['PT_No'].astype(str)
KBNUCHOut['Birth'] = KBNUCHOut['Birth'].astype(int)
KBNUCHOut['Gender'] = KBNUCHOut['Gender'].astype(int)
KBNUCHOut['Address'] = KBNUCHOut['Address'].astype(str)
KBNUCHOut['Ins_Var'] = KBNUCHOut['Ins_Var'].astype(int)
KBNUCHOut['Ins_Sub'] = KBNUCHOut['Ins_Sub'].astype(str)
KBNUCHOut['Out_Date'] = KBNUCHOut['Out_Date'].astype(int)
KBNUCHOut['Out_Dep'] = KBNUCHOut['Out_Dep'].astype(str)
KBNUCHOut['D_Code'] = KBNUCHOut['D_Code'].astype(str)
KBNUCHOut['D_Name'] = KBNUCHOut['D_Name'].astype(str)


# 07-1) Change values in 'Gender' Column ( '1' : Male, '2' : Female)
print(KBNUCHOut.Gender.unique())
KBNUCHOut.Gender.replace([1, 2], ['Male', 'Female'], inplace=True)

# 07-2) Create 'Age' Column
# df.loc[df['column name'] condition, 'new column name'] = 'value if condition is met'
KBNUCHOut.loc[(KBNUCHOut['Out_Date'] % 10000 <= KBNUCHOut['Birth'] % 10000) & (KBNUCHOut['Out_Date'] // 10000 >= KBNUCHOut['Birth'] // 10000), 'Age'] = (KBNUCHOut['Out_Date'] - KBNUCHOut['Birth'])// 10000
KBNUCHOut.loc[(KBNUCHOut['Out_Date'] % 10000 > KBNUCHOut['Birth'] % 10000) & (KBNUCHOut['Out_Date'] // 10000 == KBNUCHOut['Birth'] // 10000), 'Age'] = (KBNUCHOut['Out_Date'] - KBNUCHOut['Birth'])// 10000
KBNUCHOut.loc[(KBNUCHOut['Out_Date'] % 10000 > KBNUCHOut['Birth'] % 10000) & (KBNUCHOut['Out_Date'] // 10000 > KBNUCHOut['Birth'] // 10000), 'Age'] = (KBNUCHOut['Out_Date'] - KBNUCHOut['Birth'])// 10000 - 1


KBNUCHOut['Age'] = KBNUCHOut['Age'].astype(int)


print(KBNUCHOut.columns)
print(KBNUCHOut['Age'])



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

KBNUCHOut['Address'] = KBNUCHOut['Address'].apply(lambda x: address_map[x])


KBNUCHOut['Address'] = KBNUCHOut['Address'].fillna('NoAdd')

'''
print(KBNUCHOut['Address'].value_counts())
print(KBNUCHOut['Address'])
'''


# 07-4) Rephrase 'Ins_Var' Values
'''
print(KBNUCHOut['Ins_Var'].value_counts())
'''

KBNUCHOut['Ins_Var'] = KBNUCHOut['Ins_Var'].map({1 : 'NHIS', 2 : 'MedCareT1', 3 : 'MedCareT2', 4 : 'MedCareDis'})
KBNUCHOut['Ins_Var'] = KBNUCHOut['Ins_Var'].fillna('Others')


'''
print(KBNUCHOut['Ins_Var'].value_counts())
print(KBNUCHOut['Ins_Var'])
'''


# 07-5) Rephrase 'Ins_Sub' Values (Not Executed)
'''
print(KBNUCHOut['Ins_Sub'].value_counts())
'''

KBNUCHOut['Ins_Sub'] = KBNUCHOut.Ins_Sub.str.split(" ", expand=True)[0]

'''
print(KBNUCHOut['Ins_Sub'].value_counts())
print(KBNUCHOut['Ins_Sub'])
'''


# 07-6) Change Order of Columns


KBNUCHOut['InstName'] = np.nan
KBNUCHOut['InstName'] = KBNUCHOut['InstName'].fillna('KBNUCH')
#print(KBNUCHOut.columns)

KBNUCHOut = KBNUCHOut[['InstName', 'PT_No', 'Age', 'Birth', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'Out_Date', 'Out_Dep', 'D_Code', 'D_Name']]


print(KBNUCHOut.columns)
with pd.option_context('display.max_columns', None):
    print(KBNUCHOut)


### Save as CSV in '/Users/peterpark/Desktop/PY_START/SNUH_Project01'


KBNUCHOut.to_csv('./KBNUCH/KBNUCHOutP_R4A.csv', index=False)


