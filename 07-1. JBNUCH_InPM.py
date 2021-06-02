'''
This script is written 4 preprocessing JBNUCH's Inpatients' Data.

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
JBNUCHIn = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018JBNUCHIn_Prep.csv", encoding="utf-8", low_memory=False)
SevPri = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018-2019SevCri_5digitPri.csv", encoding="utf-8", low_memory=False)
SevSub = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018-2019SevCri_4digitSub.csv", encoding="utf-8", low_memory=False)


### Preprocessing


## 01. Renaming Columns
JBNUCHIn.reset_index(drop=True, inplace=True)
print(JBNUCHIn.columns)

'''
['환자아이디', '등록번호', '환자명', '생년월일', '성별', '주소', '유형코드', '환자유형명', '보조유형',
       '보조유형명', '산정특례 특정기호', '입원일', '입원진료과', '퇴원일', '퇴원진료과', '재원일수', 'KDRG번호',
       '주진단코드1', '주진단코드명1', '부진단코드1', '부진단코드명1', '부진단코드2', '부진단코드명2', '부진단코드3',
       '부진단코드명3', '부진단코드4', '부진단코드명4', '부진단코드5', '부진단코드명5', '수술집도의 진료과', '수술일',
       '수술코드(EDI)', '수술코드(한글명)', '수술명(영문명)', '급여-일부본인부담금', '급여-공단부담금',
       '전액보인부담', '비급여']
'''

'''
result: Index(['InstName', 'PT_No', 'Birth', 'Age', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'In_Date', 'In_Dep', 'Dis_Date', 'Dis_Dep', 'In_Prd', 'Pay_InsSelf',
                   'Pay_InsCorp', 'Pay_NoIns', 'Pay_Sel', 'D_Code', 'D_Name', 'DRGNO', 'Severity', 'Sur_Date', 'Sur_Code', 'Sur_Name'])
'''

JBNUCHIn.rename(columns={JBNUCHIn.columns[1] : 'PT_No', JBNUCHIn.columns[3] : 'Birth', JBNUCHIn.columns[4] : 'Gender', JBNUCHIn.columns[5] : 'Address', JBNUCHIn.columns[7] : 'Ins_Var',
                         JBNUCHIn.columns[10] : 'Ins_Sub', JBNUCHIn.columns[11] : 'In_Date', JBNUCHIn.columns[12] : 'In_Dep', JBNUCHIn.columns[13] : 'Dis_Date', JBNUCHIn.columns[14] : 'Dis_Dep',
                         JBNUCHIn.columns[15] : 'In_Prd', JBNUCHIn.columns[16] : 'DRGNO', JBNUCHIn.columns[17] : 'D_Code', JBNUCHIn.columns[18] : 'D_Name', JBNUCHIn.columns[19] : 'D_Code2',
                         JBNUCHIn.columns[20] : 'D_Name2', JBNUCHIn.columns[21] : 'D_Code3', JBNUCHIn.columns[22] : 'D_Name3', JBNUCHIn.columns[23] : 'D_Code4', JBNUCHIn.columns[24] : 'D_Name4',
                         JBNUCHIn.columns[25] : 'D_Code5', JBNUCHIn.columns[26] : 'D_Name5', JBNUCHIn.columns[27] : 'D_Code6', JBNUCHIn.columns[28] : 'D_Name6',
                         JBNUCHIn.columns[29] : 'Sur_Dep', JBNUCHIn.columns[30] : 'Sur_Date', JBNUCHIn.columns[31] : 'Sur_Code', JBNUCHIn.columns[32] : 'Sur_Name',
                         JBNUCHIn.columns[34] : 'Pay_InsSelf', JBNUCHIn.columns[35] : 'Pay_InsCorp', JBNUCHIn.columns[36] : 'Pay_Sel', JBNUCHIn.columns[37] : 'Pay_NoIns'}, inplace=True)


'''
print(JBNUCHIn.columns)
'''
JBNUCHIn.dropna(how='all', axis=0, inplace=True)
print(JBNUCHIn)

## 02. Deleting Unnecessary Columns
'''
print(JBNUCHIn.loc[0:5])
'''

# Diagnosis
JBNUCHIn['D_Code'] = JBNUCHIn['D_Code'].fillna('NoDiag')
JBNUCHIn['D_Code'] = JBNUCHIn['D_Code'].astype(str); JBNUCHIn['D_Code'] = JBNUCHIn['D_Code'].str.replace('/',',')
JBNUCHIn['D_Name'] = JBNUCHIn['D_Name'].fillna('NoDiag')
JBNUCHIn['D_Name'] = JBNUCHIn['D_Name'].astype(str); JBNUCHIn['D_Name'] = JBNUCHIn['D_Name'].str.replace('/',',')

JBNUCHIn.loc[JBNUCHIn.D_Code != 'NoDiag', 'D_Code'] = JBNUCHIn.D_Code.str[0:4]


del JBNUCHIn['D_Code2']; del JBNUCHIn['D_Code3']; del JBNUCHIn['D_Code4']; del JBNUCHIn['D_Code5']; del JBNUCHIn['D_Code6']
del JBNUCHIn['D_Name2']; del JBNUCHIn['D_Name3']; del JBNUCHIn['D_Name4']; del JBNUCHIn['D_Name5']; del JBNUCHIn['D_Name6']


# Surgery
JBNUCHIn['Sur_Code'] = JBNUCHIn['Sur_Code'].fillna('NoSur')
JBNUCHIn['Sur_Code'] = JBNUCHIn['Sur_Code'].astype(str); JBNUCHIn['Sur_Code'] = JBNUCHIn['Sur_Code'].str.replace('/',',')
JBNUCHIn['Sur_Name'] = JBNUCHIn['Sur_Name'].fillna('NoSur')
JBNUCHIn['Sur_Name'] = JBNUCHIn['Sur_Name'].astype(str); JBNUCHIn['Sur_Name'] = JBNUCHIn['Sur_Name'].str.replace('/',',')
print(JBNUCHIn['Sur_Date'])
JBNUCHIn['Sur_Date'] = JBNUCHIn['Sur_Date'].fillna('NoSur')
JBNUCHIn['Sur_Date'] = JBNUCHIn['Sur_Date'].astype(str); JBNUCHIn['Sur_Date'] = JBNUCHIn['Sur_Date'].str.replace('-','')
print(JBNUCHIn['Sur_Date'])



# Severity
print(SevPri.columns)
SevPri.rename(columns={SevPri.columns[0] : 'DRGNO01', SevPri.columns[1] : 'Severity01'}, inplace=True)
SevPri['Severity01'].fillna('분류오류', inplace=True)
print(SevPri)
JBNUCHIn['DRGNO01'] = JBNUCHIn['DRGNO'].astype(str).str[0:5]
JBNUCHIn['DRGNO01'].fillna('NoDRG', inplace=True)

print(SevSub.columns)
SevSub.rename(columns={SevSub.columns[0] : 'DRGNO02', SevSub.columns[1] : 'Severity02'}, inplace=True)
print(SevSub)
JBNUCHIn['DRGNO02'] = JBNUCHIn['DRGNO'].astype(str).str[0:4]
JBNUCHIn['DRGNO02'].fillna('NoDRG', inplace=True)

JBNUCHIn = JBNUCHIn.merge(SevPri, on='DRGNO01', how='left').merge(SevSub, on='DRGNO02', how='left')

JBNUCHIn.drop_duplicates(['PT_No', 'Birth', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'In_Date', 'In_Dep', 'Dis_Date', 'Dis_Dep', 'In_Prd', 'DRGNO', 'Pay_InsSelf',
                          'Pay_InsCorp', 'Pay_Sel', 'Pay_NoIns', 'D_Code', 'D_Name', 'Sur_Code', 'Sur_Name', 'Sur_Date', 'DRGNO01', 'DRGNO02', 'Severity01', 'Severity02'], inplace=True)

JBNUCHIn.reset_index(drop=True, inplace=True)


JBNUCHIn['Severity01'] = JBNUCHIn['Severity01'].map({'전문' : 'Severe', '일반' : 'Normal', '단순' : 'Simple', '분류오류' : 'SortError'})
JBNUCHIn['Severity02'] = JBNUCHIn['Severity02'].map({'전문' : 'Severe', '일반' : 'Normal', '단순' : 'Simple', '분류오류' : 'SortError'})

JBNUCHIn['Severity01'] = JBNUCHIn['Severity01'].map({'Severe' : 4, 'Normal' : 3, 'Simple' : 2, 'SortError' : 1})
JBNUCHIn['Severity01'] = JBNUCHIn['Severity01'].fillna(0)
JBNUCHIn['Severity02'] = JBNUCHIn['Severity02'].map({'Severe' : 4, 'Normal' : 3, 'Simple' : 2, 'SortError' : 1})
JBNUCHIn['Severity02'] = JBNUCHIn['Severity02'].fillna(0)

JBNUCHIn.loc[(JBNUCHIn['Severity01']  >= JBNUCHIn['Severity02']), 'Severity'] = JBNUCHIn['Severity01']
JBNUCHIn.loc[(JBNUCHIn['Severity01']  < JBNUCHIn['Severity02']), 'Severity'] = JBNUCHIn['Severity02']

JBNUCHIn['Severity01'] = JBNUCHIn['Severity01'].map({4 :'Severe', 3 :'Normal', 2 :'Simple', 1 :'SortError', 0 :'NoDRG'})
JBNUCHIn['Severity02'] = JBNUCHIn['Severity02'].map({4 :'Severe', 3 :'Normal', 2 :'Simple', 1 :'SortError', 0 :'NoDRG'})
JBNUCHIn['Severity'] = JBNUCHIn['Severity'].map({4 :'Severe', 3 :'Normal', 2 :'Simple', 1 :'SortError', 0 :'NoDRG'})

JBNUCHIn.loc[(JBNUCHIn['Severity']  == 'NoDRG'), 'DRGNO'] = 'NoDRG'

print(JBNUCHIn.columns)
print(JBNUCHIn)



## 03. Refine JBNUCHIn

JBNUCHIn.drop_duplicates(['PT_No', 'In_Date'], inplace=True)
JBNUCHIn.reset_index(drop=True, inplace=True)

JBNUCHIn['DRGNO'] = JBNUCHIn['DRGNO'].fillna('NoDRG')
JBNUCHIn['Severity'] = JBNUCHIn['Severity'].fillna('NoDRG')

JBNUCHIn['Sur_Date'] = JBNUCHIn['Sur_Date'].fillna('NoSur')
JBNUCHIn['Sur_Code'] = JBNUCHIn['Sur_Code'].fillna('NoSur')
JBNUCHIn['Sur_Name'] = JBNUCHIn['Sur_Name'].fillna('NoSur')


JBNUCHIn['Address'] = JBNUCHIn['Address'].fillna('NoAdd')
JBNUCHIn['Ins_Var'] = JBNUCHIn['Ins_Var'].fillna('NoInsVar')
JBNUCHIn['Ins_Sub'] = JBNUCHIn['Ins_Sub'].fillna('NoVCode')
JBNUCHIn['In_Prd'] = JBNUCHIn['In_Prd'].fillna(0)
JBNUCHIn['Pay_InsSelf'] = JBNUCHIn['Pay_InsSelf'].fillna(0)
JBNUCHIn['Pay_InsCorp'] = JBNUCHIn['Pay_InsCorp'].fillna(0)
JBNUCHIn['Pay_NoIns'] = JBNUCHIn['Pay_NoIns'].fillna(0)
JBNUCHIn['Pay_Sel'] = JBNUCHIn['Pay_Sel'].fillna(0)


JBNUCHIn.dropna(how='any')
'''
print(len(JBNUCHIn))
'''

print(JBNUCHIn)


JBNUCHIn['PT_No'] = JBNUCHIn['PT_No'].astype(str)
JBNUCHIn['Birth'] = JBNUCHIn['Birth'].astype(str); JBNUCHIn['Birth'] = JBNUCHIn['Birth'].str.replace('-','')
JBNUCHIn['Birth'] = JBNUCHIn['Birth'].astype(int)
JBNUCHIn['Gender'] = JBNUCHIn['Gender'].astype(str)
JBNUCHIn['Address'] = JBNUCHIn['Address'].astype(str)
JBNUCHIn['Ins_Var'] = JBNUCHIn['Ins_Var'].astype(str)
JBNUCHIn['Ins_Sub'] = JBNUCHIn['Ins_Sub'].astype(str)
JBNUCHIn['In_Date'] = JBNUCHIn['In_Date'].astype(str); JBNUCHIn['In_Date'] = JBNUCHIn['In_Date'].str.replace('-','')
JBNUCHIn['In_Date'] = JBNUCHIn['In_Date'].astype(int)
JBNUCHIn['In_Dep'] = JBNUCHIn['In_Dep'].astype(str)
JBNUCHIn['Dis_Date'] = JBNUCHIn['Dis_Date'].astype(str); JBNUCHIn['Dis_Date'] = JBNUCHIn['Dis_Date'].str.replace('-','')
JBNUCHIn['Dis_Date'] = JBNUCHIn['Dis_Date'].astype(int)
JBNUCHIn['Dis_Dep'] = JBNUCHIn['Dis_Dep'].astype(str)
JBNUCHIn['In_Prd'] = JBNUCHIn['In_Prd'].astype(int)
JBNUCHIn['Pay_InsSelf'] = JBNUCHIn['Pay_InsSelf'].astype(int)
JBNUCHIn['Pay_InsCorp'] = JBNUCHIn['Pay_InsCorp'].astype(int)
JBNUCHIn['Pay_NoIns'] = JBNUCHIn['Pay_NoIns'].astype(int)
JBNUCHIn['Pay_Sel'] = JBNUCHIn['Pay_Sel'].astype(int)
JBNUCHIn['D_Code'] = JBNUCHIn['D_Code'].astype(str)
JBNUCHIn['D_Name'] = JBNUCHIn['D_Name'].astype(str)
JBNUCHIn['DRGNO'] = JBNUCHIn['DRGNO'].astype(str)
JBNUCHIn['Severity'] = JBNUCHIn['Severity'].astype(str)
JBNUCHIn['Sur_Date'] = JBNUCHIn['Sur_Date'].astype(str)
JBNUCHIn['Sur_Code'] = JBNUCHIn['Sur_Code'].astype(str)
JBNUCHIn['Sur_Name'] = JBNUCHIn['Sur_Name'].astype(str)



# 04-1) Change values in 'Gender' Column ( '1' : Male, '2' : Female)
JBNUCHIn.Gender.replace(['M', 'F'], ['Male', 'Female'], inplace=True)

# 04-2) Create 'Age' Column
# df.loc[df['column name'] condition, 'new column name'] = 'value if condition is met'
JBNUCHIn.loc[(JBNUCHIn['In_Date'] % 10000 <= JBNUCHIn['Birth'] % 10000) & (JBNUCHIn['In_Date'] // 10000 >= JBNUCHIn['Birth'] // 10000), 'Age'] = (JBNUCHIn['In_Date'] - JBNUCHIn['Birth'])// 10000
JBNUCHIn.loc[(JBNUCHIn['In_Date'] % 10000 > JBNUCHIn['Birth'] % 10000) & (JBNUCHIn['In_Date'] // 10000 == JBNUCHIn['Birth'] // 10000), 'Age'] = (JBNUCHIn['In_Date'] - JBNUCHIn['Birth'])// 10000
JBNUCHIn.loc[(JBNUCHIn['In_Date'] % 10000 > JBNUCHIn['Birth'] % 10000) & (JBNUCHIn['In_Date'] // 10000 > JBNUCHIn['Birth'] // 10000), 'Age'] = (JBNUCHIn['In_Date'] - JBNUCHIn['Birth'])// 10000 - 1


JBNUCHIn['Age'] = JBNUCHIn['Age'].astype(int)


print(JBNUCHIn.columns)
print(JBNUCHIn['Age'])



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

JBNUCHIn['Address'] = JBNUCHIn['Address'].apply(lambda x: address_map[x])


JBNUCHIn['Address'] = JBNUCHIn['Address'].fillna('NoAdd')




# 04-4) Rephrase 'Ins_Var' Values

JBNUCHIn['Ins_Var'] = JBNUCHIn['Ins_Var'].map({'건강보험' : 'NHIS', '의료급여1종' : 'MedCareT1', '의료급여2종' : 'MedCareT2', '의료급여 2종 장애인' : 'MedCareDis'})
JBNUCHIn['Ins_Var'] = JBNUCHIn['Ins_Var'].fillna('Others')


# 04-5) Change Order of Columns
JBNUCHIn['InstName'] = np.nan
JBNUCHIn['InstName'] = JBNUCHIn['InstName'].fillna('JBNUCH')
JBNUCHIn = JBNUCHIn[['InstName', 'PT_No', 'Birth', 'Age', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'In_Date', 'In_Dep', 'Dis_Date', 'Dis_Dep', 'In_Prd', 'Pay_InsSelf',
                     'Pay_InsCorp', 'Pay_NoIns', 'Pay_Sel', 'D_Code', 'D_Name', 'DRGNO', 'Severity', 'Sur_Date', 'Sur_Code', 'Sur_Name']]


print(JBNUCHIn.columns)



### Save as CSV in '/Users/peterpark/Desktop/PY_START/SNUH_Project01'

JBNUCHIn.to_csv('./JBNUCH/JBNUCHInPMain_R4A.csv', index=False)

