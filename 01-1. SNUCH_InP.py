'''
This script is written 4 preprocessing SNUCH's Inpatients' Data.

Written Date: 2019.12.09
Written By: Peter JH Park

'''

### Import modules in needs

import os, sys, csv
import pandas as pd
import numpy as np
import datetime, time

print("\n Current Working Directory is: ", os.getcwd())

### READ Files & Check

InD = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018SNUCHInDiag_Prep.csv", encoding="utf-8", low_memory=False)
InS = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018SNUCHInSur_Prep.csv", encoding="utf-8", low_memory=False)
InsPayCri = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018SNUCH_InsPayCri.csv", encoding="utf-8", low_memory=False)
InR = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018SNUCHInSev_Prep.csv", encoding="utf-8", low_memory=False)
InV = pd.read_csv("/Users/peterpark/Desktop/DATA_ANALYSIS/Research/Dev4PubChildCenter/RawData/2018SNUCHInRare_Prep.csv", encoding="utf-8", low_memory=False)

'''
print(InD.columns)
print(InS.columns)
print(InR.columns)
'''

### Preprocessing


## 01. Renaming Columns

'''
Codes :
PACT_ID(Del); 환자번호 = PT_No; 생년월일 = Birth; 성별 = Gender; 주소 = Address; 급종 = Ins_Var; 급종보조 = Ins_Sub; 입원일 = In_Date; 입원진료과 = In_Dep; 퇴원일 = Dis_Date; 퇴원진료과 = Dis_Dep;
재원일수(입원일수) = In_Prd; DRGNO(Del); 급여본인부담 = Pay_InsSelf; 공단부담금 = Pay_InsCorp; 비급여 = Pay_NoIns; 선택진료료 = Pay_Sel; 주진단코드 = D_Code1; 주진단명 = D_Name1; 부진단코드# = D_Code#; 부진단명# = D_Name#;
수술일 = Sur_Date; 수술코드 = Sur_Code; 수술명 = Sur_Name; 소속진료과 = Sur_Dep; 수진자명(Del); KDRG = DRGNO; ADRG(Del); 질병군\n구분 = Severity; 요양개시일(Del); 진료과(Del); KDRG명칭(Del);
'''

InD.rename(columns={InD.columns[1] : 'PT_No', InD.columns[2] : 'Birth', InD.columns[3] : 'Gender', InD.columns[4] : 'Address', InD.columns[5] : 'Ins_Var', InD.columns[6] : 'Ins_Sub', InD.columns[7] : 'In_Date',
                    InD.columns[8] : 'In_Dep', InD.columns[9] : 'Dis_Date', InD.columns[10] : 'Dis_Dep', InD.columns[11] : 'In_Prd', InD.columns[13] : 'Pay_InsSelf', InD.columns[14] : 'Pay_InsCorp',
                    InD.columns[15] : 'Pay_NoIns', InD.columns[16] : 'Pay_Sel', InD.columns[17] : 'D_Code1', InD.columns[18] : 'D_Name1', InD.columns[19] : 'D_Code2', InD.columns[20] : 'D_Name2',
                    InD.columns[21] : 'D_Code3', InD.columns[22] : 'D_Name3', InD.columns[23] : 'D_Code4', InD.columns[24] : 'D_Name4', InD.columns[25] : 'D_Code5', InD.columns[26] : 'D_Name5',
                    InD.columns[27] : 'D_Code6', InD.columns[28] : 'D_Name6'}, inplace=True)

InS.rename(columns={InS.columns[1] : 'PT_No', InS.columns[2] : 'Birth', InS.columns[3] : 'Gender', InS.columns[4] : 'Address', InS.columns[5] : 'Ins_Var', InS.columns[6] : 'Ins_Sub', InS.columns[7] : 'In_Date',
                    InS.columns[8] : 'In_Dep', InS.columns[9] : 'Dis_Date', InS.columns[10] : 'Dis_Dep', InS.columns[11] : 'In_Prd', InS.columns[13] : 'Pay_InsSelf', InS.columns[14] : 'Pay_InsCorp',
                    InS.columns[15] : 'Pay_NoIns', InS.columns[16] : 'Pay_Sel', InS.columns[17] : 'Sur_Date', InS.columns[18] : 'Prescript_Code', InS.columns[19] : 'Sur_Code', InS.columns[20] : 'Sur_Name'}, inplace=True)

InR.rename(columns={InR.columns[1] : "DRGNO", InR.columns[3] : "Severity", InR.columns[4] : "PT_No", InR.columns[5] : "In_Date"}, inplace=True)

InV.rename(columns={InV.columns[0] : "PACT_ID", InV.columns[1] : "PT_No", InV.columns[2] : "Diag_ID", InV.columns[3] : "SevCode", InV.columns[4] : "VCode", InV.columns[5] : "VCodeSub"}, inplace=True)

'''
print(InD.columns)
print(InS.columns)
print(InR.columns)
'''

## 02. Deleting Unnecessary Columns
'''
print(InD.loc[0:5])
'''

InD['D_Code1'] = InD['D_Code1'].fillna('NoDiag')
InD['D_Code1'] = InD['D_Code1'].astype(str); InD['D_Code1'] = InD['D_Code1'].str.replace('/',',')
InD['D_Name1'] = InD['D_Name1'].fillna('NoDiag')
InD['D_Name1'] = InD['D_Name1'].astype(str); InD['D_Name1'] = InD['D_Name1'].str.replace('/',',')
InD['D_Code2'] = InD['D_Code2'].fillna('NoDiag')
InD['D_Code2'] = InD['D_Code2'].astype(str); InD['D_Code2'] = InD['D_Code2'].str.replace('/',',')
InD['D_Name2'] = InD['D_Name2'].fillna('NoDiag')
InD['D_Name2'] = InD['D_Name2'].astype(str); InD['D_Name2'] = InD['D_Name2'].str.replace('/',',')
InD['D_Code3'] = InD['D_Code3'].fillna('NoDiag')
InD['D_Code3'] = InD['D_Code3'].astype(str); InD['D_Code3'] = InD['D_Code3'].str.replace('/',',')
InD['D_Name3'] = InD['D_Name3'].fillna('NoDiag')
InD['D_Name3'] = InD['D_Name3'].astype(str); InD['D_Name3'] = InD['D_Name3'].str.replace('/',',')
InD['D_Code4'] = InD['D_Code4'].fillna('NoDiag')
InD['D_Code4'] = InD['D_Code4'].astype(str); InD['D_Code4'] = InD['D_Code4'].str.replace('/',',')
InD['D_Name4'] = InD['D_Name4'].fillna('NoDiag')
InD['D_Name4'] = InD['D_Name4'].astype(str); InD['D_Name4'] = InD['D_Name4'].str.replace('/',',')
InD['D_Code5'] = InD['D_Code5'].fillna('NoDiag')
InD['D_Code5'] = InD['D_Code5'].astype(str); InD['D_Code5'] = InD['D_Code5'].str.replace('/',',')
InD['D_Name5'] = InD['D_Name5'].fillna('NoDiag')
InD['D_Name5'] = InD['D_Name5'].astype(str); InD['D_Name5'] = InD['D_Name5'].str.replace('/',',')
InD['D_Code6'] = InD['D_Code6'].fillna('NoDiag')
InD['D_Code6'] = InD['D_Code6'].astype(str); InD['D_Code6'] = InD['D_Code6'].str.replace('/',',')
InD['D_Name6'] = InD['D_Name6'].fillna('NoDiag')
InD['D_Name6'] = InD['D_Name6'].astype(str); InD['D_Name6'] = InD['D_Name6'].str.replace('/',',')

InD.loc[InD.D_Code1 != 'NoDiag', 'D_Code1'] = InD.D_Code1.str[0:4]
InD.loc[InD.D_Code2 != 'NoDiag', 'D_Code2'] = InD.D_Code2.str[0:4]
InD.loc[InD.D_Code3 != 'NoDiag', 'D_Code3'] = InD.D_Code3.str[0:4]
InD.loc[InD.D_Code4 != 'NoDiag', 'D_Code4'] = InD.D_Code4.str[0:4]
InD.loc[InD.D_Code5 != 'NoDiag', 'D_Code5'] = InD.D_Code5.str[0:4]
InD.loc[InD.D_Code6 != 'NoDiag', 'D_Code6'] = InD.D_Code6.str[0:4]


InD['D_Code'] = InD['D_Code1']+'/'+InD['D_Code2']+'/'+InD['D_Code3']+'/'+InD['D_Code4']+'/'+InD['D_Code5']+'/'+InD['D_Code6']
InD['D_Name'] = InD['D_Name1']+'/'+InD['D_Name2']+'/'+InD['D_Name3']+'/'+InD['D_Name4']+'/'+InD['D_Name5']+'/'+InD['D_Name6']

del InD['D_Code1']; del InD['D_Code2']; del InD['D_Code3']; del InD['D_Code4']; del InD['D_Code5']; del InD['D_Code6']
del InD['D_Name1']; del InD['D_Name2']; del InD['D_Name3']; del InD['D_Name4']; del InD['D_Name5']; del InD['D_Name6']
del InD['Ins_Sub']

'''
print(InD.loc[0:5,'D_Code'])
'''

InD.drop('DRGNO', axis=1, inplace=True)
InS.pop('DRGNO')
del InR['수진자명']; del InR['ADRG']; del InR['요양개시일']; del InR['진료과']; del InR['KDRG명칭']; del InR['입원일수']

'''
print(InD.columns)
print(InS.columns)
print(InR.columns)
'''

## 03. Refine InD(Criteria: columns[0:15], Base)
'''
print(len(InD))
'''

InD.drop_duplicates(['PT_No', 'In_Date'], inplace=True)

'''
print(len(InD))
'''
print(InD.columns)


## 04. Combine duplicated from InS(Criteria: ['PT_No', 'In_Date'])
del InS['Birth']; del InS['Gender']; del InS['Address']; del InS['Ins_Var']; del InS['Ins_Sub']; del InS['In_Dep']; del InS['Dis_Date']; del InS['Dis_Dep']
del InS['In_Prd']; del InS['Pay_InsSelf']; del InS['Pay_InsCorp']; del InS['Pay_NoIns']; del InS['Pay_Sel']; del InS['Prescript_Code']

'''
print(InS.loc[0:5, ['PT_No', 'In_Date', 'Sur_Code']])
'''
print(InsPayCri.columns)
InsPayCri.drop(columns=['보험구분', '수가구분', ' 보험단가 ', ' 건강보험수가 ', ' 일반단가 '], inplace=True)
InsPayCri.rename(columns={'수가코드':'Sur_Code', '수가명':'Sur_NameM', 'EDI 대응수가':'Sur_CodeM'}, inplace=True)
InsPayCri['Sur_CodeM'] = InsPayCri['Sur_CodeM'].fillna('GroupPay_SNUCH')
InsPayCri['Sur_CodeM'].replace({'0': 'GroupPay_SNUCH'}, inplace=True)

InsPayCri.reset_index(drop=True, inplace=True)
print(InsPayCri.columns)
InS.reset_index(drop=True, inplace=True)
InS = pd.merge(InS, InsPayCri, on='Sur_Code', how='left')
InS.drop(columns=['Sur_Code', 'Sur_Name'], inplace=True)
InS.rename(columns={'Sur_CodeM':'Sur_Code', 'Sur_NameM':'Sur_Name'}, inplace=True)
InS = InS[['PT_No', 'In_Date', 'Sur_Date', 'Sur_Code', 'Sur_Name']]
InS['Sur_Date'] = InS['Sur_Date'].fillna('NoSur')
InS['Sur_Code'] = InS['Sur_Code'].fillna('NoSur')
InS['Sur_Name'] = InS['Sur_Name'].fillna('NoSur')

print(InS.columns)
print(InS)

InS['Sur_Date'] = InS['Sur_Date'].astype(str); InS['Sur_Date'] = InS['Sur_Date'].str.replace('/',',')
InS['Sur_Code'] = InS['Sur_Code'].astype(str); InS['Sur_Code'] = InS['Sur_Code'].str.replace('/',',')
InS['Sur_Name'] = InS['Sur_Name'].astype(str); InS['Sur_Name'] = InS['Sur_Name'].str.replace('/',',')

InS = InS.groupby(['PT_No', 'In_Date']).agg({'Sur_Date': lambda a: '/'.join(a), 'Sur_Code': lambda b: '/'.join(b), 'Sur_Name': lambda c: '/'.join(c)}).reset_index()


'''
print(InS.loc[0:5, ['PT_No', 'In_Date', 'Sur_Code']])
'''

'''
InS['PT_No'] = InS['PT_No'].astype(str)
print(InS.loc[(InS['PT_No'].str.contains('78671328')), ['In_Date', 'Sur_Code']])
'''
print(InS.columns)


## 05. Combine duplicated from InR(Criteria: ['PT_No', 'In_Date'])
print(InR)

InR['Severity'] = InR['Severity'].map({'전문' : 'Severe', '일반' : 'Normal', '단순' : 'Simple', '분류오류' : 'SortError'})

InR['Severity'] = InR['Severity'].map({'Severe' : 4, 'Normal' : 3, 'Simple' : 2, 'SortError' : 1})

InRsub = InR.copy()
InR = InR.groupby(['PT_No', 'In_Date'], as_index= False)['Severity'].agg(lambda x: x.max())
InR = InR.merge(InRsub, on=['PT_No', 'In_Date', 'Severity'], how='left')
InR.drop_duplicates(subset =['PT_No', 'In_Date', 'Severity'], inplace = True)
InR.reset_index(drop=True, inplace=True)

InR['Severity'] = InR['Severity'].map({4:'Severe', 3:'Normal', 2:'Simple', 1:'SortError'})

print(InR)

#InR['DRGNO'] = InR['DRGNO'].astype(str); InR['DRGNO'] = InR['DRGNO'].str.replace('/',',')
#InR['Severity'] = InR['Severity'].astype(str); InR['Severity'] = InR['Severity'].str.replace('/',',')

#InR = InR.groupby(['PT_No', 'In_Date']).agg({'DRGNO': lambda a: '/'.join(a), 'Severity': lambda b: '/'.join(b)}).reset_index()

print(InR.columns)


## 06. Combine duplicated from InR(Criteria: ['PT_No', 'In_Date'])
print(InV)

print(InV.columns)

InV.fillna('', inplace=True)
InV.loc[(InV.SevCode == 'V193'), 'VCode'] = 'V193'
InV.loc[(InV.VCode == InV.VCodeSub), 'VCodeSub'] = ''
InV.loc[(InV.SevCode == 'V193'), 'SevCode'] = ''
InV.loc[(InV.VCode == ''), 'VCode'] = InV.VCodeSub

del InV['PACT_ID']; del InV['Diag_ID']; del InV['SevCode']; del InV['VCodeSub']

InV.drop_duplicates(['PT_No','VCode'], inplace=True)
InV = InV.groupby('PT_No').agg({'VCode': lambda a: '/'.join(a)}).reset_index()

InV.rename(columns={'VCode':'Ins_Sub'}, inplace=True)

print(InV)



## 07. Combine InD, InD, InR(Criteria: ['PT_No', 'In_Date'])
'''
InD['PT_No'] = InD['PT_No'].astype(str)
print(InD.loc[(InD['PT_No'].str.contains('71255934')), 'D_Code'])
InR['PT_No'] = InR['PT_No'].astype(str)
print(InR.loc[(InR['PT_No'].str.contains('71255934')), 'DRGNO'])
'''

'''
print(len(InD))
'''
InDR = pd.merge(InD, InR, how='left', on=['PT_No', 'In_Date'])
InDR['DRGNO'] = InDR['DRGNO'].fillna('NoDRG')
InDR['Severity'] = InDR['Severity'].fillna('NoDRG')
'''
print(len(InDR))
'''

'''
print(InDR.loc[0:5, ['PT_No', 'In_Date', 'D_Code', 'DRGNO']])
print(InDR.columns)
'''

'''
print(len(InDR))
'''
InDRS = pd.merge(InDR, InS, how='left', on=['PT_No', 'In_Date'])
InDRS['Sur_Date'] = InDRS['Sur_Date'].fillna('NoSur')
InDRS['Sur_Code'] = InDRS['Sur_Code'].fillna('NoSur')
InDRS['Sur_Name'] = InDRS['Sur_Name'].fillna('NoSur')
'''
print(len(SNUCHIn))
'''

SNUCHIn = pd.merge(InDRS, InV, how='left', on='PT_No')

SNUCHIn.drop_duplicates(['PT_No', 'In_Date'], inplace=True)
SNUCHIn.reset_index(drop=True, inplace=True)


SNUCHIn['Address'] = SNUCHIn['Address'].fillna('NoAdd')
SNUCHIn['Ins_Var'] = SNUCHIn['Ins_Var'].fillna('NoInsVar')
SNUCHIn['Ins_Sub'] = SNUCHIn['Ins_Sub'].fillna('NoVCode')
SNUCHIn['In_Prd'] = SNUCHIn['In_Prd'].fillna(0)
SNUCHIn['Pay_InsSelf'] = SNUCHIn['Pay_InsSelf'].fillna(0)
SNUCHIn['Pay_InsCorp'] = SNUCHIn['Pay_InsCorp'].fillna(0)
SNUCHIn['Pay_NoIns'] = SNUCHIn['Pay_NoIns'].fillna(0)
SNUCHIn['Pay_Sel'] = SNUCHIn['Pay_Sel'].fillna(0)

'''
print(SNUCHIn.loc[0:5, ['PT_No', 'Ins_Var', 'D_Code', 'DRGNO', 'Sur_Code']])
print(SNUCHIn.columns)
'''


## 08. Refine SNUCHIn
'''
print(len(SNUCHIn))
'''
SNUCHIn.dropna(how='any')
'''
print(len(SNUCHIn))
'''

SNUCHIn['PT_No'] = SNUCHIn['PT_No'].astype(str)
SNUCHIn['Birth'] = SNUCHIn['Birth'].astype(str); SNUCHIn['Birth'] = SNUCHIn['Birth'].str.replace('-',''); SNUCHIn['Birth'] = SNUCHIn['Birth'].astype(int)
SNUCHIn['Gender'] = SNUCHIn['Gender'].astype(str)
SNUCHIn['Address'] = SNUCHIn['Address'].astype(str)
SNUCHIn['Ins_Var'] = SNUCHIn['Ins_Var'].astype(str)
SNUCHIn['Ins_Sub'] = SNUCHIn['Ins_Sub'].astype(str)
SNUCHIn['In_Date'] = SNUCHIn['In_Date'].astype(str); SNUCHIn['In_Date'] = SNUCHIn['In_Date'].str.replace('-',''); SNUCHIn['In_Date'] = SNUCHIn['In_Date'].astype(int)
SNUCHIn['In_Dep'] = SNUCHIn['In_Dep'].astype(str)
SNUCHIn['Dis_Date'] = SNUCHIn['Dis_Date'].astype(str); SNUCHIn['Dis_Date'] = SNUCHIn['Dis_Date'].str.replace('-',''); SNUCHIn['Dis_Date'] = SNUCHIn['Dis_Date'].astype(int)
SNUCHIn['Dis_Dep'] = SNUCHIn['Dis_Dep'].astype(str)
SNUCHIn['In_Prd'] = SNUCHIn['In_Prd'].astype(int)
SNUCHIn['Pay_InsSelf'] = SNUCHIn['Pay_InsSelf'].astype(int)
SNUCHIn['Pay_InsCorp'] = SNUCHIn['Pay_InsCorp'].astype(int)
SNUCHIn['Pay_NoIns'] = SNUCHIn['Pay_NoIns'].astype(int)
SNUCHIn['Pay_Sel'] = SNUCHIn['Pay_Sel'].astype(int)
SNUCHIn['D_Code'] = SNUCHIn['D_Code'].astype(str)
SNUCHIn['D_Name'] = SNUCHIn['D_Name'].astype(str)
SNUCHIn['DRGNO'] = SNUCHIn['DRGNO'].astype(str)
SNUCHIn['Severity'] = SNUCHIn['Severity'].astype(str)
SNUCHIn['Sur_Date'] = SNUCHIn['Sur_Date'].astype(str)
SNUCHIn['Sur_Code'] = SNUCHIn['Sur_Code'].astype(str)
SNUCHIn['Sur_Name'] = SNUCHIn['Sur_Name'].astype(str)

# 08-1) Change values in 'Gender' Column ( '1' : Male, '2' : Female)
SNUCHIn.Gender.replace(['1', '2'], ['Male', 'Female'], inplace=True)

# 08-2) Create 'Age' Column
# df.loc[df['column name'] condition, 'new column name'] = 'value if condition is met'
SNUCHIn.loc[(SNUCHIn['In_Date'] % 10000 <= SNUCHIn['Birth'] % 10000) & (SNUCHIn['In_Date'] // 10000 >= SNUCHIn['Birth'] // 10000), 'Age'] = (SNUCHIn['In_Date'] - SNUCHIn['Birth'])// 10000
SNUCHIn.loc[(SNUCHIn['In_Date'] % 10000 > SNUCHIn['Birth'] % 10000) & (SNUCHIn['In_Date'] // 10000 == SNUCHIn['Birth'] // 10000), 'Age'] = (SNUCHIn['In_Date'] - SNUCHIn['Birth'])// 10000
SNUCHIn.loc[(SNUCHIn['In_Date'] % 10000 > SNUCHIn['Birth'] % 10000) & (SNUCHIn['In_Date'] // 10000 > SNUCHIn['Birth'] // 10000), 'Age'] = (SNUCHIn['In_Date'] - SNUCHIn['Birth'])// 10000 - 1


SNUCHIn['Age'] = SNUCHIn['Age'].astype(int)


print(SNUCHIn.columns)
print(SNUCHIn['Age'])



# 08-3) Rephrase 'Address' Values
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

SNUCHIn['Address'] = SNUCHIn['Address'].apply(lambda x: address_map[x])


SNUCHIn['Address'] = SNUCHIn['Address'].fillna('NoAdd')

'''
print(SNUCHIn['Address'].value_counts())
print(SNUCHIn['Address'])
'''


# 08-4) Rephrase 'Ins_Var' Values
'''
print(SNUCHIn['Ins_Var'].value_counts())
'''

SNUCHIn['Ins_Var'] = SNUCHIn['Ins_Var'].map({'국민건강보험공단' : 'NHIS', '의료급여1종' : 'MedCareT1', '의료급여2종' : 'MedCareT2', '의료급여장애인' : 'MedCareDis'})
SNUCHIn['Ins_Var'] = SNUCHIn['Ins_Var'].fillna('Others')


'''
print(SNUCHIn['Ins_Var'].value_counts())
print(SNUCHIn['Ins_Var'])
'''



# 08-5) Change Order of Columns


SNUCHIn['InstName'] = np.nan
SNUCHIn['InstName'] = SNUCHIn['InstName'].fillna('SNUCH')
SNUCHIn = SNUCHIn[['InstName', 'PT_No', 'Birth', 'Age', 'Gender', 'Address', 'Ins_Var', 'Ins_Sub', 'In_Date', 'In_Dep', 'Dis_Date', 'Dis_Dep', 'In_Prd', 'Pay_InsSelf',
                   'Pay_InsCorp', 'Pay_NoIns', 'Pay_Sel', 'D_Code', 'D_Name', 'DRGNO', 'Severity', 'Sur_Date', 'Sur_Code', 'Sur_Name']]




print(SNUCHIn.columns)
print(SNUCHIn.info)



### Save as CSV in '/Users/peterpark/Desktop/PY_START/SNUH_Project01'

SNUCHIn.to_csv('./SNUCH/SNUCHInP_R4A.csv', index=False)


