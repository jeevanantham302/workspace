import pandas as pd
import numpy as np
! pip install python-Levenshtein##Optional
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import itertools


dataset1_FilePath=r"path\..\dataset1.xls"#user input needed
dataset2_FilePath=r"path\..\dateset2.xls"#user input needed
dataset1_key='primary_identity_ID'#user input needed for joining across dataset(unique values and common between datasets)
dataset2_key='primary_identity_ID'#user input needed for joining across dataset(unique values and common between datasets)
output_path=r"path\..\Output.xls"#user input needed


"""Loading excel into dataframe"""

dataset1 = pd.read_excel(dataset1_FilePath, sheet_name='qv')
dataset2 = pd.read_excel(dataset2_FilePath, sheet_name='fdp')

"""Getting schema into Dictionary for further processing"""
colindex={'dataset1':list(dataset1.columns),'dataset2':list(dataset2.columns)}


""" Trying to match the schema between two dataframe(dataset) using famous Fuzzy String Matching

Note: based on cloumns name this matching will be accurate.
        for more accurate matching, then give similer/closer column name
"""

index_ds2=[]
tem=0
for i in colindex['dataset1']:
    tem=0
    tem1=''
    for j in colindex['dataset2']:             
        if (fuzz.ratio(i.lower(),j.lower())>tem and fuzz.ratio(i.lower(),j.lower())>80):#can change the accuracy limit by default 80 for more accurate 100
            tem=fuzz.ratio(i.lower(),j.lower())
            tem1=j
    index_ds2.append(tem1)
colindex['dataset2']=index_ds2 

"""Final outcome dict have only valid matching of column names in dataset2 key else null as value"""

"""Remove non matching column names"""
for i,j,l in zip(colindex['dataset1'],colindex['dataset2'],range(0,len(colindex['dataset2']))):
    if(len(colindex['dataset2'][l])==0):
        colindex['dataset1'].pop(l)
        colindex['dataset2'].pop(l)

"""finding the position/index of duplicate column name matching within dataset2 itself"""

s={}
for item in colindex['dataset2']:
    indexes=[i for i, j in enumerate(colindex['dataset2']) if j == item]
    if len(indexes)>1:
        s[item]=(indexes)

"""Splitting duplicate column name into different set of list for looping"""
pairs = [   (key, value) 
            for key, values in s.items() 
            for value in values ]

"""renaming column name in dataframe2 , Handling duplicate of matching to avoid error in dataframe"""
for i,j in pairs:
    dataset2[str(i)+'_dup'+str(j)]=dataset2[i]
    colindex['dataset2'][j]=str(str(i)+'_dup'+str(j))

"""Now matched and sorted the schema across the dataframe"""
dataset1=dataset1[colindex['dataset1']]
dataset2=dataset2[colindex['dataset2']]


"""Preparing the dataframe by renaming, sorting , merging and defining finalized schema in easy readable format"""
dataset1_col = { i : str(i)+"_dataset1" for i in colindex['dataset1'] } 
dataset2_col = { i : str(i)+"_dataset2" for i in colindex['dataset2'] } 
dataset1.rename(columns=dataset1_col,inplace=True)
dataset2.rename(columns=dataset2_col,inplace=True)
colindex={'dataset1':list(dataset1.columns),'dataset2':list(dataset2.columns)}
df1=dataset1.merge(dataset2, how='outer', left_on=[dataset1_key+'_dataset1'], right_on=[dataset2_key+'_dataset2'])
schema_tup = [(i,j) for i,j in zip(colindex['dataset1'],colindex['dataset2'] )]
schema = [item for t in schema_tup for item in t]
df_final=df1[schema]
df_final=df_final.astype(str)

data={}
res=[]
"""matching the data in row level and creating the matching outcome and fuzzy match ratio as a new column"""
for i,j in schema_tup:
    df_final[i].fillna('Nan',inplace=True)
    df_final[j].fillna('Nan',inplace=True)
    com='False'
    unique, counts = np.unique(np.where(df_final[i] == df_final[j], 'True', 'False'), return_counts=True)
    if 'False' in unique:
        com='False'
    else:
        com='True'
    res.append((str(i),str(j),com,dict(zip(unique, counts)))) 
    data[str(i)]=df_final[i]
    data[str(j)]=df_final[j]
    data[str(j)+'_isMatch']=np.where(df_final[i] == df_final[j], 'True', 'False')
    data[str(j)+'_fuzzyRatio']=df_final.apply(lambda x: fuzz.ratio(x[i],x[j]),axis=1)

#Matched summary
result = pd.DataFrame(res,columns=['dataset1_columnName','dataset2_columnName','isMatch','diffStat'])
#complete dataset post comparing
result2 = pd.DataFrame(data)


"""picking not matched data in row level of sample 10 records for quick analysis 
which has top5 and bottom5 fuzzy matched ratio"""
j=0
sampling = pd.DataFrame(columns=( dataset1_key+'_dataset1',dataset2_key+'_dataset2','dataset1_value','dataset2_value','is_match','FuzzyRatio','dataset1_columnName', 'dataset2_columnName'))
print(len(result2.columns))
col=list(result2.columns)
for i in range(0,int(len(result2.columns)/4)):
    r1=pd.DataFrame()
    r2=pd.DataFrame()
    r=pd.DataFrame()
    if(dataset1_key+'_dataset1'!=col[j] or dataset2_key+'_dataset2'!=col[j+1]):
        r=result2[[dataset1_key+'_dataset1',dataset2_key+'_dataset2',col[j],col[j+1],col[j+2],col[j+3]]]
        r1=r[r[col[j+2]]=='False'].sort_values(col[j+3]).head(5)
        r2=r[r[col[j+2]]=='False'].sort_values(col[j+3]).tail(5)
        r1['dataset1_columnName']=[col[j]]*len(r1)
        r1['dataset2_columnName']=[col[j+1]]*len(r1)
        r2['dataset1_columnName']=[col[j]]*len(r2)
        r2['dataset2_columnName']=[col[j+1]]*len(r2)
        r1=r1.append(r2,ignore_index = True)
        r1.rename(columns = {col[j]:'dataset1_value',col[j+1]:'dataset2_value',col[j+2]:'is_match',col[j+3]:'FuzzyRatio'}, inplace = True)
        sampling=sampling.append(r1,ignore_index = True)
    j=j+4
    
"""writing outcome into excel"""
with pd.ExcelWriter(output_path) as writer:  
    result.to_excel(writer, sheet_name='Overall_summary')
    result2.to_excel(writer, sheet_name='Data_wise_match')
    sampling.to_excel(writer, sheet_name='sample_data')
