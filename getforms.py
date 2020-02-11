import pandas as pds
import pymysql
import pyodbc
import psycopg2
#miscellaneous extra goodies for psycopg2
import psycopg2.extras
import os
import sqlalchemy as np
from datetime import datetime
x = datetime.today().strftime('%Y%m%d')
# Uses Windows authentication on your laptops

connection = pyodbc.connect('Driver={SQL Server};'
                       'Server=5CG7086ZJQ\SQLEXPRESS;'
                       'Database=datadictionary;'                       
                       'Trusted_Connection=yes;')
                       

cursor = connection.cursor()
csvDF1 = {'column_name': [],
         'question_text': [],
         'question_description': [],        
         'valid_value': [], 
         'export_value': [],       
         'data_type': [],
         'field_format': [],         
         'field_size': [],
         'logic': [],
         'min_value': [],
         'max_value': [],
         'max_length': []}
csvDF2 = {'form_type': [],
         'form_id': [],
         'form_version':[],
         'web_label': [],
         'export_table1': [],
         'export_table2': []
         }
#created tableName variable which gets filled in by the input
tableName = input("Please enter form name: ")
version = input("Please enter form version: ")
#select statement from table that was choosen by the input statement 
query = ("""
SELECT  
    column_name,    
    data_type,   
    CASE DATA_TYPE WHEN  'date'     THEN 10
                   WHEN  'varchar'  THEN CHARACTER_MAXIMUM_LENGTH
                   WHEN  'char'     THEN CHARACTER_MAXIMUM_LENGTH
                   WHEN  'decimal'  THEN 10                  
   END   
FROM 
  INFORMATION_SCHEMA.COLUMNS
WHERE    
   table_name = '%s';""" % tableName)

#runs the select statement
cursor.execute(query)
#fetches the data from the table
tables = cursor.fetchall()

#adding relatable column headers to our csvDF
#loop to get all the data requested in the select and adds it to dataframe we call csvDF
for column in tables:
    csvDF1['column_name'].append(column[0]) 
    csvDF1['data_type'].append(column[1]) 
    csvDF1['field_size'].append(column[2]) 
    #sql to get minimum value from each column and formatted to get only what we need
    min_query = ("""select min(%s) from %s""") % (column[0], tableName)
    cursor.execute(min_query)
    mins = cursor.fetchall()
    #adding it to csvDF and formatted  ([0][0] given a list of lists value in the list at the 0 index and 0 value of 2nd list)
    csvDF1['min_value'].append(mins[0][0])
    #sql to get maximum value from each field and formatted to only get what we need
    max_query = ("""select max(%s) from %s""") % (column[0], tableName)
    cursor.execute(max_query)  
    maxes = cursor.fetchall()
    csvDF1['max_value'].append(maxes[0][0])
#sql to get the maximum number of characters in the field and formated
    maxlength_query = ("""
    select 
        len(cast(max(%s) AS VARCHAR)) 
    from 
         %s""") % (column[0], 
        tableName)
    cursor.execute(maxlength_query)
    maxlengths = cursor.fetchall()
    csvDF1['max_length'].append(maxlengths[0][0])
joinQuery = (""" 
SELECT 
    teleforms.formlabel,
    teleforms.form_id,
    teleforms.formver,        
    teleforms.web_label, 
    teleforms.datatable1,
    teleforms.datatable2
FROM 
    teleforms 
INNER JOIN %s ON CAST(teleforms.form_id AS VARCHAR) = CAST(%s.form_id AS VARCHAR)
WHERE
    teleforms.formver = '%s';
""" % (tableName, tableName,version))
cursor.execute(joinQuery)
joinResults = cursor.fetchmany() 
for data in joinResults:
    csvDF2['form_type'].append(data[0])    
    csvDF2['form_id'].append(data[1]) 
    csvDF2['form_version'].append(data[2])  
    csvDF2['web_label'].append(data[3])
    csvDF2['export_table1'].append(data[4])
    csvDF2['export_table2'].append(data[5])        
max_length = 1
for k, v in csvDF1.items():
  if len(v) > max_length:
    max_length = len(v)
for k, v in csvDF1.items():
  if len(v) == 0:
    csvDF1[k] = ["" for i in range(max_length)]
  elif len(v) != max_length:
    v += ["" for i in range(max_length, max_length - len(v), 1)]

#adding all the data to the dataframe called df
df1 = pds.DataFrame(csvDF1)
df2 = pds.DataFrame(csvDF2)
#create a helper row to connect the two dataframes to start on the same row
id=0
for i in df1.index:
    df1.loc[i,'id']=id
    id=id
for i in df2.index:
    df2.loc[i,'id']=id
    id=id+1
df3=df2.merge(df1,on='id',how='outer', sort=False)

#getting the filename with the tablename from the input 
fileName = tableName
#variable to hold the path to where the csv file will go 
path ='DataTables'
#if not os.path.exists(tableName):
try:
    dirName =  path + '/' + fileName   
    os.makedirs(dirName)
    export_csv = df3.to_csv(dirName  + '/' + fileName + '_' + x + '.csv', index=None, header=True)
except FileExistsError:
    path =  path + '/' + fileName
    export_csv = df3.to_csv(path  + '/' + fileName + '_' + x + '.csv', index=None, header=True)

# Pulling out individual elements from each row 
# inserting into the DB

sql="""INSERT INTO dbo.forminfo(
        [index],
        [form_type], 
        [form_id],
        [form_version],
        [web_label], 
        [export_table1],
        [export_table2],
        [column_name], 
        [question_text],
        [question_description],
        [valid_value],
        [export_value],
        [data_type],
        [field_format],
        [field_size],
        [logic],
        [min_value],
        [max_value],
        [max_length]) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
for index, row in df3.iterrows(): 
    row = row.astype(str)        
    data =(index,row['form_type'],row['form_id'],row['form_version'],row['web_label'],row['export_table1'],
           row['export_table2'],row['column_name'],row['question_text'],row['question_description'],
           row['valid_value'],row['export_value'],row['data_type'],row['field_format'],row['field_size'],
           row['logic'],row['min_value'],row['max_value'],row['max_length'])
    
    cursor.execute(sql,data)
                                                                                        
connection.commit()
cursor.close()
connection.close() 