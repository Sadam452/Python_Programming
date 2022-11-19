#This python program loads the data into state table from Us_state.csv
# import pandas as pd
# sData = pd.read_csv('US_state.csv',index_col=False)
import csv
sData = csv.reader(open("US_state.csv","r"))
import cx_Oracle as orcCon
from cx_Oracle import DatabaseError

lines =[]
for line in sData:
    lines.append(line)
print(lines)

# try:
#     #orcCon.connect('username/password@localhost')
conn = orcCon.connect('SYSTEM/test')
cursor = conn.cursor()
if conn:
    
    print("cx_Oracle version:", orcCon.version)
    print("Database version:", conn.version)
    print("Client version:", orcCon.clientversion())
    cursor = conn.cursor()
    print("You're connected: ")
    print('Inserting data into table....')
    # for i,row in sData.iterrows():
    i=0
    for line in lines:
        print("inserting ,",line)
        i+=1
        if i==1:
            continue;
        sql = "insert into state(State_id,State,Abbreviation,YearOfStatehood,Capital,CapitalSince,LandArea,IsPopulousCity,MunicipalPopulation,MetroPopulation) values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)"
        try:
            cursor.execute(sql, line)
        except DatabaseError as e:
            print(e)
    # the connection is not autocommitted by default, so we must commit to save our changes
    conn.commit()
    print("Record inserted succesfully")
# except DatabaseError as e:
#     err, = e.args
#     print("Oracle-Error-Code:", err.code)
#     print("Oracle-Error-Message:", err.message)
# finally:
cursor.close()
conn.close()
