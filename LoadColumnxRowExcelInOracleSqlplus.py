#This python program loads the data into confirmed_cases table from Us_confirmed_cases.csv
import csv
sData = csv.reader(open("Us_confirmed_cases.csv","r"))
import cx_Oracle as orcCon
from cx_Oracle import DatabaseError

lines =[]
for line in sData:
    lines.append(line)
# print(lines)

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
    
    first_row = lines[0]
    second_row = lines[1]
    for i in range(1,len(first_row)):
        data = []
        state = first_row[i]
        country = second_row[i]
        data.append(state)
        data.append(country)
        if(country == "Unassigned"):
            break
        j=0
        for line in lines:
            j+=1
            if j<=2:
                continue;
            print("data = ",data)
            data_new = []
            data_new.extend(data)      
            data_new.append(line[0])
            data_new.append(line[i])
            print(data_new)
            sql = "insert into confirmed_cases(State,Country,TestDate,PositiveCount) values(:1,:2,:3,:4)"
            try:
                cursor.execute(sql, data_new)
            except DatabaseError as e:
                print(e)
    conn.commit()
    print("Record inserted succesfully")
cursor.close()
conn.close()
