#importing sql library
import sqlalchemy, os
import pandas as pd
from dev import settings as setting


from sqlalchemy.sql.sqltypes import VARCHAR

directory = setting.directory
server = setting.server
database = setting.database
Trusted_Connection = setting.Trusted_Connection
schema = setting.schema

engine = sqlalchemy.create_engine('mssql+pyodbc://' + server + '/' + database + '?driver=SQL+Server')


def csvToSQL(isCustom,filePath):
    dataTypeDict = {}
    i = 0

    for file in os.listdir(directory):
        # Import CSV
        data = pd.read_csv (directory + "\\" + file)   
        df = pd.DataFrame(data)
        #df = df.replace(np.nan, 0)
        numOfClolumnsInData = (len(df.columns))
        
        if isCustom != True:
            for column in df.columns:
                dataTypeDict[column] = sqlalchemy.types.VARCHAR(length=255)

        else:
            dataTypeFile = pd.read_csv(filePath)
            print(len(dataTypeFile.columns))
            if dataTypeFile.shape[0] == numOfClolumnsInData:
                while i < numOfClolumnsInData:
                    for column in df.columns:
                        if dataTypeFile["Datatype"][i] == "Varchar":
                            dataTypeDict[column] = sqlalchemy.types.VARCHAR(length=int(dataTypeFile["Size"][i]))
                        elif dataTypeFile["Datatype"][i] == "Int":
                            dataTypeDict[column] = sqlalchemy.types.INT
                        else:
                            print("Invalid")
                        i = i + 1
            else:
                print("Please check the number of columns in the specified spreadsheet. They do not match the number of columns in the CSV you want to import to SQL Server")
                break
                
        print(dataTypeDict)
        # Create Table
        tableName = str(str(file).replace(".csv",""))
    
        #Insert Dataframe into SQL Server. If the Table exisit alreadt it will replace. 
        df.to_sql(tableName, con = engine, if_exists='replace', index = False, dtype=dataTypeDict, schema=schema)
    
        # show the complete data
        # from Employee_Data table
        print(engine.execute(f"SELECT TOP (5) * FROM {tableName}").fetchall())

csvToSQL(True,"datatypeSheet.csv")