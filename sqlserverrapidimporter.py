#importing sql library
import sqlalchemy, os
import pandas as pd
from dev import setup as setting
from pprint import pprint


from sqlalchemy.sql.sqltypes import VARCHAR

directory = setting.directory
server = setting.server
database = setting.database
Trusted_Connection = setting.Trusted_Connection
schema = setting.schema

engine = sqlalchemy.create_engine('mssql+pyodbc://' + server + '/' + database + '?driver=SQL+Server')

def dataTypeProcessor(file,dictionary,i,dfColumn):
    if file["Datatype"][i] == "Varchar":
        dictionary[dfColumn] = sqlalchemy.types.VARCHAR(length=int(file["Size"][i]))
    elif file["Datatype"][i] == "Int":
        dictionary[dfColumn] = sqlalchemy.types.INT
    else:
        pprint("Invalid")

def csvToSQL(isCustom,filePath):
    dataTypeDict = {}
    iterator = 0

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
            if dataTypeFile.shape[0] == numOfClolumnsInData:
                while iterator < numOfClolumnsInData:
                    for column in df.columns:
                        dataTypeProcessor(dataTypeFile,dataTypeDict,iterator,column)
                        iterator = iterator + 1
            else:
                pprint("Please check the number of columns in the specified spreadsheet. They do not match the number of columns in the CSV you want to import to SQL Server")
                break
                
        #print(dataTypeDict)
        # Create Table
        tableName = str(str(file).replace(".csv",""))
    
        #Insert Dataframe into SQL Server. If the Table exisit alreadt it will replace.
        try:
            df.to_sql(tableName, con = engine, if_exists='replace', index = False, dtype=dataTypeDict, schema=schema)
        except sqlalchemy.exc.ProgrammingError:
            pprint("There is likely a problem with the settings entered. Please double check server permissions and specified database settings.")
            break
    
        # show the complete data
        # from Employee_Data table
        pprint(engine.execute(f"SELECT TOP (5) * FROM {tableName}").fetchall())
        pprint("Job Complete")

csvToSQL(True,"datatypeSheet.csv")