#importing sql libraries
import sqlalchemy, os
import pandas as pd
from dev import setup as setting
from pprint import pprint

#Setting up variables that wil be used throughout the script
directory = setting.directory #file directory
server = setting.server #Database Server
database = setting.database #Name of Database
Trusted_Connection = setting.Trusted_Connection
schema = setting.schema #Database Schema

engine = sqlalchemy.create_engine('mssql+pyodbc://' + server + '/' + database + '?driver=SQL+Server') #Create a database object 

def dataTypeProcessor(file,dictionary,i,dfColumn): # Fuction that defines how SQL datatypes in sql alchemy
    if file["Datatype"][i] == "Varchar":
        dictionary[dfColumn] = sqlalchemy.types.VARCHAR(length=int(file["Size"][i]))
    elif file["Datatype"][i] == "Int":
        dictionary[dfColumn] = sqlalchemy.types.INT
    else:
        pprint("Invalid")

def csvToSQL(isCustom,filePath): #Function that creates CSV to SQL Table
    dataTypeDict = {} #Empty variable that will contain a dictionary that contains key value pairs of Columns Name and Datatype 
    iterator = 0 #Simple counter for loop. 

    for file in os.listdir(directory): #Loop that will process each file in the specified directory
        data = pd.read_csv (directory + "\\" + file) #Import CSV
        df = pd.DataFrame(data) #assign pandas dataframe to variable "df"
        #df = df.replace(np.nan, 0)
        numOfColumns = (len(df.columns)) #Asigns the number of columns in the data frame to the variable "numOfColumns"
        
        if isCustom != True: #If isCustom does not euqal true then the program will default to assign data to varchar
            try:
                if df.size < 100000:
                    samplesize = 1
                else:
                    samplesize = 100000/int(df.size) 
                maxLength  = int(df.sample(frac = samplesize).to_numpy(dtype = str, na_value="X").dtype.itemsize)*3
            except:
                print("There was some error set the varchar length to 4000")
                maxLength = 4000

            for column in df.columns:
                dataTypeDict[column] = sqlalchemy.types.VARCHAR(length=maxLength) #For each column in dataframe asign the dictionary value a varchar with a maxmium length

        else: #If is Customer Is "True"
            dataTypeFile = pd.read_csv(filePath) #Read the customer data types in the CSV file
            if dataTypeFile.shape[0] == numOfColumns: #If the number of columns is equal to the number of columns in the dataframe continue
                while iterator < numOfColumns: #Perform a loop iteration for each column as long as there are columns to iterate through
                    for column in df.columns: #for each column in the data frame perform the "DataTypeProcessor" function
                        dataTypeProcessor(dataTypeFile,dataTypeDict,iterator,column)
                        iterator = iterator + 1 #Process the next column
            else:
                pprint("Please check the number of columns in the specified spreadsheet. They do not match the number of columns in the CSV you want to import to SQL Server")
                break #End Program
        
        tableName = str(str(file).replace(".csv","")) #Create Table Name
    
        try: #Some simple error handling that handles incorrect database values such as schema, database name, etc...
            df.to_sql(tableName, con = engine, if_exists='replace', index = False, dtype=dataTypeDict, schema=schema) #Insert Dataframe into SQL Server. If the Table exisit alreadt it will replace.
        except sqlalchemy.exc.ProgrammingError: #If a programming Error is found then print and quit program
            pprint("There is likely a problem with the settings entered. Please double check server permissions and specified database settings.")
            break # End Program
    
        pprint(engine.execute(f"SELECT TOP (5) * FROM {tableName}").fetchall()) #show the sample data from Employee_Data table
        pprint("Job Complete") #Print Completion Message

csvToSQL(False,"datatypeSheet.csv") #Run the program

#TODO Do something about encoding
# More error handing
#Performance and Unit testing??
#implement cmd interface
#Add support for Excel and Google Sheets