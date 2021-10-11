#importing sql libraries
from re import L
import sqlalchemy, os, mimetypes, psutil
import pandas as pd
from dev import setup as setting
from charset_normalizer import detect
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

def fileValidation(folderPath):
    #File/Directory Validation
    avaliableRam = float(psutil.virtual_memory()[1])*.95
    totalSize = 0
    if os.path.isfile(folderPath) and mimetypes.guess_type(folderPath)[0] == 'text/csv' and folderPath[-4:] == '.csv' or folderPath[-4:] == '.txt': #Determines if is a file and if the mimetype and file extention is of a text format.
        if (os.path.getsize(folderPath)*2) < avaliableRam: #checks to see if the file size, doubled, is less than the currently aviable Ram. This is done because Pandas loads all data into memory and I read that you will need double the space of the orginal file.
            files = [folderPath] #puts file in a list to be passed through a loop in the next step
            return files
        else:
            print("You do not have enough memory. Will need to split file.")
            return
    elif os.path.isdir(folderPath):
        csvs = [os.path.abspath(folderPath) + "\\" + file for file in os.listdir(directory) if file[-4:] == '.csv' or file[-4:] == '.txt' in str(file)] #Determines if is a file and if the mimetype and file extention is of a text format.
        validcsvs = [csvFile for csvFile in csvs if mimetypes.guess_type(csvFile)[0] == 'text/csv'] #of the files that have csv extentions which ones have a consistent mimetype
        for csv in validcsvs:
            totalSize += os.path.getsize(csv)
        if totalSize*2 < avaliableRam: #checks to see if the file size, doubled, is less than the currently aviable Ram. This is done because Pandas loads all data into memory and I read that you will need double the space of the orginal file. Also Python is bad at giving ram back to the OS.
            files = validcsvs
            return files
        else:
            print("You do not have enough memory to process all of the files. Will need to get creative and spit files.")
    else:
        print("Please Provide a valid file in the \".csv\" format.")

def csvToSQL(isCustom,folderPath): #Function that creates CSV to SQL Table
    dataTypeDict = {} #Empty variable that will contain a dictionary that contains key value pairs of Columns Name and Datatype 
    iterator = 0 #Simple counter for loop. 
    
    def guessEncoding(csvFile, tableName):
        with open(csvFile,'rb') as f:
            data = f.read(1000000)
            encoding=detect(data).get("encoding")
            print(f"The guessed encoding for \"{tableName}\" is {encoding}.")
        return encoding

    files = fileValidation(folderPath)

    for file in files: #Loop that will process each file in the specified directory
        tableName = str(str(os.path.basename(file)).replace(".csv","")) #Create Table Name
        #if file extention is valid
        data = pd.read_csv (file, encoding=guessEncoding(file,tableName)) #Import CSV
        df = pd.DataFrame(data) #assign pandas dataframe to variable "df"
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

        else: #If is Custom Is "True"
            dataTypeFile = pd.read_csv(folderPath, encoding=guessEncoding(file,tableName)) #Read the customer data types in the CSV file
            if dataTypeFile.shape[0] == numOfColumns: #If the number of columns is equal to the number of columns in the dataframe continue
                while iterator < numOfColumns: #Perform a loop iteration for each column as long as there are columns to iterate through
                    for column in df.columns: #for each column in the data frame perform the "DataTypeProcessor" function
                        dataTypeProcessor(dataTypeFile,dataTypeDict,iterator,column)
                        iterator = iterator + 1 #Process the next column
            else:
                pprint("Please check the number of columns in the specified spreadsheet. They do not match the number of columns in the CSV you want to import to SQL Server")
                break #End Program
    
        try: #Some simple error handling that handles incorrect database values such as schema, database name, etc...
            df.to_sql(tableName, con = engine, if_exists='replace', index = False, dtype=dataTypeDict, schema=schema) #Insert Dataframe into SQL Server. If the Table exisit alreadt it will replace.
        except sqlalchemy.exc.ProgrammingError: #If a programming Error is found then print and quit program
            pprint("There is likely a problem with the settings entered. Please double check server permissions and specified database settings.")
            break # End Program
    
        pprint(engine.execute(f"SELECT TOP (5) * FROM [{tableName}]").fetchall()) #show the sample data from Employee_Data table
        pprint("Job Complete") #Print Completion Message

csvToSQL(False,r'G:\SampleDataset\samples\pkmn') #Run the program

#TODO Do something about encoding
# split out validation to seperate function. 
# More error handing
# How to efficelty spit date to load into database
#determine usage of bulk load
#add timer
#Performance and Unit testing??
#implement cmd interface
#Add support for Excel and Google Sheets
#add customer logic