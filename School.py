# NiyamIT
# COLIN LINDEMAN, GIS Developer
# Proof of Concept - HIFLD Public Schools, Private Schools, Colleges and Universities, Supplemental Colleges into HAZUS School.
# Last Update: 2019-04-18
# Requirements:
#    Python 2.7, pyodbc
#    SQL Server 12.0.4237
#    HAZUS be installed and config.ini updated with username and password;
#    See readme file.
#
# Tips on HAZUS DB
#    cdms_p_Counties has State and County names and FIPS


# Import necessary modules
import os
import tempfile
import urllib
import csv
import pyodbc
import ConfigParser
import codecs


print "Read config.ini file..."
# User defined variables from .ini file...
# User needs to change config path
configPath = ".\config.ini"
cfgParser = ConfigParser.ConfigParser()
cfgParser.read(configPath)
url = cfgParser.get("HIFLD OPEN DATA URLS", "School_URL")
url2 = cfgParser.get("HIFLD OPEN DATA URLS", "School2_URL")
url3 = cfgParser.get("HIFLD OPEN DATA URLS", "School3_URL")
url4 = cfgParser.get("HIFLD OPEN DATA URLS", "School4_URL")
userDefinedServer = cfgParser.get("SQL SERVER", "ServerName")
UserName = cfgParser.get("SQL SERVER", "UserName")
Password = cfgParser.get("SQL SERVER", "Password")
possibleDatabaseListRaw = cfgParser.get("DATABASE LIST", "possibleDatabaseList")
possibleDatabaseList = []
for database in possibleDatabaseListRaw.split(","):
    possibleDatabaseList.append(database)
print "Done"
print


print "Download CSV's..."
tempDir = tempfile.gettempdir()
#  for example: r'C:\Users\User1\AppData\Local\Temp'
# Download CSV
try:
    tempCSVPath = os.path.join(tempDir, "PublicSchools.csv")
    csvFile = urllib.urlopen(url).read()
    with open(tempCSVPath, "w") as fx:
        fx.write(csvFile)
    fx.close()
except:
    print " exception downloading csv"
print " Done"
# Download CSV2
try:
    tempCSVPath2 = os.path.join(tempDir, "PrivateSchools.csv")
    csvFile2 = urllib.urlopen(url2).read()
    with open(tempCSVPath2, "w") as fx2:
        fx2.write(csvFile2)
    fx2.close()
except:
    print " exception downloading csv 2"
print " Done"
# Download CSV3
try:
    tempCSVPath3 = os.path.join(tempDir, "CollegesAndUniversities.csv")
    csvFile3 = urllib.urlopen(url3).read()
    with open(tempCSVPath3, "w") as fx3:
        fx3.write(csvFile3)
    fx3.close()
except:
    print " exception downloading csv 3"
print " Done" 
# Download CSV4
try:
    tempCSVPath4 = os.path.join(tempDir, "SupplementalColleges.csv")
    csvFile4 = urllib.urlopen(url4).read()
    with open(tempCSVPath4, "w") as fx4:
        fx4.write(csvFile4)
    fx4.close()
except:
    print " exception downloading csv 4"
print " Done"
print


print "Determine which of the 58 Databases exist..."
existingDatabaseList = []
for database in possibleDatabaseList:
    try:
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+database+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        existingDatabaseList.append(database)
    except:
        pass
print existingDatabaseList
print "Done"
print


print "Create 'hifld_School' Staging Table..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        try:
            if cursor.tables(table="hifld_School", tableType="TABLE").fetchone():
                print " hifld_School already exists, dropping table..."
                cursor.execute("DROP TABLE hifld_School")
                conn.commit()
                print " done"
            else:
                print " hifld_School doesn't exist"
            print " Creating hifld_School table..."
            createTable = "CREATE TABLE hifld_School \
                            (ID varchar(150), \
                            NAME varchar(150), \
                            ADDRESS varchar(150), \
                            CITY varchar(50), \
                            STATE varchar(50), \
                            ZIP varchar(16), \
                            TELEPHONE varchar(50), \
                            TYPE varchar(50), \
                            COUNTY varchar(50), \
                            Y float, \
                            X float, \
                            NAICSCODE int, \
                            NAICSDESCR varchar(100))"
            cursor.execute(createTable)
            conn.commit()
            print " done"
        except:
            print " cursor execute createTable exception"
except:
    print " exception Staging Table"
print "Done"
print


print "ADD hifld_School fields..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        hifldtable = "["+state+"]..[hifld_School]"
        try:
            cursor.execute("ALTER TABLE "+hifldtable+" \
                            ADD SchoolId varchar(8), \
                            EfClass varchar(5), \
                            Usage varchar(50), \
                            BldgCost numeric(38,8), \
                            ContentsCost numeric(38,8), \
                            Cost numeric(38,8), \
                            MeansAdjNonRes real, \
                            IDseq int IDENTITY(1,1), \
                            CensusTractID nvarchar(11), \
                            BldgSchemesId nvarchar(5), \
                            Shape geometry, \
                            MedianYearBuilt smallint, \
                            eqDesignLevel nvarchar(2), \
                            eqBldgType nvarchar(4), \
                            UATYP nvarchar(1), \
                            FoundationType char(1), \
                            FirstFloorHt float, \
                            BldgDamageFnId varchar(10), \
                            ContDamageFnId varchar(10), \
                            FloodProtection int, \
                            NameTRUNC nvarchar(40), \
                            ContactTRUNC nvarchar(40), \
                            CommentTRUNC nvarchar(40), \
                            BldgType nvarchar(50), \
                            AddressTRUNC nvarchar(40), \
                            FIPSCountyID nvarchar(5), \
                            Area numeric(38,8), \
                            Kitchen smallint, \
                            BackupPower smallint, \
                            ShelterCapacity int,\
                            Population int,\
                            NumStudents int")
            conn.commit()
        except Exception as e:
            print "  cursor ALTER TABLE exception: {}".format((e))
except:
    print " exception ADD hifld_School fields"
print "Done"
print


print "Copy Downloaded HIFLD Public School CSV to SQL Staging Table..."
RowCountCSV1Dict = {}
try:
    # Define the columns that data will be inserted into
    hifld_School_Columns = "ID, \
                            NAME, \
                            ADDRESS, \
                            CITY, \
                            STATE, \
                            ZIP, \
                            TELEPHONE, \
                            TYPE, \
                            COUNTY, \
                            Y, \
                            X, \
                            NAICSCODE, \
                            NAICSDESCR, \
                            Population, \
                            EfClass, \
                            NumStudents"
    for state in existingDatabaseList:
        RowCountCSV1 = 0
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        # Iterate CSV and insert into sql
        try:
            f = open(tempCSVPath)
            reader = csv.DictReader(f)
            for row in reader:
                if row["STATE"] == state:
                    RowCountCSV1 += 1
                    csvCounty = row["COUNTY"].decode("utf-8").encode("ascii", "ignore")
                    # This list order must match the order of the created table that it's being inserted into                 
                    sqlInsertData = "INSERT INTO ["+state+"]..[hifld_School] ("\
                                    +hifld_School_Columns+") \
                                    VALUES \
                                    (?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?)"
                    try:
                        csvNAME = row["NAME"].decode("utf-8").encode("ascii", "ignore")
                        cursor.execute(sqlInsertData,
                                       [row["NCESID"], \
                                        csvNAME, \
                                        row["ADDRESS"], \
                                        row["CITY"], \
                                        row["STATE"], \
                                        row["ZIP"], \
                                        row["TELEPHONE"], \
                                        row["TYPE"], \
                                        csvCounty, \
                                        row["Y"], \
                                        row["LONGITUDE"], \
                                        row["NAICS_CODE"], \
                                        row["NAICS_DESC"],
                                        row["POPULATION"], \
                                        "EFS1", \
                                        row["ENROLLMENT"]])
                        conn.commit()
                    except Exception as e:
                        print " cursor execute insertData CSV exception: ID {}, {}".format(row["NCESID"], (e))
                    RowCountCSV1Dict[state] = RowCountCSV1
        except Exception as e:
            print " csv dict exception: ID {}, {}".format(row["NCESID"], (e))
except:
    print " exception Copy Downloaded HIFLD CSV to Staging Table"
print "Done"
print
        

print "Copy Downloaded HIFLD Private School CSV2 to SQL Staging Table..."
RowCountCSV2Dict = {}
try:
    # Define the columns that data will be inserted into
    hifld_School_Columns = "ID, \
                            NAME, \
                            ADDRESS, \
                            CITY, \
                            STATE, \
                            ZIP, \
                            TELEPHONE, \
                            TYPE, \
                            COUNTY, \
                            Y, \
                            X, \
                            NAICSCODE, \
                            NAICSDESCR, \
                            Population, \
                            EfClass, \
                            NumStudents"
    for state in existingDatabaseList:
        RowCountCSV2 = 0
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        # Iterate CSV and insert into sql
        try:
            f = open(tempCSVPath2)
            reader = csv.DictReader(f)
            for row in reader:
                if row["STATE"] == state:
                    RowCountCSV2 += 1
                    csvCounty = row["COUNTY"].decode("utf-8").encode("ascii", "ignore")
                    # This list order must match the order of the created table that it's being inserted into                 
                    sqlInsertData = "INSERT INTO ["+state+"]..[hifld_School] ("\
                                    +hifld_School_Columns+") \
                                    VALUES \
                                    (?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?)"
                    try:
                        csvNAME = row["NAME"].decode("utf-8").encode("ascii", "ignore")
                        cursor.execute(sqlInsertData,
                                       [row["NCESID"], \
                                        csvNAME, \
                                        row["ADDRESS"], \
                                        row["CITY"], \
                                        row["STATE"], \
                                        row["ZIP"], \
                                        row["TELEPHONE"], \
                                        row["TYPE"], \
                                        csvCounty, \
                                        row["Y"], \
                                        row["LONGITUDE"], \
                                        row["NAICS_CODE"], \
                                        row["NAICS_DESC"],
                                        row["POPULATION"],
                                        "EFS1", \
                                        row["ENROLLMENT"]])
                        conn.commit()
                    except Exception as e:
                        print " cursor execute insertData CSV2 exception: ID {}, {}".format(row["NCESID"], (e))
                    RowCountCSV2Dict[state] = RowCountCSV2
        except Exception as e:
            print " csv2 dict exception: ID {}, {}".format(row["NCESID"], (e))

except:
    print " exception Copy Downloaded HIFLD School CSV 2 to Staging Table"
print "Done"
print


print "Copy Downloaded HIFLD Colleges and Universities CSV3 to SQL Staging Table..."
RowCountCSV3Dict = {}
try:
    # Define the columns that data will be inserted into
    hifld_School_Columns = "ID, \
                            NAME, \
                            ADDRESS, \
                            CITY, \
                            STATE, \
                            ZIP, \
                            TELEPHONE, \
                            TYPE, \
                            COUNTY, \
                            Y, \
                            X, \
                            NAICSCODE, \
                            NAICSDESCR, \
                            Population, \
                            EfClass, \
                            NumStudents"
    for state in existingDatabaseList:
        RowCountCSV3 = 0
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        # Iterate CSV and insert into sql
        try:
            f = open(tempCSVPath3)
            reader = csv.DictReader(f)
            for row in reader:
                if row["STATE"] == state:
                    RowCountCSV3 += 1
                    csvCounty = row["COUNTY"].decode("utf-8").encode("ascii", "ignore")
                    csvName = row["NAME"].decode("utf-8").encode("ascii", "ignore")
                    csvAddress = row["ADDRESS"].decode("utf-8").encode("ascii", "ignore")
                    csvCity = row["CITY"].decode("utf-8").encode("ascii", "ignore")
                    # This list order must match the order of the created table that it's being inserted into                 
                    sqlInsertData = "INSERT INTO ["+state+"]..[hifld_School] ("\
                                    +hifld_School_Columns+") \
                                    VALUES \
                                    (?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?)"
                    try:
                        csvNAME = row["NAME"].decode("utf-8").encode("ascii", "ignore")
                        cursor.execute(sqlInsertData,
                                       [row["IPEDSID"], \
                                        csvNAME, \
                                        csvAddress, \
                                        csvCity, \
                                        row["STATE"], \
                                        row["ZIP"], \
                                        row["TELEPHONE"], \
                                        row["TYPE"], \
                                        csvCounty, \
                                        row["Y"], \
                                        row["LONGITUDE"], \
                                        row["NAICS_CODE"], \
                                        row["NAICS_DESC"],
                                        row["POPULATION"], \
                                        "EFS2", \
                                        row["TOT_ENROLL"]])
                        conn.commit()
                    except Exception as e:
                        print " cursor execute insertData CSV3 exception: ID {}, {}".format(row["IPEDSID"], (e))
                    RowCountCSV3Dict[state] = RowCountCSV3
        except Exception as e:
            print " csv3 dict exception: ID {}, {}".format(row["IPEDSID"], (e))
except:
    print " exception Copy Downloaded HIFLD CSV's to Staging Table"
print "Done"
print

print "Copy Downloaded HIFLD Supplemental Colleges CSV4 to SQL Staging Table..."
RowCountCSV4Dict = {}
try:
    # Define the columns that data will be inserted into
    hifld_School_Columns = "ID, \
                            NAME, \
                            ADDRESS, \
                            CITY, \
                            STATE, \
                            ZIP, \
                            TELEPHONE, \
                            TYPE, \
                            COUNTY, \
                            Y, \
                            X, \
                            NAICSCODE, \
                            NAICSDESCR, \
                            Population, \
                            EfClass, \
                            NumStudents"
    for state in existingDatabaseList:
        RowCountCSV4 = 0
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        # Iterate CSV and insert into sql
        try:
            f = open(tempCSVPath4)
            reader = csv.DictReader(f)
            for row in reader:
                if row["STATE"] == state:
                    RowCountCSV4 += 1
                    csvCounty = row["COUNTY"].decode("utf-8").encode("ascii", "ignore")
                    csvName = row["NAME"].decode("utf-8").encode("ascii", "ignore")
                    csvAddress = row["ADDRESS"].decode("utf-8").encode("ascii", "ignore")
                    csvCity = row["CITY"].decode("utf-8").encode("ascii", "ignore")
                    # This list order must match the order of the created table that it's being inserted into                 
                    sqlInsertData = "INSERT INTO ["+state+"]..[hifld_School] ("\
                                    +hifld_School_Columns+") \
                                    VALUES \
                                    (?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?)"
                    try:
                        cursor.execute(sqlInsertData,
                                       [row["IPEDSID"], \
                                        csvName, \
                                        csvAddress, \
                                        csvCity, \
                                        row["STATE"], \
                                        row["ZIP"], \
                                        row["TELEPHONE"], \
                                        row["TYPE"], \
                                        csvCounty, \
                                        row["Y"], \
                                        row["LONGITUDE"], \
                                        row["NAICS_CODE"], \
                                        row["NAICS_DESC"],
                                        row["POPULATION"], \
                                        "EFS2", \
                                        row["TOT_ENROLL"]])
                        conn.commit()
                    except Exception as e:
                        print " cursor execute insertData CSV4 exception: ID {}, {}".format(row["IPEDSID"], (e))
                    RowCountCSV4Dict[state] = RowCountCSV4
        except Exception as e:
            print " csv4 dict exception: ID {}, {}".format(row["IPEDSID"], (e))

except:
    print " exception Copy Downloaded HIFLD CSV's to Staging Table"
print "Done"
print


print "Calculate hifld_School fields..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+";\
                        Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        hifldtable = "["+state+"]..[hifld_School]"

        # If territory, add BldgSchemesId then set to null before completing script
        if state in ["GU", "AS", "VI", "MP"]:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                            ";Database="+state+";UID="+UserName+";PWD="+Password
            conn = pyodbc.connect(connectString, autocommit=False)
            cursor = conn.cursor()
            hzTracttable = "["+state+"]..[hzTract]"

            # Set BldgSchemesId
            try:
                cursor.execute("UPDATE "+hzTracttable+" SET BldgSchemesId = '"+state+"1"+"'")
                conn.commit()
            except:
                print " cursor execute UPDATE hzTract"
            
        # SchoolId (State abbreviation plus 6 digits eg WA123456,
        # this must be unique and will persist across four tables.
        # IDSeq should be unique, non null and int)
        try:
            cursor.execute("UPDATE "+hifldtable+" SET SchoolId = '"+state+\
                           "' + RIGHT('000000'+cast(IDseq as varchar(6)),6)")
            conn.commit()
        except:
            print " cursor execute UPDATE SchoolId"

        # Kitchen
        try:
            updateData = "UPDATE "+hifldtable+" SET Kitchen = 0"
            cursor.execute(updateData)
            conn.commit()
        except:
            print " cursor execute UPDATE Kitchen exception"

        # BackupPower
        try:
            updateData = "UPDATE "+hifldtable+" SET BackupPower = 0"
            cursor.execute(updateData)
            conn.commit()
        except:
            print " cursor execute UPDATE BackupPower exception"

        # ShelterCapacity
        try:
            updateData = "UPDATE "+hifldtable+" SET ShelterCapacity = 0"
            cursor.execute(updateData)
            conn.commit()
        except:
            print " cursor execute UPDATE ShelterCapacity exception"

        # Usage
        try:
            updateData = "UPDATE "+hifldtable+" SET Usage='School'"
            cursor.execute(updateData)
            conn.commit()
        except:
            print " cursor execute UPDATE Usage exception"

        # EfClass
        # This is set when loading the CSV data into the hifld table

        # Area
        try:
            # get value to use as default if another value is not found
            connectStringCDMS = "Driver={SQL Server};Server="+userDefinedServer+\
                                ";Database=CDMS;UID="+UserName+";PWD="+Password
            connCDMS = pyodbc.connect(connectStringCDMS, autocommit=False)
            cursorCDMS = connCDMS.cursor()
            cursorCDMS.execute("SELECT Occupancy,SquareFootage \
                                FROM [CDMS]..[hzSqftFactors] \
                                WHERE Occupancy = 'EDU1'")
            rows = cursorCDMS.fetchall()
            # Get EDU1 SqFt
            for row in rows:
                HazusDefaultSqFtEDU1 = str(row.SquareFootage)
            cursorCDMS.execute("SELECT Occupancy,SquareFootage \
                                FROM [CDMS]..[hzSqftFactors] \
                                WHERE Occupancy = 'EDU2'")
            rows = cursorCDMS.fetchall()
            # Get EDU2 SqFt
            for row in rows:
                HazusDefaultSqFtEDU2 = str(row.SquareFootage)
            # Get EDU1 PeakDay
            cursorCDMS.execute("SELECT Occupancy,PeakDay \
                                FROM [CDMS]..[cdms_AEBMParameters] \
                                WHERE Occupancy = 'EDU1'")
            rows = cursorCDMS.fetchall()
            for row in rows:
                HazusEDU1PeakDay = str(row.PeakDay)
            # Get EDU2 PeakDay
            cursorCDMS.execute("SELECT Occupancy,PeakDay \
                                FROM [CDMS]..[cdms_AEBMParameters] \
                                WHERE Occupancy = 'EDU2'")
            rows = cursorCDMS.fetchall()
            for row in rows:
                HazusEDU2PeakDay = str(row.PeakDay)
                            
            # cdms_AEBMParameters edu1, edu2. population * FEMA P58 PeakDay squarefootage estimate,
            # otherwise default from hzSqFtFactors if population is missing
            updateData = "UPDATE "+hifldtable+" \
                            SET Area = Population * "+HazusEDU1PeakDay+"\
                            WHERE EfClass = 'EFS1' AND Population <> -999"
            cursor.execute(updateData)
            conn.commit()
            updateData = "UPDATE "+hifldtable+" \
                            SET Area = Population * "+HazusEDU2PeakDay+"\
                            WHERE EfClass = 'EFS2' AND Population <> -999"
            cursor.execute(updateData)
            conn.commit()
            updateData = "UPDATE "+hifldtable+" \
                            SET Area = "+HazusDefaultSqFtEDU1+"\
                            WHERE EfClass = 'EFS1' AND Population = -999"
            cursor.execute(updateData)
            conn.commit()
            updateData = "UPDATE "+hifldtable+" \
                            SET Area = "+HazusDefaultSqFtEDU2+"\
                            WHERE EfClass = 'EFS2' AND Population = -999"
            cursor.execute(updateData)
            conn.commit()

        except Exception as e:
            print " cursor execute Update Area exception: {}".format((e))

        # CENSUS TRACTS ID      
        # Calculate Shape
        try:
            cursor.execute("UPDATE "+hifldtable+\
                           " SET Shape = geometry::Point(X, Y, 4326)")
            conn.commit()
        except:
            print " cursor execute UPDATE Shape exception"
        # Calculate TractID field...
        # To get all tract id's from hzTract based on the intersection of hzEmergencyCtr and hztract...
        try:
            cursor.execute("UPDATE a \
                            SET a.CensusTractID = b.tract, \
                            a.BldgSchemesId = b.BldgSchemesId \
                            FROM ["+state+"]..[hzTract] b \
                            INNER JOIN "+hifldtable+" a \
                            ON b.shape.STIntersects(a.shape) = 1")
            conn.commit()
        except:
            print " cursor execute UPDATE tractid exception"
            
        # Update CountyFIPSID based on CensusTractID
        try:
            cursor.execute("UPDATE "+hifldtable+" \
                            SET [FIPSCountyID] = LEFT([CensusTractID], 5)")
            conn.commit()
        except:
            print "cursor execute UPDATE CountyFIPSID based on CensusTractID"

        # Cost
        try:
            # get value to use as default if another value is not found
            connectStringCDMS = "Driver={SQL Server};Server="+userDefinedServer+\
                                ";Database=CDMS;UID="+UserName+";PWD="+Password
            connCDMS = pyodbc.connect(connectStringCDMS, autocommit=False)
            cursorCDMS = connCDMS.cursor()

            # Join table on CountyFIPS and assign MeansAdjNonRes values
            try:
                cursor.execute("UPDATE table1 \
                                SET table1.MeansAdjNonRes = table2.MeansAdjNonRes \
                                FROM "+hifldtable+" AS table1 \
                                LEFT JOIN ["+state+"]..[hzMeansCountyLocationFactor] as table2 \
                                ON [table1].[FIPSCountyID] = [table2].[CountyFIPS]")
                conn.commit()
            except:
                print "cursor execute UPDATE Calculate Cost exception - MeansAdjNonRes table join"

            # Set MeansAdjNonRes to 1 if its value is null due to not having a hzMeansCountyLocationFactor table (GU, AS, VI, MP)
            try:
                cursor.execute("UPDATE "+hifldtable+" \
                                SET MeansAdjNonRes = 1 \
                                WHERE MeansAdjNonRes IS NULL")
                conn.commit()
            except:
                print "cursor execute UPDATE Calculate Cost exception - MeansAdjNonRes table assign 1"
                
            # Get MeansCost based on EDU1
            cursorCDMS.execute("SELECT Occupancy, MeansCost \
                                FROM [CDMS]..[hzReplacementCost] \
                                WHERE Occupancy = 'EDU1'")
            rows = cursorCDMS.fetchall()
            for row in rows:
                MeansCost1 = str(row.MeansCost)

            # Get ContentValPct based on EDU1
            cursorCDMS.execute("SELECT Occupancy, ContentValPct \
                                FROM [CDMS]..[hzPctContentOfStructureValue] \
                                WHERE Occupancy = 'EDU1'")
            rows = cursorCDMS.fetchall()
            for row in rows:
                ContentValPct1 = str(row.ContentValPct/100.0)

            # Get MeansCost based on EDU2
            cursorCDMS.execute("SELECT Occupancy, MeansCost \
                                FROM [CDMS]..[hzReplacementCost] \
                                WHERE Occupancy = 'EDU2'")
            rows = cursorCDMS.fetchall()
            for row in rows:
                MeansCost2 = str(row.MeansCost)

            # Get ContentValPct based on EDU2
            cursorCDMS.execute("SELECT Occupancy, ContentValPct \
                                FROM [CDMS]..[hzPctContentOfStructureValue] \
                                WHERE Occupancy = 'EDU2'")
            rows = cursorCDMS.fetchall()
            for row in rows:
                ContentValPct2 = str(row.ContentValPct/100.0)
            
            # Update Bldgcost EFS1
            updateData = "UPDATE "+hifldtable+" \
                        SET BldgCost = (Area * "+MeansCost1+" * MeansAdjNonRes) / 1000 \
                        WHERE EfClass = 'EFS1'"
            cursor.execute(updateData)
            conn.commit()

            # Update ContentsCost EFS1
            updateData = "UPDATE "+hifldtable+" \
                        SET ContentsCost = BldgCost * "+ContentValPct1+" \
                        WHERE EfClass = 'EFS1'"
            cursor.execute(updateData)
            conn.commit()

            # Update Bldgcost EFS2
            updateData = "UPDATE "+hifldtable+" \
                        SET BldgCost = (Area * "+MeansCost2+" * MeansAdjNonRes) / 1000 \
                        WHERE EfClass = 'EFS2'"
            cursor.execute(updateData)
            conn.commit()

            # Update ContentsCost EFS2
            updateData = "UPDATE "+hifldtable+" \
                        SET ContentsCost = BldgCost * "+ContentValPct2+" \
                        WHERE EfClass = 'EFS2'"
            cursor.execute(updateData)
            conn.commit()

            # Update Cost
            updateData = "UPDATE "+hifldtable+" \
                        SET Cost = BldgCost + ContentsCost"
            cursor.execute(updateData)
            conn.commit()
            
        except:
            print "cursor execute UPDATE Calculate Cost exception"

        # MedianYearBuilt
        try:
            cursor.execute("UPDATE a SET a.MedianYearBuilt = b.MedianYearBuilt \
                            FROM ["+state+"]..[hzDemographicsT] b \
                            INNER JOIN "+hifldtable+" a ON b.Tract = a.CensusTractID \
                            WHERE a.MedianYearBuilt IS NULL OR a.MedianYearBuilt = 0")
            conn.commit()
        except:
            print " cursor execute MedianYearBuilt exception"

        # EARTHQUAKE SPECIFIC FIELDS
        # DesignLevel
        try:
            pass
            cursor.execute("UPDATE "+hifldtable+" SET [eqDesignLevel]=(CASE \
                            WHEN RIGHT([BldgSchemesId],1)=3 AND [MedianYearBuilt] >= 0 AND [MedianYearBuilt] <1940 THEN 'PC' \
                            WHEN RIGHT([BldgSchemesId],1)=3 AND [MedianYearBuilt] >= 1940 AND [MedianYearBuilt] <1960 THEN 'LC' \
                            WHEN RIGHT([BldgSchemesId],1)=3 AND [MedianYearBuilt] >= 1960 AND [MedianYearBuilt] <1973 THEN 'MC' \
                            WHEN RIGHT([BldgSchemesId],1)=3 AND [MedianYearBuilt] >= 1973 AND [MedianYearBuilt] <2000 THEN 'HC' \
                            WHEN RIGHT([BldgSchemesId],1)=3 AND [MedianYearBuilt] >= 2000 THEN 'HS' \
                            WHEN RIGHT([BldgSchemesId],1)=2 AND [MedianYearBuilt] >= 0 AND [MedianYearBuilt] <1940 THEN 'PC' \
                            WHEN RIGHT([BldgSchemesId],1)=2 AND [MedianYearBuilt] >= 1940 AND [MedianYearBuilt] <1973 THEN 'LC' \
                            WHEN RIGHT([BldgSchemesId],1)=2 AND [MedianYearBuilt] >= 1973 AND [MedianYearBuilt] <2000 THEN 'MC' \
                            WHEN RIGHT([BldgSchemesId],1)=2 AND [MedianYearBuilt] >= 2000 THEN 'HC' \
                            WHEN RIGHT([BldgSchemesId],1)=1 AND [MedianYearBuilt] >= 0 AND [MedianYearBuilt] <1973 THEN 'PC' \
                            WHEN RIGHT([BldgSchemesId],1)=1 AND [MedianYearBuilt] >= 1973 AND [MedianYearBuilt] <2000 THEN 'LC' \
                            WHEN RIGHT([BldgSchemesId],1)=1 AND [MedianYearBuilt] >= 2000 THEN 'MC' \
                            ELSE '' END) ")
            conn.commit()
        except:
            print " cursor execute eqDesignLevel exception"

        # UATYP
        try:
            cursor.execute("UPDATE a SET a.UATYP = b.UATYP FROM [CDMS]..[cdms_CensusUrbanAreas] b \
                            INNER JOIN "+hifldtable+" a ON b.shape.STIntersects(a.shape) = 1")
            cursor.execute("UPDATE "+hifldtable+" SET UATYP = \
                            (CASE WHEN UATYP='U' THEN 'U' ELSE 'R' END)")
            conn.commit()
        except:
            print " cursor execute UPDATE UATYP exception"
            
        # eqBldgType
        # Get State eqBldgTypes
        try:
            cursor.execute("SELECT MCHCHS_U_eqSchool FROM [CDMS]..[eqEFBldgTypeDefault] \
                            WHERE StateId = '"+state+"'")
            rows = cursor.fetchall()
            for row in rows:
                MCHCHS_U_eqSchool = row.MCHCHS_U_eqSchool
                
            cursor.execute("SELECT MCHCHS_R_eqSchool FROM [CDMS]..[eqEFBldgTypeDefault] \
                            WHERE StateId = '"+state+"'")
            rows = cursor.fetchall()
            for row in rows:
                MCHCHS_R_eqSchool = row.MCHCHS_R_eqSchool
                
            cursor.execute("SELECT PCLC_U_eqSchool FROM [CDMS]..[eqEFBldgTypeDefault] \
                            WHERE StateId = '"+state+"'")
            rows = cursor.fetchall()
            for row in rows:
                PCLC_U_eqSchool = row.PCLC_U_eqSchool
                
            cursor.execute("SELECT PCLC_R_eqSchool FROM [CDMS]..[eqEFBldgTypeDefault] \
                            WHERE StateId = '"+state+"'")
            rows = cursor.fetchall()
            for row in rows:
                PCLC_R_eqSchool = row.PCLC_R_eqSchool
        except Exception as e:
            print " cursor execute GET State eqBldgTypes exception: {}".format((e))
            
        # Set eqBldgType
        try:
            cursor.execute("UPDATE "+hifldtable+" SET eqBldgType=(CASE \
                            WHEN UATYP='U' AND (eqDesignLevel='MC' OR eqDesignLevel='HC' OR eqDesignLevel='HS') THEN '"+MCHCHS_U_eqSchool+"' \
                            WHEN UATYP='R' AND (eqDesignLevel='MC' OR eqDesignLevel='HC' OR eqDesignLevel='HS') THEN '"+MCHCHS_R_eqSchool+"' \
                            WHEN UATYP='U' AND (eqDesignLevel='PC' OR eqDesignLevel='LC') THEN '"+PCLC_U_eqSchool+"' \
                            WHEN UATYP='R' AND (eqDesignLevel='PC' OR eqDesignLevel='LC') THEN '"+PCLC_R_eqSchool+"' \
                            ELSE '' END)")
            conn.commit()
        except:
            print " cursor execute UPDATE eqBldgType exception"

        # FLOOD SPECIFIC FIELDS        
        # BldgType
        try:
            cursor.execute("UPDATE a SET a.BldgType = b.GeneralBuildingType \
                            FROM [CDMS]..[GeneralBuildingEarthquakeBuilding] b \
                            INNER JOIN "+hifldtable+" a \
                            ON REPLACE(a.eqBldgType, 'L', '') = b.EarthquakeBuildingType")
            conn.commit()
        except:
            print " cursor execute UPDATE FLOOD BldgType exception"
            
        # Hazus_model.dbo.flSchoolDflt; EFS1
        efClassList = ["EFS1", "EFS2"]
        for efClass in efClassList:
            try:
                cursor.execute("UPDATE a \
                                SET a.FoundationType=b.FoundationType, \
                                a.FirstFloorHt=b.FirstFloorHt, \
                                a.BldgDamageFnId=b.BldgDamageFnId, \
                                a.ContDamageFnId=b.ContDamageFnId, \
                                a.FloodProtection=b.FloodProtection \
                                FROM [Hazus_model]..[flSchoolDflt] b \
                                INNER JOIN "+hifldtable+" a \
                                ON b.EFClass = a.EfClass \
                                WHERE a.EfClass = '"+efClass+"'")
                conn.commit()
            except:
                print " cursor execute calc flFoundationType, flFirstFloorHt, flBldgDamageFnId, flContDamageFnId, flFloodProtection exception"

        # Add 1 foot to firstfloorht for records whose MedianYearBuilt > EntryDate
        try:
            cursor.execute("UPDATE a SET a.FirstFloorHt = a.FirstFloorHt + 1 \
                            FROM "+hifldtable+" a \
                            LEFT JOIN (SELECT MAX(EntryDate) as EntryDateMAX, \
                            SUBSTRING(CensusBlock, 1,11) as CensusBlockTract \
                            FROM ["+state+"]..[flSchemeMapping] \
                            GROUP BY SUBSTRING(CensusBlock, 1,11)) b \
                            ON a.CensusTractID = b.CensusBlockTract \
                            WHERE a.MedianYearBuilt > b.EntryDateMax")
            conn.commit()
        except:
            print " cursor execute firstfloor modification" 

        # Update MedianYearBuilt values of < 1939 to be 1939 before moving into HAZUS tables
        try:
            updateData = "UPDATE "+hifldtable+" \
                            SET MedianYearBuilt = 1939 \
                            WHERE MedianYearBuilt < 1939"
            cursor.execute(updateData)
            conn.commit()
        except Exception as e:
            print " cursor execute Update MedianYearBuilt <1939 exception: {}".format((e))

        # Update NumStudents values < 1 to be NULL before moving into HAZUS tables
        try:
            updateData = "UPDATE "+hifldtable+" \
                            SET NumStudents = NULL \
                            WHERE NumStudents < 0"
            cursor.execute(updateData)
            conn.commit()
        except Exception as e:
            print " cursor execute Update NumStudents < 1 exception: {}".format((e))

##        # Update ZIP length < 5 with prefix 0
##        try:
##            cursor.execute("UPDATE "+hifldtable+" \
##                            SET ZIP = RIGHT('00000'+cast(ZIP as varchar(5)),5) \
##                            WHERE LEN(ZIP) < 5")
##            conn.commit()
##        except Exception as e:
##            print " cursor execute Update ZIP exception: {}".format((e))
        
        # CONDITION DATA TO FIT WITHIN MAX LIMITS
        # Calculate the truncated fields
        try:
            cursor.execute("UPDATE "+hifldtable\
                           +" SET NameTRUNC = \
                            (CASE WHEN LEN(NAME)>40 THEN CONCAT(LEFT(NAME,37),'...') \
                            ELSE NAME END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET CommentTRUNC = \
                            (CASE WHEN LEN(NAICSDESCR)>40 THEN CONCAT(LEFT(NAICSDESCR,37),'...') \
                            ELSE NAICSDESCR END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET AddressTRUNC = \
                            (CASE WHEN LEN(ADDRESS)>40 THEN CONCAT(LEFT(ADDRESS,37),'...') \
                            ELSE ADDRESS END)")
            conn.commit()
        except Exception as e:
            print " cursor execute TRUNC Fields to be under 40 exception: {}".format((e))

        # and update to not be more than 32767, and if it is change it to 32767 (smallint limitations in hz schema)
        try:
            updateData = "UPDATE "+hifldtable+" \
                            SET NumStudents = 32767 \
                            WHERE NumStudents > 32767"
            cursor.execute(updateData)
            conn.commit()
        except Exception as e:
            print " cursor execute Update NumStudents 32767 Exceeds Max Value exception: {}".format((e))
        


        # If territory, add BldgSchemesId then set to null before completing script
        if state in ["GU", "AS", "VI", "MP"]:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                            ";Database="+state+";UID="+UserName+";PWD="+Password
            conn = pyodbc.connect(connectString, autocommit=False)
            cursor = conn.cursor()
            hzTracttable = "["+state+"]..[hzTract]"

            # Set BldgSchemesId
            try:
                cursor.execute("UPDATE "+hzTracttable+" SET BldgSchemesId = NULL")
                conn.commit()
            except:
                print " cursor execute UPDATE hzTract"
            
except:
    print " exception Calculate hifld_School Fields"
print "Done"
print


tempRowCountPath = os.path.join(tempDir, "rowcount_School.txt")
with open(tempRowCountPath, "w") as xf:
    print "Move data from the HIFLD staging table to the HAZUS tables."
    try:
        for state in existingDatabaseList:
            print state
            hifldTable = "["+state+"]..[hifld_School]"
            hzTable = "["+state+"]..[hzSchool]"
            flTable = "["+state+"]..[flSchool]"
            eqTable = "["+state+"]..[eqSchool]"
            
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                            ";Database="+state+";UID="+UserName+";PWD="+Password
            conn = pyodbc.connect(connectString, autocommit=False)
            cursor = conn.cursor()

            # Remove HAZUS rows
            print " Remove HAZUS rows from hzSchool"
            try:
                cursor.execute("TRUNCATE TABLE "+hzTable)
                conn.commit()
            except:
                print " cursor execute Delete HAZUS from hzSchool exception"
            print " done"
            
            print " Remove hazus rows from flSchool"
            try:
                cursor.execute("TRUNCATE TABLE "+flTable)
                conn.commit()
            except:
                print " cursor execute Delete HAZUS from flSchool exception"
            print " done"
            
            print " Remove hazus rows from eqSchool"
            try:
                cursor.execute("TRUNCATE TABLE "+eqTable)
                conn.commit()
            except:
                print " cursor execute Delete HAZUS from eqSchool exception"
            print " done"

            # Copy Rows from HIFLD to HAZUS hazard
            print " Copy rows from hifld_School to hzSchool..."
            try:
                cursor.execute("INSERT INTO "+hzTable+" \
                                (Shape, \
                                SchoolId, \
                                EfClass, \
                                Tract, \
                                Name, \
                                Address, \
                                City, \
                                Zipcode, \
                                Statea, \
                                PhoneNumber, \
                                YearBuilt, \
                                Cost, \
                                NumStudents, \
                                Latitude, \
                                Longitude, \
                                Area, \
                                ShelterCapacity, \
                                BackupPower, \
                                Kitchen, \
                                Comment) \
                                \
                                SELECT \
                                Shape, \
                                SchoolId, \
                                EfClass, \
                                CensusTractId, \
                                NameTRUNC, \
                                AddressTRUNC, \
                                LEFT(City, 40), \
                                Zip, \
                                State, \
                                LEFT(Telephone,14), \
                                MedianYearBuilt, \
                                BldgCost, \
                                NumStudents, \
                                Y, \
                                X, \
                                Area, \
                                ShelterCapacity, \
                                BackupPower, \
                                Kitchen, \
                                CommentTRUNC \
                                \
                                FROM "+hifldTable+\
                                " WHERE SchoolId IS NOT NULL \
                                AND EfClass IS NOT NULL \
                                AND CensusTractId IS NOT NULL \
                                ORDER BY SchoolId ASC")
                conn.commit()
            except Exception as e:
                print " cursor execute Insert Into hzSchool exception: {}".format((e))
            print " done"
            
            # Copy Rows from HIFLD to HAZUS flood
            print " Copy rows from hifld_School to flSchool..."
            try:
                cursor.execute("INSERT INTO "+flTable+"\
                                (SchoolId, \
                                BldgType, \
                                DesignLevel, \
                                FoundationType, \
                                FirstFloorHt, \
                                BldgDamageFnId, \
                                ContDamageFnId, \
                                FloodProtection) \
                                \
                                SELECT \
                                SchoolId, \
                                BldgType, \
                                LEFT(eqDesignLevel,1), \
                                FoundationType, \
                                FirstFloorHt, \
                                BldgDamageFnId, \
                                ContDamageFnId, \
                                FloodProtection \
                                \
                                FROM "+hifldTable+\
                                " WHERE SchoolId IS NOT NULL \
                                AND CensusTractId IS NOT NULL \
                                ORDER BY SchoolId ASC")
                conn.commit()
            except Exception as e:
                print " cursor execute Insert Into flSchool exception: {}".format((e))
            print " done"
            
            # Copy Rows from HIFLD to HAZUS earthquake
            print " Copy rows from hifld_School to eqSchool..."
            try:
                cursor.execute("INSERT INTO "+eqTable+" \
                                (SchoolId, \
                                eqBldgType, \
                                DesignLevel, \
                                FoundationType, \
                                SoilType, \
                                LqfSusCat, \
                                LndSusCat, \
                                WaterDepth) \
                                \
                                SELECT \
                                SchoolId, \
                                eqBldgType, \
                                eqDesignLevel, \
                                0, \
                                'D', \
                                0, \
                                0, \
                                5 \
                                FROM "+hifldTable+\
                                " WHERE SchoolId IS NOT NULL \
                                AND eqBldgType IS NOT NULL \
                                AND eqDesignLevel IS NOT NULL \
                                AND CensusTractId IS NOT NULL \
                                ORDER BY SchoolId ASC")
                conn.commit()
            except Exception as e:
                print " cursor execute Insert Into eqSchool exception: {}".format((e))
            print " done"

            # Get row count for HIFLD and HAZUS tables
            try:
                cursor.execute("SELECT COUNT(*) AS Column1 FROM "+hifldtable)
                rows = cursor.fetchall()
                for row in rows:
                    HIFLDRowCount = row.Column1
            except Exception as e:
                print " cursor execute row count hifld  exception: {}".format((e))
            try:
                cursor.execute("SELECT COUNT(*) AS Column1 FROM "+hzTable)
                rows = cursor.fetchall()
                for row in rows:
                    HzRowCount = row.Column1
            except Exception as e:
                print " cursor execute row count Hz  exception: {}".format((e))
            csvSum1 = RowCountCSV1Dict.get(state)
            if csvSum1 is None:
                csvSum1 = 0
            csvSum2 = RowCountCSV2Dict.get(state)
            if csvSum2 is None:
                csvSum2 = 0
            csvSum3 = RowCountCSV3Dict.get(state)
            if csvSum3 is None:
                csvSum3 = 0
            csvSum4 = RowCountCSV4Dict.get(state)
            if csvSum4 is None:
                csvSum4 = 0
            TotalCSVRows = csvSum1 + csvSum2 + csvSum3 + csvSum4
            print
            print "{} RowCountSummary".format(state)
            print "CSV: {} HIFLD: {} HZ: {}".format(TotalCSVRows, HIFLDRowCount, HzRowCount)
            print
            outstring = "{} CSV: {} HIFLD: {} HZ: {} \n".format(state, TotalCSVRows, HIFLDRowCount, HzRowCount)
            xf.write(outstring)
    except:
        print " exception Move Data from Staging to HAZUS Tables"
    print


##Cleanup shape field in staging table
##try:
##    for state in existingDatabaseList:
##        print state
##        hifldTable = "["+state+"]..[hifld_EmergencyCtr]"
##        # Drop Shape fields...
##        try:
##            cursor.execute("ALTER TABLE "+hifldTable+" DROP COLUMN Shape")
##            conn.commit()
##        except:
##            print " cursor execute DROP hifldTable Shape exception"
##except:
##    print " exception Clean up Shape field"

xf.close()
print "Big Done."

