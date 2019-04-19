# NiyamIT
# COLIN LINDEMAN, GIS Developer
# Proof of Concept - HIFLD Local Law Enforcement Locations into HAZUS EmergencyCtr.
# Last Update: 2019-04-17
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


print "Read config.ini file..."
# User defined variables from .ini file...
# User needs to change config path
configPath = "D:\Dropbox\NiyaMIT\config.ini"
cfgParser = ConfigParser.ConfigParser()
cfgParser.read(configPath)
url = cfgParser.get("HIFLD OPEN DATA URLS", "EmergencyCtr_URL")
url2 = cfgParser.get("HIFLD OPEN DATA URLS", "EmergencyCtr2_URL")
url3 = cfgParser.get("HIFLD OPEN DATA URLS", "EmergencyCtr3_URL")
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
    tempCSVPath = os.path.join(tempDir, "LocalEOC.csv")
    csvFile = urllib.urlopen(url).read()
    with open(tempCSVPath, "w") as fx:
        fx.write(csvFile)
    fx.close()
except:
    print " exception downloading csv"
print "Done"
print
# Download CSV2
try:
    tempCSVPath2 = os.path.join(tempDir, "StateEOC.csv")
    csvFile2 = urllib.urlopen(url2).read()
    with open(tempCSVPath, "w") as fx:
        fx.write(csvFile)
    fx.close()
except:
    print " exception downloading csv"
print "Done"
print
# Download CSV3
try:
    tempCSVPath3 = os.path.join(tempDir, "FEMAHQ.csv")
    csvFile3 = urllib.urlopen(url3).read()
    with open(tempCSVPath, "w") as fx:
        fx.write(csvFile)
    fx.close()
except:
    print " exception downloading csv"
print "Done"
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


print "Create 'hifld_EmergencyCtr' Staging Table..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        try:
            if cursor.tables(table="hifld_EmergencyCtr", tableType="TABLE").fetchone():
                print " hifld_EmergencyCtr already exists, dropping table..."
                cursor.execute("DROP TABLE hifld_EmergencyCtr")
                conn.commit()
                print " done"
            else:
                print " hifld_EmergencyCtr doesn't exist"
            print " Creating hifld_EmergencyCtr table..."
            createTable = "CREATE TABLE hifld_EmergencyCtr \
                            (ID int, \
                            NAME varchar(150), \
                            ADDRESS varchar(50), \
                            CITY varchar(50), \
                            STATE varchar(50), \
                            ZIP int, \
                            TELEPHONE varchar(50), \
                            TYPE varchar(50), \
                            COUNTY varchar(50), \
                            Y float, \
                            X float, \
                            NAICSCODE int, \
                            NAICSDESCR varchar(100), \
                            STATE_ID varchar(50))"
            cursor.execute(createTable)
            conn.commit()
            print " done"
        except:
            print " cursor execute createTable exception"
except:
    print " exception Staging Table"
print "Done"
print


print "ADD hifld_EmergencyCtr fields..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        hifldtable = "["+state+"]..[hifld_EmergencyCtr]"
        try:
            cursor.execute("ALTER TABLE "+hifldtable+" \
                            ADD EocId varchar(8), \
                            EfClass varchar(5), \
                            Usage varchar(50), \
                            BldgCost numeric(38,8), \
                            ContentsCost numeric(38,8), \
                            Cost numeric(38,8), \
                            CalcBldgSqFt int, \
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
                            ShelterCapacity int")
            conn.commit()
        except Exception as e:
            print "  cursor ALTER TABLE exception: {}".format((e))
except:
    print " exception ADD hifld_EmergencyCtr fields"
print "Done"
print


print "Copy Downloaded HIFLD CSV to SQL Staging Table..."
try:
    # Define the columns that data will be inserted into
    hifld_EmergencyCtr_Columns = "ID, \
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
                                STATE_ID"
    for state in existingDatabaseList:
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
                    # there are several records with funky ANSI 
                    # character, but not utf-8. Possibly not ASCII character.
##                    csvAddress = row["ADDRESS"].decode("utf-8").encode("ascii", "ignore")
##                    csvName = row["NAME"].decode("utf-8").encode("ascii", "ignore")
##                    csvCity = row["CITY"].decode("utf-8").encode("ascii", "ignore")
##                    csvTelephone = row["TELEPHONE"].decode("utf-8").encode("ascii", "ignore")
                    # This list order must match the order of the created table that it's being inserted into                 
                    sqlInsertData = "INSERT INTO ["+state+"]..[hifld_EmergencyCtr] ("\
                                    +hifld_EmergencyCtr_Columns+") \
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
                                    ?)"
                    try:
                        cursor.execute(sqlInsertData,
                                       [row["ID"], \
                                        row["NAME"], \
                                        row["ADDRESS"], \
                                        row["CITY"], \
                                        row["STATE"], \
                                        row["ZIP"], \
                                        row["TELEPHONE"], \
                                        row["TYPE"], \
                                        row["COUNTY"], \
                                        row["Y"], \
                                        row["X"], \
                                        row["NAICSCODE"], \
                                        row["NAICSDESCR"], \
                                        row["STATE_ID"]])
                    except Exception as e:
                        print " cursor execute insertData CSV exception: ID {}, {}".format(row["ID"], (e))
            conn.commit()
        except:
            print " csv dict exception"
except:
    print " exception Copy Downloaded HIFLD CSV to Staging Table"
print "Done"
print
        

print "Copy Downloaded HIFLD StateEOC CSV2 to SQL Staging Table..."
try:
    # Define the columns that data will be inserted into
    hifld_EmergencyCtr_Columns = "NAME, \
                                CITY, \
                                STATE, \
                                ZIPCODE, \
                                PHONE, \
                                Y, \
                                X"
    for state in existingDatabaseList:
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
                    # there are several records with funky ANSI 
                    # character, but not utf-8. Possibly not ASCII character.
##                    csvAddress = row["ADDRESS"].decode("utf-8").encode("ascii", "ignore")
##                    csvName = row["NAME"].decode("utf-8").encode("ascii", "ignore")
##                    csvCity = row["CITY"].decode("utf-8").encode("ascii", "ignore")
##                    csvTelephone = row["TELEPHONE"].decode("utf-8").encode("ascii", "ignore")
                    # This list order must match the order of the created table that it's being inserted into                 
                    sqlInsertData = "INSERT INTO ["+state+"]..[hifld_EmergencyCtr] ("\
                                    +hifld_EmergencyCtr_Columns+") \
                                    VALUES \
                                    (?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?)"
                    try:
                        cursor.execute(sqlInsertData,
                                       [row["NAME"], \
                                        row["STREET"], \
                                        row["CITY"], \
                                        row["STATE"], \
                                        row["ZIP"], \
                                        row["TELEPHONE"], \
                                        row["Y"], \
                                        row["X"]])
                    except Exception as e:
                        print " cursor execute insertData CSV2 exception: ID {}, {}".format(row["ID"], (e))
            conn.commit()
        except:
            print " csv2 dict exception"
except:
    print " exception Copy Downloaded HIFLD CSV to Staging Table"
print "Done"
print

print "Copy Downloaded HIFLD FEMA HQ CSV3 to SQL Staging Table..."
try:
    # Define the columns that data will be inserted into
    hifld_EmergencyCtr_Columns = "NAME, \
                                ADDRESS, \
                                CITY, \
                                STATE, \
                                ZIP, \
                                TYPE, \
                                Y, \
                                X"
    for state in existingDatabaseList:
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
                    # there are several records with funky ANSI 
                    # character, but not utf-8. Possibly not ASCII character.
##                    csvAddress = row["ADDRESS"].decode("utf-8").encode("ascii", "ignore")
##                    csvName = row["NAME"].decode("utf-8").encode("ascii", "ignore")
##                    csvCity = row["CITY"].decode("utf-8").encode("ascii", "ignore")
##                    csvTelephone = row["TELEPHONE"].decode("utf-8").encode("ascii", "ignore")
                    # This list order must match the order of the created table that it's being inserted into                 
                    sqlInsertData = "INSERT INTO ["+state+"]..[hifld_EmergencyCtr] ("\
                                    +hifld_EmergencyCtr_Columns+") \
                                    VALUES \
                                    (?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?, \
                                    ?)"
                    try:
                        cursor.execute(sqlInsertData,
                                       [row["Name"], \
                                        row["ST_ADDR"], \
                                        row["CITY_NAME"], \
                                        row["STATE"], \
                                        row["ZIP"], \
                                        row["TYPE"], \
                                        row["Y"], \
                                        row["X"]])
                    except Exception as e:
                        print " cursor execute insertData CSV3 exception: ID {}, {}".format(row["ID"], (e))
            conn.commit()
        except:
            print " csv3 dict exception"
except:
    print " exception Copy Downloaded HIFLD CSV's to Staging Table"
print "Done"
print


print "Calculate hifld_EmergencyCtr fields..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+";\
                        Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        hifldtable = "["+state+"]..[hifld_EmergencyCtr]"

        # EocId (State abbreviation plus 6 digits eg WA123456,
        # this must be unique and will persist across four tables.
        # IDSeq should be unique, non null and int)
        try:
            cursor.execute("UPDATE "+hifldtable+" SET EocId = '"+state+\
                           "' + RIGHT('000000'+cast(IDseq as varchar(6)),6)")
            conn.commit()
        except:
            print " cursor execute UPDATE EocId"

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
            updateData = "UPDATE "+hifldtable+" SET Usage='EmergencyCtr'"
            cursor.execute(updateData)
            conn.commit()
        except:
            print " cursor execute UPDATE Usage exception"

        # EfClass
        try:
            updateData = "UPDATE "+hifldtable+" SET EfClass = 'EFEO'"
            cursor.execute(updateData)
            conn.commit()
        except:
            print " cursor execute UPDATE EfClass exception"

        # Area
        try:
            # get value to use as default if another value is not found
            connectStringCDMS = "Driver={SQL Server};Server="+userDefinedServer+\
                                ";Database=CDMS;UID="+UserName+";PWD="+Password
            connCDMS = pyodbc.connect(connectStringCDMS, autocommit=False)
            cursorCDMS = connCDMS.cursor()
            cursorCDMS.execute("SELECT Occupancy,SquareFootage \
                                FROM [CDMS]..[hzSqftFactors] \
                                WHERE Occupancy = 'GOV2'")
            rows = cursorCDMS.fetchall()
            for row in rows:
                HazusDefaultSqFt = str(row.SquareFootage)
            # Update the Area field
            updateData = "UPDATE "+hifldtable+" SET Area = "+HazusDefaultSqFt
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
                
            # Get MeansCost based on GOV2
            cursorCDMS.execute("SELECT Occupancy, MeansCost \
                                FROM [CDMS]..[hzReplacementCost] \
                                WHERE Occupancy = 'GOV2'")
            rows = cursorCDMS.fetchall()
            for row in rows:
                MeansCost = str(row.MeansCost)

            # Get ContentValPct based on GOV2
            cursorCDMS.execute("SELECT Occupancy, ContentValPct \
                                FROM [CDMS]..[hzPctContentOfStructureValue] \
                                WHERE Occupancy = 'GOV2'")
            rows = cursorCDMS.fetchall()
            for row in rows:
                ContentValPct = str(row.ContentValPct)

            # Update Bldgcost
            updateData = "UPDATE "+hifldtable+" \
                        SET BldgCost = (CalcBldgSqFt * "+MeansCost+" * MeansAdjNonRes) / 1000"
            cursor.execute(updateData)
            conn.commit()

            # Update ContentsCost
            updateData = "UPDATE "+hifldtable+" \
                        SET ContentsCost = BldgCost * "+ContentValPct
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
            cursor.execute("SELECT MCHCHS_U_eqEmergencyCtr FROM [CDMS]..[eqEFBldgTypeDefault] \
                            WHERE StateID = '"+state+"'")
            rows = cursor.fetchall()
            for row in rows:
                MCHCHS_U_eqEmergencyCtr = row.MCHCHS_U_eqEmergencyCtr
            cursor.execute("SELECT MCHCHS_R_eqEmergencyCtr FROM [CDMS]..[eqEFBldgTypeDefault] \
                            WHERE StateID = '"+state+"'")
            rows = cursor.fetchall()
            for row in rows:
                MCHCHS_R_eqEmergencyCtr = row.MCHCHS_R_eqEmergencyCtr
            cursor.execute("SELECT PCLC_U_eqEmergencyCtr FROM [CDMS]..[eqEFBldgTypeDefault] \
                            WHERE StateID = '"+state+"'")
            rows = cursor.fetchall()
            for row in rows:
                PCLC_U_eqEmergencyCtr = row.PCLC_U_eqEmergencyCtr
            cursor.execute("SELECT PCLC_R_eqEmergencyCtr FROM [CDMS]..[eqEFBldgTypeDefault] \
                            WHERE StateID = '"+state+"'")
            rows = cursor.fetchall()
            for row in rows:
                PCLC_R_eqEmergencyCtr = row.PCLC_R_eqEmergencyCtr
        except:
            print " cursor execute GET State eqBldgTypes exception"
            
        # Set eqBldgType
        try:
            cursor.execute("UPDATE "+hifldtable+" SET eqBldgType=(CASE \
                            WHEN UATYP='U' AND (eqDesignLevel='MC' OR eqDesignLevel='HC' OR eqDesignLevel='HS') THEN '"+MCHCHS_U_eqEmergencyCtr+"' \
                            WHEN UATYP='R' AND (eqDesignLevel='MC' OR eqDesignLevel='HC' OR eqDesignLevel='HS') THEN '"+MCHCHS_R_eqEmergencyCtr+"' \
                            WHEN UATYP='U' AND (eqDesignLevel='PC' OR eqDesignLevel='LC') THEN '"+PCLC_U_eqEmergencyCtr+"' \
                            WHEN UATYP='R' AND (eqDesignLevel='PC' OR eqDesignLevel='LC') THEN '"+PCLC_R_eqEmergencyCtr+"' \
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
            
        # Hazus_model.dbo.flEmergencyCtrDflt; EFEO
        efClassList = ["EFEO"]
        for efClass in efClassList:
            try:
                cursor.execute("UPDATE a \
                                SET a.FoundationType=b.FoundationType, \
                                a.FirstFloorHt=b.FirstFloorHt, \
                                a.BldgDamageFnId=b.BldgDamageFnId, \
                                a.ContDamageFnId=b.ContDamageFnId, \
                                a.FloodProtection=b.FloodProtection \
                                FROM [Hazus_model]..[flEmergencyCtrDflt] b \
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
        
        # CONDITION DATA TO FIT WITHIN MAX LIMITS
        # Calculate the truncated fields
        try:
            cursor.execute("UPDATE "+hifldtable\
                           +" SET NameTRUNC = \
                            (CASE WHEN LEN(NAME)>40 THEN CONCAT(LEFT(NAME,37),'...') \
                            ELSE NAME END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET CommentTRUNC = \
                            (CASE WHEN LEN(ID)>40 THEN CONCAT(LEFT(ID,37),'...') \
                            ELSE ID END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET AddressTRUNC = \
                            (CASE WHEN LEN(ADDRESS)>40 THEN CONCAT(LEFT(ADDRESS,37),'...') \
                            ELSE ADDRESS END)")
            conn.commit()
        except Exception as e:
            print " cursor execute TRUNC Fields to be under 40 exception: {}".format((e))
            
except:
    print " exception Calculate hifld_EmergencyCtr Fields"
print "Done"
print


print "Move data from the HIFLD staging table to the HAZUS tables."
try:
    for state in existingDatabaseList:
        print state
        hifldTable = "["+state+"]..[hifld_EmergencyCtr]"
        hzTable = "["+state+"]..[hzEmergencyCtr]"
        flTable = "["+state+"]..[flEmergencyCtr]"
        eqTable = "["+state+"]..[eqEmergencyCtr]"
        
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()

        # Remove HAZUS rows
        print " Remove HAZUS rows from hzEmergencyCtr"
        try:
            cursor.execute("TRUNCATE TABLE "+hzTable)
            conn.commit()
        except:
            print " cursor execute Delete HAZUS from hzEmergencyCtr exception"
        print " done"
        
        print " Remove hazus rows from flEmergencyCtr"
        try:
            cursor.execute("TRUNCATE TABLE "+flTable)
            conn.commit()
        except:
            print " cursor execute Delete HAZUS from flEmergencyCtr exception"
        print " done"
        
        print " Remove hazus rows from eqEmergencyCtr"
        try:
            cursor.execute("TRUNCATE TABLE "+eqTable)
            conn.commit()
        except:
            print " cursor execute Delete HAZUS from eqEmergencyCtr exception"
        print " done"

        # Copy Rows from HIFLD to HAZUS hazard
        print " Copy rows from hifld_EmergencyCtr to hzEmergencyCtr..."
        try:
            cursor.execute("INSERT INTO "+hzTable+" \
                            (Shape, \
                            EocId, \
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
                            EocId, \
                            EfClass, \
                            CensusTractId, \
                            NameTrunc, \
                            AddressTRUNC, \
                            LEFT(City, 40), \
                            Zip, \
                            State, \
                            Telephone, \
                            MedianYearBuilt, \
                            Cost, \
                            Y, \
                            X, \
                            Area, \
                            ShelterCapacity, \
                            BackupPower, \
                            Kitchen, \
                            NAICSCODE \
                            \
                            FROM "+hifldTable+\
                            " WHERE EocId IS NOT NULL \
                            AND EfClass IS NOT NULL \
                            AND CensusTractId IS NOT NULL")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into hzEmergencyCtr exception: {}".format((e))
        print " done"
        
        # Copy Rows from HIFLD to HAZUS flood
        print " Copy rows from hifld_EmergencyCtr to flEmergencyCtr..."
        try:
            cursor.execute("INSERT INTO "+flTable+"\
                            (EocId, \
                            BldgType, \
                            DesignLevel, \
                            FoundationType, \
                            FirstFloorHt, \
                            BldgDamageFnId, \
                            ContDamageFnId, \
                            FloodProtection) \
                            \
                            SELECT \
                            EocId, \
                            BldgType, \
                            LEFT(eqDesignLevel,1), \
                            FoundationType, \
                            FirstFloorHt, \
                            BldgDamageFnId, \
                            ContDamageFnId, \
                            FloodProtection \
                            \
                            FROM "+hifldTable+\
                            " WHERE EocId IS NOT NULL \
                            AND CensusTractId IS NOT NULL")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into flEmergencyCtr exception: {}".format((e))
        print " done"
        
        # Copy Rows from HIFLD to HAZUS earthquake
        print " Copy rows from hifld_EmergencyCtr to eqEmergencyCtr..."
        try:
            cursor.execute("INSERT INTO "+eqTable+" \
                            (EocId, \
                            eqBldgType, \
                            DesignLevel, \
                            FoundationType, \
                            SoilType, \
                            LqfSusCat, \
                            LndSusCat, \
                            WaterDepth) \
                            \
                            SELECT \
                            EocId, \
                            eqBldgType, \
                            eqDesignLevel, \
                            0, \
                            'D', \
                            0, \
                            0, \
                            5 \
                            FROM "+hifldTable+\
                            " WHERE EocId IS NOT NULL \
                            AND eqBldgType IS NOT NULL \
                            AND eqDesignLevel IS NOT NULL \
                            AND CensusTractId IS NOT NULL")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into eqEmergencyCtr exception: {}".format((e))
        print " done"
        
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

    
print "Big Done."


