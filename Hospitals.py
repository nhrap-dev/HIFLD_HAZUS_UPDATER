# NiyamIT
# COLIN LINDEMAN, GIS Developer
# Proof of Concept - HIFLD Hospitals into HAZUS CareFlty.
# Last Update: 2019-04-07
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
url = cfgParser.get("HIFLD OPEN DATA URLS", "CareFlty_URL")
url2 = cfgParser.get("HIFLD OPEN DATA URLS", "CareFlty2_URL")
userDefinedServer = cfgParser.get("SQL SERVER", "ServerName")
UserName = cfgParser.get("SQL SERVER", "UserName")
Password = cfgParser.get("SQL SERVER", "Password")
possibleDatabaseListRaw = cfgParser.get("DATABASE LIST", "possibleDatabaseList")
possibleDatabaseList = []
for database in possibleDatabaseListRaw.split(","):
    possibleDatabaseList.append(database)
userDefinedSqFt = cfgParser.get("HOSPITALS", "BedRoomSqFt")
print "Done"
print


print "Download CSV's..."
tempDir = tempfile.gettempdir()
#  for example: r'C:\Users\User1\AppData\Local\Temp'
# Download CSV 1
try:
    tempCSVPath = os.path.join(tempDir, "hospital.csv")
    csvFile = urllib.urlopen(url).read()
    with open(tempCSVPath, "w") as fx:
        fx.write(csvFile)
    fx.close()
except:
    print " exception downloading csv"
# Download CSV 2
try:
    tempCSV2Path = os.path.join(tempDir, "VAAdminFacilities.csv")
    csvFile = urllib.urlopen(url2).read()
    with open(tempCSV2Path, "w") as fx2:
        fx2.write(csvFile)
    fx2.close()
except:
    print " exception downloading csv2"
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


print "Create 'HIFLD_CareFlty' Staging Table..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        try:
            if cursor.tables(table="hifld_CareFlty", tableType="TABLE").fetchone():
                print " hifld_CareFlty already exists, dropping table..."
                cursor.execute("DROP TABLE hifld_CareFlty")
                conn.commit()
                print " done"
            else:
                print " hifld_CareFlty doesn't exist"
            print " Creating hifld_CareFlty table..."
            createTable = "CREATE TABLE hifld_CareFlty (ID varchar(50), \
                            NAME varchar(250), ADDRESS varchar(80), \
                            CITY varchar(50), STATE varchar(2), ZIP varchar(15), \
                            TELEPHONE varchar(30), TYPE varchar(20), \
                            STATUS varchar(10), POPULATION int, \
                            COUNTY varchar(30), COUNTYFIPS varchar(50), \
                            COUNTRY varchar(3), LATITUDE numeric(38,8), \
                            LONGITUDE numeric(38,8), NAICS_CODE varchar(50), \
                            NAICS_DESC varchar(255), SOURCE varchar(255), \
                            SOURCEDATE datetime2(7), VAL_METHOD varchar(30), \
                            VAL_DATE datetime2(7), WEBSITE varchar(255), \
                            STATE_ID varchar(50), ALT_NAME varchar(255), \
                            ST_FIPS varchar(50), OWNER varchar(50), \
                            TTL_STAFF int, BEDS int, TRAUMA varchar(50), \
                            HELIPAD varchar(50))"
            cursor.execute(createTable)
            conn.commit()
            print " done"
        except:
            print " cursor execute createTable exception"
except:
    print " exception Staging Table"
print "Done"
print


print "ADD HIFLD_CareFlty fields..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        hifldtable = "["+state+"]..[hifld_CareFlty]"
        try:
            cursor.execute("ALTER TABLE "+hifldtable+" ADD CareFltyId varchar(8), \
                            EfClass varchar(5), Usage varchar(10), \
                            Cost numeric(38,8), CalcBldgSqFt int, \
                            MeansAdjNonRes real, IDseq int IDENTITY(1,1), \
                            CensusTractID nvarchar(11), BldgSchemesId nvarchar(5), \
                            Shape geometry, MedianYearBuilt smallint, \
                            eqDesignLevel nvarchar(2), eqBldgType nvarchar(4), \
                            UATYP nvarchar(1), FoundationType char(1), \
                            FirstFloorHt float, BldgDamageFnId varchar(10), \
                            ContDamageFnId varchar(10), FloodProtection int, \
                            NameTRUNC nvarchar(40), ContactTRUNC nvarchar(40), \
                            CommentTRUNC nvarchar(40), BldgType nvarchar(50), \
                            AddressTRUNC nvarchar(40), FIPSCountyID nvarchar(5)")
            conn.commit()
        except:
            print " cursor ALTER TABLE exception"
except:
    print " exception ADD hifld_CareFlty fields"
print "Done"
print


print "Copy Downloaded HIFLD CSV to SQL Staging Table..."
# CSV 1
try:
    # Define the (30) columns that data will be inserted into
    hifld_CareFlty_Columns = "ID, NAME, ADDRESS, CITY, STATE, ZIP, TELEPHONE, \
                            TYPE, STATUS, POPULATION, COUNTY, COUNTYFIPS, COUNTRY, \
                            LATITUDE, LONGITUDE, NAICS_CODE, NAICS_DESC, SOURCE, \
                            SOURCEDATE, VAL_METHOD, VAL_DATE, WEBSITE, STATE_ID, \
                            ALT_NAME, ST_FIPS, OWNER, TTL_STAFF, BEDS, TRAUMA, \
                            HELIPAD"
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
                    csvAddress = row["ADDRESS"].decode("utf-8").encode("ascii", "ignore")
                    csvName = row["NAME"].decode("utf-8").encode("ascii", "ignore")
                    csvCity = row["CITY"].decode("utf-8").encode("ascii", "ignore")
                    csvAlt_Name = row["ALT_NAME"].decode("utf-8").encode("ascii", "ignore")
                    csvTelephone = row["TELEPHONE"].decode("utf-8").encode("ascii", "ignore")
                    csvWebsite = row["WEBSITE"].decode("utf-8").encode("ascii", "ignore")
                    # This list order must match the order of the created table that it's being inserted into                 
                    sqlInsertData = "INSERT INTO ["+state+"]..[hifld_CareFlty] ("\
                                    +hifld_CareFlty_Columns+") VALUES (?, ?, ?, ?, \
                                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    try:
                        cursor.execute(sqlInsertData, [row["ID"], csvName,\
                                        csvAddress, csvCity, row["STATE"],\
                                        row["ZIP"], csvTelephone,row["TYPE"],\
                                        row["STATUS"], row["POPULATION"],\
                                        row["COUNTY"], row["COUNTYFIPS"],\
                                        row["COUNTRY"], row["LATITUDE"],\
                                        row["LONGITUDE"], row["NAICS_CODE"],\
                                        row["NAICS_DESC"], row["SOURCE"],\
                                        row["SOURCEDATE"], row["VAL_METHOD"],\
                                        row["VAL_DATE"], csvWebsite,\
                                        row["STATE_ID"], csvAlt_Name,\
                                        row["ST_FIPS"], row["OWNER"],\
                                        row["TTL_STAFF"], row["BEDS"],\
                                        row["TRAUMA"], row["HELIPAD"]])
                    except:
                        print " cursor execute insertData CSV exception: ID {}".format(row["ID"])
            conn.commit()
        except:
            print " csv dict exception"
except:
    print " exception Copy Downloaded HIFLD CSV to Staging Table"
print "Done"
# CSV 2
try:
    # Define the columns that data will be inserted into
    hifld_CareFlty_Columns = "NAME, ADDRESS, CITY, STATE, ZIP, COUNTY, \
                            LATITUDE, LONGITUDE, NAICS_CODE, NAICS_DESC, \
                            COUNTYFIPS, BEDS, MedianYearBuilt"
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        # Iterate CSV and insert into sql
        try:
            f = open(tempCSV2Path)
            reader = csv.DictReader(f)
            for row in reader:
                if row["STATE"] == state:
                    sqlInsertData = "INSERT INTO ["+state+"]..[hifld_CareFlty] ("\
                                    +hifld_CareFlty_Columns+") \
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, -999, ?)"
                    try:
                        cursor.execute(sqlInsertData, [row["NAME"], \
                                        row["ADDRESS"], row["CITY"], \
                                        row["STATE"], row["ZIP"], \
                                        row["COUNTY"], row["LATITUDE"], \
                                        row["LONGITUDE"],row["NAICSCODE"], \
                                        row["NAICSDESCR"], row["FIPS"], \
                                        row["OPER_DATE"][:4]])
                    except:
                        print " cursor execute insertData CSV2 exception: NAME {}".format(row["NAME"])
            conn.commit()
        except:
            print " csv dict exception 2"
except:
    print " exception Copy Downloaded HIFLD CSV2 to Staging Table"
print "Done"
print
        

print "Calculate HIFLD_CareFlty fields..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        hifldtable = "["+state+"]..[hifld_CareFlty]"

        # CareFltyId (State abbreviation plus 6 digits eg WA123456,
        # this must be unique and will persist across four tables.
        # IDSeq should be unique, non null and int)
        try:
            cursor.execute("UPDATE "+hifldtable+" SET CareFltyId = '"+state+\
                           "' + RIGHT('000000'+cast(IDseq as varchar(6)),6)")
            conn.commit()
        except:
            print " cursor execute UPDATE CareFltyId"

        # Usage (set to Hospital)
        try:
            updateData = "UPDATE "+hifldtable+" SET Usage='Hospital'"
            cursor.execute(updateData)
            conn.commit()
        except:
            print " cursor execute UPDATE Usage exception"

        # EfClass (-999 should be EFHS)
        try:
            updateData = "UPDATE "+hifldtable+" SET EfClass= \
                        (CASE WHEN BEDS < 50 THEN 'EFHS' \
                        WHEN BEDS >= 50 AND BEDS <= 150 THEN 'EFHM' \
                        WHEN BEDS > 150 THEN 'EFHL' \
                        ELSE '' END)"
            cursor.execute(updateData)
            conn.commit()
        except:
            print " cursor execute UPDATE EfClass exception"

        # Building SqFt
        try:
            # get value to use as default if another value is not found
            connectStringCDMS = "Driver={SQL Server};Server="+userDefinedServer+\
                                ";Database=CDMS;UID="+UserName+";PWD="+Password
            connCDMS = pyodbc.connect(connectStringCDMS, autocommit=False)
            cursorCDMS = connCDMS.cursor()
            cursorCDMS.execute("SELECT Occupancy,SquareFootage FROM \
                                [CDMS]..[hzSqftFactors] WHERE Occupancy = 'COM6'")
            rows = cursorCDMS.fetchall()
            for row in rows:
                HazusDefaultSqFt = str(row.SquareFootage)
            # Update the CalcBldgSqFt field
            updateData = "UPDATE "+hifldtable+" SET CalcBldgSqFt = \
                        (CASE WHEN BEDS != -999 THEN BEDS*"+userDefinedSqFt+\
                        " ELSE "+HazusDefaultSqFt+" END)"
            cursor.execute(updateData)
            conn.commit()
        except:
            print " cursor execute UPDATE CalcBldgSqFt exception"

        # CENSUS TRACTS ID      
        # Calculate Shape
        try:
            cursor.execute("UPDATE "+hifldtable+\
                           " SET Shape = geometry::Point(LONGITUDE, LATITUDE, 4326)")
            conn.commit()
        except:
            print " cursor execute UPDATE Shape exception"
        # Calculate TractID field...
        # To get all tract id's from hzTract based on the intersection of hzcareflty and hztract...
        try:
            cursor.execute("UPDATE a SET a.CensusTractID = b.tract, \
                            a.BldgSchemesId = b.BldgSchemesId FROM ["+state+\
                            "]..[hzTract] b INNER JOIN "+hifldtable+\
                            " a ON b.shape.STIntersects(a.shape) = 1")
            conn.commit()
        except:
            print " cursor execute UPDATE tractid exception"
            
        # Update CountyFIPSID based on CensusTractID
        try:
            cursor.execute("UPDATE "+hifldtable+" SET [FIPSCountyID] = LEFT([CensusTractID], 5);")
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
                cursor.execute("UPDATE table1 SET table1.MeansAdjNonRes = table2.MeansAdjNonRes \
                                FROM "+hifldtable+" AS table1 \
                                LEFT JOIN ["+state+"]..[hzMeansCountyLocationFactor] as table2 \
                                ON [table1].[FIPSCountyID] = [table2].[CountyFIPS]")
                conn.commit()
            except:
                print "cursor execute UPDATE Calculate Cost exception - MeansAdjNonRes table join"
                
            # Get MeansCost based on COM6
            cursorCDMS.execute("SELECT Occupancy, MeansCost \
                                FROM [CDMS]..[hzReplacementCost] \
                                WHERE Occupancy = 'COM6'")
            rows = cursorCDMS.fetchall()
            for row in rows:
                MeansCost = str(row.MeansCost)

            # Get ContentValPct based on COM6
            cursorCDMS.execute("SELECT Occupancy, ContentValPct \
                                FROM [CDMS]..[hzPctContentOfStructureValue] \
                                WHERE Occupancy = 'COM6'")
            rows = cursorCDMS.fetchall()
            for row in rows:
                ContentValPct = str(row.ContentValPct)
            
            # Update the Cost field (in thousands of U.S. Dollars)
            userDefinedSqFt = str(450)
            updateData = "UPDATE "+hifldtable+" SET Cost=((CalcBldgSqFt*"+MeansCost+\
                         "*MeansAdjNonRes) + ((CalcBldgSqFt*"+MeansCost+")*"\
                         +ContentValPct+"))/1000"
            cursor.execute(updateData)
            conn.commit()
        except:
            print "cursor execute UPDATE Calculate Cost exception"

        # MedianYearBuilt
        try:
            # WA.dbo.hzDemographicsT; need to lookup by census tract id.
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
            cursor.execute("SELECT MCHCHS_U_eqCareFlty FROM [CDMS]..[eqEFBldgTypeDefault] \
                            WHERE StateID = '"+state+"'")
            rows = cursor.fetchall()
            for row in rows:
                MCHCHS_U_eqCareFlty = row.MCHCHS_U_eqCareFlty
            cursor.execute("SELECT MCHCHS_R_eqCareFlty FROM [CDMS]..[eqEFBldgTypeDefault] \
                            WHERE StateID = '"+state+"'")
            rows = cursor.fetchall()
            for row in rows:
                MCHCHS_R_eqCareFlty = row.MCHCHS_R_eqCareFlty
            cursor.execute("SELECT PCLC_U_eqCareFlty FROM [CDMS]..[eqEFBldgTypeDefault] \
                            WHERE StateID = '"+state+"'")
            rows = cursor.fetchall()
            for row in rows:
                PCLC_U_eqCareFlty = row.PCLC_U_eqCareFlty
            cursor.execute("SELECT PCLC_R_eqCareFlty FROM [CDMS]..[eqEFBldgTypeDefault] \
                            WHERE StateID = '"+state+"'")
            rows = cursor.fetchall()
            for row in rows:
                PCLC_R_eqCareFlty = row.PCLC_R_eqCareFlty
        except:
            print " cursor execute GET State eqBldgTypes exception"
            
        # Set eqBldgType
        try:
            cursor.execute("UPDATE "+hifldtable+" SET eqBldgType=(CASE \
                            WHEN UATYP='U' AND (eqDesignLevel='MC' OR eqDesignLevel='HC' OR eqDesignLevel='HS') THEN '"+MCHCHS_U_eqCareFlty+"' \
                            WHEN UATYP='R' AND (eqDesignLevel='MC' OR eqDesignLevel='HC' OR eqDesignLevel='HS') THEN '"+MCHCHS_R_eqCareFlty+"' \
                            WHEN UATYP='U' AND (eqDesignLevel='PC' OR eqDesignLevel='LC') THEN '"+PCLC_U_eqCareFlty+"' \
                            WHEN UATYP='R' AND (eqDesignLevel='PC' OR eqDesignLevel='LC') THEN '"+PCLC_R_eqCareFlty+"' \
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
            
        # Hazus_model.dbo.flCareFltyDflt; EFHL, EFHM, EFHS
        efClassList = ["EFHS", "EFHM", "EFHL"]
        for efClass in efClassList:
            try:
                cursor.execute("UPDATE a SET a.FoundationType=b.FoundationType, \
                                a.FirstFloorHt=b.FirstFloorHt, a.BldgDamageFnId=b.BldgDamageFnId, \
                                a.ContDamageFnId=b.ContDamageFnId, a.FloodProtection=b.FloodProtection \
                                FROM [Hazus_model]..[flCareFltyDflt] b \
                                INNER JOIN "+hifldtable+" a \
                                ON b.EFClass = a.EfClass \
                                WHERE a.EfClass = '"+efClass+"'")
                conn.commit()
            except:
                print " cursor execute calc flFoundationType, flFirstFloorHt, flBldgDamageFnId, flContDamageFnId, flFloodProtection exception"

        # Add 1 foot to firstfloorht for records whose MedianYearBuilt < EntryDate
        try:
            cursor.execute("UPDATE a SET a.FirstFloorHt = a.FirstFloorHt + 1 \
                            FROM "+hifldtable+" a \
                            LEFT JOIN (SELECT MAX(EntryDate) as EntryDateMAX, SUBSTRING(CensusBlock, 1,11) as CensusBlockTract \
                            FROM ["+state+"]..[flSchemeMapping] \
                            GROUP BY SUBSTRING(CensusBlock, 1,11)) b ON a.CensusTractID = b.CensusBlockTract \
                            WHERE a.MedianYearBuilt < b.EntryDateMax")
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
                           +" SET ContactTRUNC = \
                            (CASE WHEN LEN(SOURCE)>40 THEN CONCAT(LEFT(SOURCE,37),'...') \
                            ELSE SOURCE END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET CommentTRUNC = \
                            (CASE WHEN LEN(ID)>40 THEN CONCAT(LEFT(ID,37),'...') \
                            ELSE ID END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET AddressTRUNC = \
                            (CASE WHEN LEN(ADDRESS)>40 THEN CONCAT(LEFT(ADDRESS,37),'...') \
                            ELSE ADDRESS END)")
            conn.commit()
        except:
            print " cursor execute TRUNC Fields to be under 40 exception"
            
except:
    print " exception Calculate hifld_CareFlty Fields"
print "Done"
print


print "Move data from the HIFLD staging table to the HAZUS tables."
try:
    for state in existingDatabaseList:
        print state
        hifldTable = "["+state+"]..[hifld_CareFlty]"
        hzTable = "["+state+"]..[hzCareFlty]"
        flTable = "["+state+"]..[flCareFlty]"
        eqTable = "["+state+"]..[eqCareFlty]"
        
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()

        # THESE WILL NEED TO BE UPDATED TO ONLY TARGET HIFLD RECORDS
        # ONCE ALL FIELDS IN HIFLD.CAREFLTY ARE PRESENT
        print " Remove hifld rows from hzCareFlty"
        try:
            cursor.execute("DELETE FROM "+hzTable+\
                           " WHERE Name is null AND Comment is not null")
            conn.commit()
        except:
            print " cursor execute Delete hifld from hzCareFlty exception"
        print " done"
        
        print " Remove hifld rows from flCareFlty"
        try:
            cursor.execute("DELETE FROM "+flTable+\
                           " WHERE BldgType is null")
            conn.commit()
        except:
            print " cursor execute Delete hifld from flCareFlty exception"
        print " done"

        print " Remove hifld rows from eqCareFlty"
        try:
            cursor.execute("DELETE FROM "+eqTable+\
                           " WHERE FoundationType IS NOT NULL")
            conn.commit()
        except:
            print " cursor execute Delete hifld from eqCareFlty exception"
        print " done"

        # Copy Rows from HIFLD to HAZUS hazard
        print " Copy rows from hifld_CareFlty to hzCareFlty..."
        try:
            cursor.execute("INSERT INTO "+hzTable+" (Shape, CareFltyId, \
                            EfClass, Tract, Address, City, Zipcode, Statea, \
                            PhoneNumber, Usage, Cost, BackupPower, NumBeds, \
                            Latitude, Longitude, Comment) \
                            SELECT Shape, CareFltyId, EfClass, CensusTractId, \
                            AddressTRUNC, City, Zip, State, Telephone, Usage, \
                            Cost, 0, Beds, Latitude, Longitude, \
                            NAICS_CODE FROM "+hifldTable+\
                            " WHERE CareFltyId IS NOT NULL \
                            AND EfClass IS NOT NULL AND CensusTractId IS NOT NULL")
            conn.commit()
        except:
            print " cursor execute Insert Into hzCareFlty exception"
        print " done"
        
        # Copy Rows from HIFLD to HAZUS flood
        print " Copy rows from hifld_CareFlty to flCareFlty..."
        try:
            cursor.execute("INSERT INTO "+flTable+\
                            " (CareFltyId, DesignLevel) \
                            SELECT CareFltyId, 0 FROM "+hifldTable)
            conn.commit()
        except:
            print " cursor execute Insert Into flCareFlty exception"
        print " done"
        
        # Copy Rows from HIFLD to HAZUS earthquake
        print " Copy rows from hifld_CareFlty to eqCareFlty..."
        try:
            cursor.execute("INSERT INTO "+eqTable+" (CareFltyId, eqBldgType, \
                            DesignLevel, FoundationType, SoilType, LqfSusCat, \
                            LndSusCat, WaterDepth) SELECT CareFltyId, \
                            eqBldgType, eqDesignLevel, 0, 'D', 0, 0, 5 \
                            FROM "+hifldTable)
            conn.commit()
        except:
            print " cursor execute Insert Into eqCareFlty exception"
            print 
        print " done"


        print " Remove HAZUS rows from hzCareFlty"
        try:
            cursor.execute("")
            conn.commit()
        except:
            print " cursor execute Delete HAZUS from hzCareFlty exception"
        print "done"
        
        print " Remove hazus rows from flCareFlty"
        try:
            cursor.execute("")
            conn.commit()
        except:
            print " cursor execute Delete HAZUS from flCareFlty exception"
        print "done"
        
        print " Remove hazus rows from eqCareFlty"
        try:
            cursor.execute("")
            conn.commit()
        except:
            print " cursor execute Delete HAZUS from eqCareFlty exception"
        print "done"
        
except:
    print " exception Move Data from Staging to HAZUS Tables"
print


##Cleanup shape field in staging table
##try:
##    for state in existingDatabaseList:
##        print state
##        hifldTable = "["+state+"]..[hifld_CareFlty]"
##        # Drop Shape fields...
##        try:
##            cursor.execute("ALTER TABLE "+hifldTable+" DROP COLUMN Shape")
##            conn.commit()
##        except:
##            print " cursor execute DROP hifldTable Shape exception"
##except:
##    print " exception Clean up Shape field"


    
print "Big Done."


