# NiyamIT
# COLIN LINDEMAN, GIS Developer
# Proof of Concept - HIFLD LightRailFlty into HAZUS LightRailFlty.
# Last Update: 2019-09-08
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
configPath = "D:\Dropbox\NiyaMIT\Transportation Utility\config.ini"
cfgParser = ConfigParser.ConfigParser()
cfgParser.read(configPath)
url = cfgParser.get("HIFLD OPEN DATA URLS", "LightRailFlty_CSV_URL")
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
    tempCSVPath = os.path.join(tempDir, "LightRailFlty.csv")
    csvFile = urllib.urlopen(url).read()
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


print "Lookup StateFIPS and create tuple list"
FIPSDatabaseList = []
for database in possibleDatabaseList:
    try:
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+database+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT [StateFips] FROM [CDMS].[dbo].[cdms_syCounty] WHERE [State] = '"+database+"' GROUP BY [StateFips]")
            rows = cursor.fetchall()
            for row in rows:
                stateFIPS = (database, row.StateFips)
                FIPSDatabaseList.append(stateFIPS)
        except Exception as e:
            print "cursor execute state to statefips exception: {}".format((e))
    except Exception as e:
        print " exception checking existing database: {}".format((e))
print "Done"
print


print "Create 'hifld_LightRailFlty' Staging Table..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        try:
            if cursor.tables(table="hifld_LightRailFlty", tableType="TABLE").fetchone():
                print " hifld_LightRailFlty already exists, dropping table..."
                cursor.execute("DROP TABLE hifld_LightRailFlty")
                conn.commit()
                print " done"
            else:
                print " hifld_LightRailFlty doesn't exist"
            print " Creating hifld_LightRailFlty table..."
            createTable = "CREATE TABLE hifld_LightRailFlty \
                            (LightRailFltyID varchar(8), \
                            IDseq int IDENTITY(1,1), \
                            TranspFcltyCLASS varchar(5), \
                            CENSUSTRACTID varchar(11),\
                            NAME varchar(200), \
                            ADDRESS varchar(200), \
                            CITY varchar(200), \
                            STATE varchar(2),\
                            ZIPCODE varchar(5),\
                            OWNER varchar(200), \
                            CONTACT varchar(200), \
                            PHONENUMBER varchar(200), \
                            USAGE varchar(200), \
                            YEARBUILT int, \
                            NUMSTORIES int,  \
                            COSTKUSD numeric(38,8), \
                            BACKUPPOWER int, \
                            TRAFFIC int, \
                            LATITUDE float, \
                            LONGITUDE float, \
                            COMMENT varchar(4), \
                            ANCHOR int, \
                            FOUNDATIONTYPE int, \
                            EQBLDGTYPE varchar(4), \
                            DESIGNLEVEL varchar (2), \
                            SOILTYPE varchar(4), \
                            LQFSUSCAT int, \
                            LNDSUSCAT int, \
                            WATERDEPTH int, \
                            SHAPE geometry, \
                            STFIPS varchar(2))"
            cursor.execute(createTable)
            conn.commit()
            print " done"
        except:
            print " cursor execute createTable exception"
except:
    print " exception Staging Table"
print "Done"
print


print "Copy Downloaded HIFLD CSV to SQL Staging Table..."
RowCountCSV1Dict = {}
try:
    # Define the columns that data will be inserted into
    hifld_LightRailFlty_Columns = "TranspFcltyCLASS, \
                                                    NAME, \
                                                    ADDRESS, \
                                                    CITY, \
                                                    STATE, \
                                                    ZIPCODE,\
                                                    COSTKUSD, \
                                                    BACKUPPOWER, \
                                                    LATITUDE, \
                                                    LONGITUDE, \
                                                    ANCHOR, \
                                                    FOUNDATIONTYPE, \
                                                    EQBLDGTYPE, \
                                                    DESIGNLEVEL, \
                                                    SOILTYPE, \
                                                    LQFSUSCAT, \
                                                    LNDSUSCAT, \
                                                    WATERDEPTH , \
                                                    STFIPS"

    for item in FIPSDatabaseList:
        state = item[0]
        statefips = int(item[1])                
    #for state in existingDatabaseList:
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
                if int(row["STFIPS"]) == statefips:
                #if row["STATE"] == state:
                    RowCountCSV1 += 1
                    # This list order must match the order of the created table that it's being inserted into                 
                    sqlInsertData = "INSERT INTO ["+state+"]..[hifld_LightRailFlty] ("\
                                    +hifld_LightRailFlty_Columns+") \
                                    VALUES \
                                    (?, ?, ?, ?, ?,\
                                    ?, ?, ?, ?, ?,\
                                    ?, ?, ?, ?, ?,\
                                    ?, ?, ?, ?)"
                    try:
                        cursor.execute(sqlInsertData,
                                       ["LDFLT", \
                                        row["STATION"], \
                                        row["STR_ADD"], \
                                        row["CITY"], \
                                        state, \
                                        row["ZIPCODE"], \
                                        2600, \
                                        0, \
                                        row["Y"], \
                                        row["\xef\xbb\xbfX"], \
                                        0, \
                                        0, \
                                        "DFLT", \
                                        "LC", \
                                        "DFLT", \
                                        0, \
                                        0, \
                                        5,\
                                        row["STFIPS"]])
                        conn.commit()
                    except Exception as e:
                        print " cursor execute insertData CSV exception: {}".format((e))
        except Exception as e:
            print " csv dict exception: {}".format((e))
        RowCountCSV1Dict[state] = RowCountCSV1
except:
    print " exception Copy Downloaded HIFLD CSV to Staging Table"
print "Done"
print
        

print "Calculate hifld_LightRailFlty fields..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+";\
                        Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        hifldtable = "["+state+"]..[hifld_LightRailFlty]"

        # LightRailFltyId (State abbreviation plus 6 digits eg WA123456,
        # this must be unique and will persist across four tables.
        # IDSeq should be unique, non null and int)
        try:
            cursor.execute("UPDATE "+hifldtable+" SET LightRailFltyId = '"+state+\
                           "' + RIGHT('000000'+cast(IDseq as varchar(6)),6)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE RailwayBridgesId: {}".format((e))

        # CENSUS TRACTS ID      
        # Calculate Shape
        try:
            cursor.execute("UPDATE "+hifldtable+\
                           " SET Shape = geometry::Point(LONGITUDE, LATITUDE, 4326)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE Shape exception: {}".format((e))
        # Calculate TractID field...
        # To get all tract id's from hzTract based on the intersection of hzRailwayBridges and hztract...
        try:
            cursor.execute("UPDATE a \
                            SET a.CensusTractID = b.tract \
                            FROM ["+state+"]..[hzTract] b \
                            INNER JOIN "+hifldtable+" a \
                            ON b.shape.STIntersects(a.shape) = 1")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE CensusTractID exception: {}".format((e))
            
        # Cost
        try:
            connectStringCDMS = "Driver={SQL Server};Server="+userDefinedServer+\
                            ";Database=CDMS;UID="+UserName+";PWD="+Password
            connCDMS = pyodbc.connect(connectStringCDMS, autocommit=False)
            cursorCDMS = connCDMS.cursor()
            # Get StateRsMeans value...
            cursorCDMS.execute("SELECT [RSMeansNonResAvg] \
                            FROM [CDMS]..[StateRSMeans] \
                            WHERE StateID = '" + state + "'")
            rows = cursorCDMS.fetchall()
            for row in rows:
                RSMeansNonResAvg = str(row.RSMeansNonResAvg)
            # Update hifldtable...
            cursor.execute("UPDATE "+hifldtable+" \
                            SET [COSTKUSD] = (3200 * " + RSMeansNonResAvg + ")")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE CostKUSD: {}".format((e))


        # EARTHQUAKE SPECIFIC FIELDS

        # FLOOD SPECIFIC FIELDS        

        # CONDITION DATA TO FIT WITHIN MAX LIMITS

except:
    print " exception Calculate hifld_LightRailFlty Fields"
print "Done"
print



print "Move data from the HIFLD staging table to the HAZUS tables."
try:
    for state in existingDatabaseList:
        print state
        hifldTable = "["+state+"]..[hifld_LightRailFlty]"
        hzTable = "["+state+"]..[hzLightRailFlty]"
##        flTable = "["+state+"]..[flLightRailFlty]"
        eqTable = "["+state+"]..[eqLightRailFlty]"
            
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                            ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()

        # Remove HAZUS rows
        print " Remove HAZUS rows from hzLightRailFlty"
        try:
             cursor.execute("TRUNCATE TABLE "+hzTable)
             conn.commit()
        except:
            print " cursor execute Delete HAZUS from hzLightRailFlty exception"
        print " done"
            
##        print " Remove hazus rows from flLightRailFlty"
##        try:
##            cursor.execute("TRUNCATE TABLE "+flTable)
##            conn.commit()
##        except:
##            print " cursor execute Delete HAZUS from flLightRailFlty exception"
##        print " done"
        
        print " Remove hazus rows from eqLightRailFlty"
        try:
            cursor.execute("TRUNCATE TABLE "+eqTable)
            conn.commit()
        except:
            print " cursor execute Delete HAZUS from eqLightRailFlty exception"
        print " done"

        # Copy Rows from HIFLD to HAZUS hazard
        print " Copy rows from hifld to hz.."
        try:
            cursor.execute("INSERT INTO "+hzTable+" \
                            (Shape, \
                            LightRailFltyId, \
                            TranspFcltyClass, \
                            Tract, \
                            Name, \
                            Address, \
                            City, \
                            Statea, \
                            Zipcode, \
                            Owner, \
                            Contact, \
                            PhoneNumber, \
                            Usage, \
                            YearBuilt, \
                            NumStories, \
                            Cost, \
                            BackupPower, \
                            Traffic, \
                            Latitude, \
                            Longitude, \
                            Comment)\
                            \
                            SELECT \
                            Shape, \
                            LightRailFltyId, \
                            TranspFcltyClass, \
                            CENSUSTRACTID, \
                            NAME, \
                            ADDRESS, \
                            CITY, \
                            STATE, \
                            ZIPCODE, \
                            OWNER, \
                            CONTACT, \
                            PHONENUMBER, \
                            USAGE, \
                            YEARBUILT, \
                            NUMSTORIES, \
                            COSTKUSD, \
                            BACKUPPOWER, \
                            TRAFFIC, \
                            LATITUDE, \
                            LONGITUDE, \
                            COMMENT \
                            \
                            FROM "+hifldTable+\
                            " WHERE LightRailFltyId IS NOT NULL \
                            AND CENSUSTRACTID IS NOT NULL \
                            ORDER BY LightRailFltyId ASC")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into hz exception: {}".format((e))
        print " done"
        
##        # Copy Rows from HIFLD to HAZUS flood
##        print " Copy rows from hifld to fl..."
##        try:
##            cursor.execute("INSERT INTO "+flTable+"\
##                            (LightRailFltyId,\
##                            Elevation) \
##                            \
##                            SELECT \
##                            LightRailFltyId, \
##                            0 \
##                            \
##                            FROM "+hifldTable+\
##                            " WHERE LightRailFltyId IS NOT NULL \
##                            AND CENSUSTRACTID IS NOT NULL \
##                            ORDER BY LightRailFltyId ASC")
##            conn.commit()
##        except Exception as e:
##            print " cursor execute Insert Into fl exception: {}".format((e))
##        print " done"
        
        # Copy Rows from HIFLD to HAZUS earthquake
        print " Copy rows from hifld to eq..."
        try:
            cursor.execute("INSERT INTO "+eqTable+" \
                            (LightRailFltyId, \
                            Anchor, \
                            FoundationType, \
                            eqBldgType, \
                            DesignLevel, \
                            SoilType, \
                            LqfSusCat, \
                            LndSusCat, \
                            WaterDepth) \
                            \
                            SELECT \
                            LightRailFltyId, \
                            0, \
                            0, \
                            'DFLT', \
                            'LC', \
                            'D', \
                            0, \
                            0, \
                            5 \
                            \
                            FROM "+hifldTable+\
                            " WHERE LightRailFltyId IS NOT NULL \
                            AND CensusTractId IS NOT NULL \
                            ORDER BY LightRailFltyId ASC")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into eq exception: {}".format((e))
        print " done"
        
except:
    print " exception Move Data from Staging to HAZUS Tables"
print


####Cleanup shape field in staging table
####try:
####    for state in existingDatabaseList:
####        print state
####        hifldTable = "["+state+"]..[hifld_FireStation]"
####        # Drop Shape fields...
####        try:
####            cursor.execute("ALTER TABLE "+hifldTable+" DROP COLUMN Shape")
####            conn.commit()
####        except:
####            print " cursor execute DROP hifldTable Shape exception"
####except:
####    print " exception Clean up Shape field"

print "Big Done."


