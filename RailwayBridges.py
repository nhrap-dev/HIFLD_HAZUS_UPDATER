# NiyamIT
# COLIN LINDEMAN, GIS Developer
# Proof of Concept - HIFLD RailwayBridges into HAZUS RailwayBridges.
# Last Update: 2019-09-04
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
configPath = ".\config.ini"
cfgParser = ConfigParser.ConfigParser()
cfgParser.read(configPath)
url = cfgParser.get("HIFLD OPEN DATA URLS", "RailwayBridges_CSV_URL")
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
    tempCSVPath = os.path.join(tempDir, "RailwayBridges.csv")
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


print "Create 'hifld_RailwayBridges' Staging Table..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        try:
            if cursor.tables(table="hifld_RailwayBridges", tableType="TABLE").fetchone():
                print " hifld_RailwayBridges already exists, dropping table..."
                cursor.execute("DROP TABLE hifld_RailwayBridges")
                conn.commit()
                print " done"
            else:
                print " hifld_RailwayBridges doesn't exist"
            print " Creating hifld_RailwayBridges table..."
            createTable = "CREATE TABLE hifld_RailwayBridges \
                            (RailwayBridgeID varchar(8), \
                            IDseq int IDENTITY(1,1), \
                            BRIDGECLASS varchar(5), \
                            CENSUSTRACTID varchar(11),\
                            STATE varchar(2),\
                            NAME varchar(200), \
                            NameTRUNC varchar(40), \
                            OWNER varchar(200), \
                            OwnerTRUNC varchar(25), \
                            BRIDGETYPE int, \
                            WIDTH int, \
                            NUMSPANS int, \
                            LENGTH int, \
                            MAXSPANLENGTH int, \
                            SKEWANGLE int, \
                            SEATLENGTH int, \
                            SEATWIDTH int, \
                            YEARBUILT int, \
                            YEARREMODELED int, \
                            PIERTYPE int, \
                            FOUNDATIONTYPE int, \
                            SCOURINDEX int, \
                            TRAFFIC int, \
                            TRAFFICINDEX int, \
                            CONDITION varchar(4), \
                            ConditionTRUNC varchar(3), \
                            COSTUSD numeric(38,8), \
                            LATITUDE float, \
                            LONGITUDE float, \
                            COMMENT varchar(4), \
                            SHAPE geometry)"
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
    hifld_RailwayBridges_Columns = "BRIDGECLASS, \
                                    NAME, \
                                    OWNER, \
                                    WIDTH, \
                                    NUMSPANS, \
                                    LENGTH, \
                                    MAXSPANLENGTH, \
                                    SKEWANGLE, \
                                    YEARBUILT, \
                                    PIERTYPE, \
                                    SCOURINDEX, \
                                    STATE, \
                                    TRAFFIC, \
                                    CONDITION, \
                                    COSTUSD, \
                                    LATITUDE, \
                                    LONGITUDE"
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
                    # This list order must match the order of the created table that it's being inserted into                 
                    sqlInsertData = "INSERT INTO ["+state+"]..[hifld_RailwayBridges] ("\
                                    +hifld_RailwayBridges_Columns+") \
                                    VALUES \
                                    (?, ?, ?, ?, ?,\
                                    ?, ?, ?, ?, ?,\
                                    ?, ?, ?, ?, ?,\
                                    ?, ?)"
                    try:
                        cursor.execute(sqlInsertData,
                                       ["RDFLT", \
                                        row["NAME"], \
                                        row["FACCARIED"], \
                                        0, \
                                        0, \
                                        0, \
                                        0, \
                                        0, \
                                        row["YEAR_BUILT"], \
                                        0, \
                                        "", \
                                        row["STATE"], \
                                        0, \
                                        "", \
                                        5000, \
                                        row["Y"], \
                                        row["X"]])
                        conn.commit()
                    except Exception as e:
                        print " cursor execute insertData CSV exception: ID {}".format((e))
        except Exception as e:
            print " csv dict exception: {}".format((e))
        RowCountCSV1Dict[state] = RowCountCSV1
except:
    print " exception Copy Downloaded HIFLD CSV to Staging Table"
print "Done"
print
        

print "Calculate hifld_RailwayBridges fields..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+";\
                        Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        hifldtable = "["+state+"]..[hifld_RailwayBridges]"

        # RailwayBridgesId (State abbreviation plus 6 digits eg WA123456,
        # this must be unique and will persist across four tables.
        # IDSeq should be unique, non null and int)
        try:
            cursor.execute("UPDATE "+hifldtable+" SET RailwayBridgeId = '"+state+\
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

        # Set yearbuilt 0 to NULL...
        try:
            cursor.execute("UPDATE "+hifldtable+" \
                            SET YEARBUILT = \
                            (CASE WHEN YEARBUILT = 0 THEN NULL \
                            ELSE YEARBUILT END)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE yearbuilt 0 to NULL exception: {}".format((e))
            
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
                            SET [COSTUSD] = (5000 * " + RSMeansNonResAvg + ")")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE CostUSD: {}".format((e))


        # EARTHQUAKE SPECIFIC FIELDS

        # FLOOD SPECIFIC FIELDS
        
        # CONDITION DATA TO FIT WITHIN MAX LIMITS
        # Calculate the truncated fields
        try:
            cursor.execute("UPDATE "+hifldtable\
                           +" SET NameTRUNC = \
                            (CASE WHEN LEN(NAME)>40 THEN CONCAT(LEFT(NAME,37),'...') \
                            ELSE NAME END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET ConditionTRUNC = \
                            (CASE WHEN LEN(CONDITION)>25 THEN CONCAT(LEFT(CONDITION,22),'...') \
                            ELSE CONDITION END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET OwnerTRUNC = \
                            (CASE WHEN LEN(OWNER)>3 THEN LEFT(OWNER,3) \
                            ELSE OWNER END)")
            conn.commit()
        except Exception as e:
            print " cursor execute TRUNC Fields to be under 40 exception: {}".format((e))
except:
    print " exception Calculate hifld_RailwayBridges Fields"
print "Done"
print



print "Move data from the HIFLD staging table to the HAZUS tables."
try:
    for state in existingDatabaseList:
        print state
        hifldTable = "["+state+"]..[hifld_RailwayBridges]"
        hzTable = "["+state+"]..[hzRailwayBridge]"
        flTable = "["+state+"]..[flRailwayBridge]"
        eqTable = "["+state+"]..[eqRailwayBridge]"
            
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                            ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()

        # Remove HAZUS rows
        print " Remove HAZUS rows from hzRailwayBridge"
        try:
             cursor.execute("TRUNCATE TABLE "+hzTable)
             conn.commit()
        except Exception as e:
            print " cursor execute Delete HAZUS from hzRailwayBridge exception: {}".format((e))
        print " done"
            
        print " Remove hazus rows from flRailwayBridge"
        try:
            cursor.execute("TRUNCATE TABLE "+flTable)
            conn.commit()
        except Exception as e:
            print " cursor execute Delete HAZUS from flRailwayBridge exception: {}".format((e))
        print " done"
        
        print " Remove hazus rows from eqRailwayBridge"
        try:
            cursor.execute("TRUNCATE TABLE "+eqTable)
            conn.commit()
        except Exception as e:
            print " cursor execute Delete HAZUS from eqRailwayBridge exception: {}".format((e))
        print " done"

        # Copy Rows from HIFLD to HAZUS hazard
        print " Copy rows from hifld_RailwayBridges to hzRailwayBridges..."
        try:
            cursor.execute("INSERT INTO "+hzTable+" \
                            (Shape, \
                            RailwayBridgeId, \
                            BridgeClass, \
                            Tract, \
                            Name, \
                            Owner, \
                            BridgeType, \
                            Width, \
                            NumSpans, \
                            Length, \
                            MaxSpanLength, \
                            SkewAngle, \
                            SeatLength, \
                            SeatWidth, \
                            YearBuilt, \
                            YearRemodeled, \
                            PierType, \
                            FoundationType, \
                            ScourIndex, \
                            Traffic, \
                            TrafficIndex, \
                            Condition, \
                            Cost, \
                            Latitude, \
                            Longitude, \
                            Comment) \
                            \
                            SELECT \
                            SHAPE, \
                            RailwayBridgeID, \
                            BRIDGECLASS, \
                            CENSUSTRACTID, \
                            NameTRUNC, \
                            OwnerTRUNC, \
                            BRIDGETYPE, \
                            WIDTH, \
                            NUMSPANS, \
                            LENGTH, \
                            MAXSPANLENGTH, \
                            SKEWANGLE, \
                            SEATLENGTH, \
                            SEATWIDTH, \
                            YEARBUILT, \
                            YEARREMODELED, \
                            PIERTYPE, \
                            FOUNDATIONTYPE, \
                            SCOURINDEX, \
                            TRAFFIC, \
                            TRAFFICINDEX, \
                            ConditionTRUNC, \
                            COSTUSD, \
                            LATITUDE, \
                            LONGITUDE, \
                            COMMENT \
                            \
                            FROM "+hifldTable+" \
                            WHERE RailwayBridgeId IS NOT NULL \
                            AND CensusTractId IS NOT NULL \
                            ORDER BY RailwayBridgeId ASC")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into hzRailwayBridge exception: {}".format((e))
        print " done"
        
        # Copy Rows from HIFLD to HAZUS flood
        print " Copy rows from hifld_RailwayBridges to flRailwayBridge..."
        try:
            cursor.execute("INSERT INTO "+flTable+"\
                            (RailwayBridgeId,\
                            Elevation) \
                            \
                            SELECT \
                            RailwayBridgeId, \
                            0 \
                            \
                            FROM "+hifldTable+\
                            " WHERE RailwayBridgeId IS NOT NULL \
                            AND CensusTractId IS NOT NULL \
                            ORDER BY RailwayBridgeId ASC")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into flRailwayBridge exception: {}".format((e))
        print " done"
        
        # Copy Rows from HIFLD to HAZUS earthquake
        print " Copy rows from hifld_RailwayBridges to eqRailwayBridge..."
        try:
            cursor.execute("INSERT INTO "+eqTable+" \
                            (RailwayBridgeId, \
                            SoilType, \
                            LqfSusCat, \
                            LndSusCat, \
                            WaterDepth) \
                            \
                            SELECT \
                            RailwayBridgeId, \
                            'D', \
                            0, \
                            0, \
                            5 \
                            FROM "+hifldTable+\
                            " WHERE RailwayBridgeId IS NOT NULL \
                            AND CensusTractId IS NOT NULL \
                            ORDER BY RailwayBridgeId ASC")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into eqRailwayBridge exception: {}".format((e))
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


