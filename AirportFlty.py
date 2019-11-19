# NiyamIT
# COLIN LINDEMAN, GIS Developer
# Proof of Concept - HIFLD PortFlty into HAZUS AirportFlty.
# Last Update: 2019-10-06
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
url = cfgParser.get("HIFLD OPEN DATA URLS", "AirportFlty_CSV_URL")
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
    tempCSVPath = os.path.join(tempDir, "AirportFlty.csv")
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


##print "Lookup StateFIPS and create tuple list"
##FIPSDatabaseList = []
##for database in possibleDatabaseList:
##    try:
##        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
##                        ";Database="+database+";UID="+UserName+";PWD="+Password
##        conn = pyodbc.connect(connectString, autocommit=False)
##        cursor = conn.cursor()
##        try:
##            cursor.execute("SELECT [StateFips] FROM [CDMS].[dbo].[cdms_syCounty] WHERE [State] = '"+database+"' GROUP BY [StateFips]")
##            rows = cursor.fetchall()
##            for row in rows:
##                stateFIPS = (database, row.StateFips)
##                FIPSDatabaseList.append(stateFIPS)
##        except Exception as e:
##            print "cursor execute state to statefips exception: {}".format((e))
##    except Exception as e:
##        print " exception checking existing database: {}".format((e))
##print "Done"
##print


print "Create 'hifld_AirportFlty' Staging Table..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        try:
            if cursor.tables(table="hifld_AirportFlty", tableType="TABLE").fetchone():
                print " hifld_AirportFlty already exists, dropping table..."
                cursor.execute("DROP TABLE hifld_AirportFlty")
                conn.commit()
                print " done"
            else:
                print " hifld_AirportFlty doesn't exist"
            print " Creating hifld_AirportFlty table..."
            createTable = "CREATE TABLE hifld_AirportFlty \
                            (AirportFltyID varchar(8), \
                            IDseq int IDENTITY(1,1), \
                            TranspFcltyCLASS varchar(5), \
                            CENSUSTRACTID varchar(11),\
                            NAME varchar(200), \
                            NameTRUNC varchar(40), \
                            ADDRESS varchar(200), \
                            AddressTRUNC varchar(40), \
                            CITY varchar(200), \
                            CityTrunc varchar(40), \
                            STATE varchar(2),\
                            ZIPCODE varchar(5),\
                            OWNER varchar(254), \
                            OwnerTRUNC varchar(25), \
                            CONTACT varchar(200), \
                            ContactTRUNC varchar(40), \
                            PHONENUMBER varchar(200), \
                            PhoneNumberTRUNC varchar(14), \
                            USAGE varchar(200), \
                            UsageTRUNC varchar(10), \
                            YEARBUILT int, \
                            COSTKUSD numeric(38,8), \
                            CARGO int,\
                            NUMFLIGHTS int,\
                            NumFlightsTRUNC smallint, \
                            ARRIVALS int, \
                            DEPARTURES int, \
                            NUMPASSENGERS int,\
                            NUMPASSENGERSTRUNC smallint, \
                            AreaSqft float, \
                            BACKUPPOWER int,\
                            LATITUDE float, \
                            LONGITUDE float, \
                            COMMENT varchar(254), \
                            CommentTRUNC varchar(40), \
                            URBAN varchar(1), \
                            SHAPE geometry)"
            cursor.execute(createTable)
            conn.commit()
            print " done"
        except Exception as e:
            print " cursor execute createTable exception: {}".format((e))
except:
    print " exception Staging Table"
print "Done"
print


print "Copy Downloaded HIFLD CSV to SQL Staging Table..."
RowCountCSV1Dict = {}
try:
    # Define the columns that data will be inserted into
    hifld_AirportFlty_Columns = "TranspFcltyCLASS, \
                                            NAME, \
                                            ADDRESS, \
                                            CITY, \
                                            STATE, \
                                            ZIPCODE,\
                                            CONTACT, \
                                            PHONENUMBER, \
                                            OWNER,\
                                            USAGE, \
                                            CARGO,\
                                            ARRIVALS,\
                                            DEPARTURES,\
                                            NUMPASSENGERS, \
                                            LATITUDE, \
                                            LONGITUDE,\
                                            COMMENT"
 
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
                if row["state_post_office_code"] == state and row["owner_type"] <> 'PR':
                    RowCountCSV1 += 1
                    # This list order must match the order of the created table that it's being inserted into                 
                    sqlInsertData = "INSERT INTO ["+state+"]..[hifld_AirportFlty] ("\
                                    +hifld_AirportFlty_Columns+") \
                                    VALUES \
                                    (?, ?, ?, ?, ?,\
                                    ?, ?, ?, ?, ?,\
                                    ?, ?, ?, ?, ?,\
                                    ?, ?)"
                    # get zipcode...
                    ownerCityStateZip = row["owner_city_state_zip"]
                    if len(ownerCityStateZip) > 0:
                        try:
                            z = ownerCityStateZip.split(",")
                            x = z[1].split(" ")
                            # only get the first five digits of the zipcode...
                            ownerZip = str(x[2])[0:5]
                            ownerCity = x[0]
                        except Exception as e:
                            ownerZip = ""
                            print " zipcode parse exception(objectid={}): {}".format(row["objectid"], (e))
                    else:
                        ownerZip = ""
                        ownerCity = ""
                    try:
                        cursor.execute(sqlInsertData,
                                       ["ADFLT", \
                                        row["fac_name"], \
                                        row["owner_address"], \
                                        ownerCity, \
                                        row["state_post_office_code"], \
                                        ownerZip, \
                                        row["manager_name"], \
                                        row["owner_phone"], \
                                        row["owner_name"], \
                                        row["fac_use"], \
                                        row["freightlbs"], \
                                        row["arrivals"], \
                                        row["departures"], \
                                        row["passengers"], \
                                        row["Y"], \
                                        row["\xef\xbb\xbfX"], \
                                        row["fac_type"]])
                        conn.commit()
                    except Exception as e:
                        print " cursor execute insertData CSV exception(objectid={}): {}".format(row["objectid"], (e))
        except Exception as e:
            print " csv dict exception: {}".format((e))
        RowCountCSV1Dict[state] = RowCountCSV1
except:
    print " exception Copy Downloaded HIFLD CSV to Staging Table"
print "Done"
print
        

print "Calculate hifld fields..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+";\
                        Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        hifldtable = "["+state+"]..[hifld_AirportFlty]"

        # LightRailFltyId (State abbreviation plus 6 digits eg WA123456,
        # this must be unique and will persist across four tables.
        # IDSeq should be unique, non null and int)
        try:
            cursor.execute("UPDATE "+hifldtable+" SET AirportFltyId = '"+state+\
                           "' + RIGHT('000000'+cast(IDseq as varchar(6)),6)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE AirportFltyId: {}".format((e))

        # CENSUS TRACTS ID      
        # Calculate Shape
        try:
            cursor.execute("UPDATE "+hifldtable+\
                           " SET Shape = geometry::Point(LONGITUDE, LATITUDE, 4326)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE Shape exception: {}".format((e))
        # Calculate TractID field...
        # To get all tract id's from hzTract based on the intersection of hzBusFlty and hztract...
        try:
            cursor.execute("UPDATE a \
                            SET a.CensusTractID = b.tract \
                            FROM ["+state+"]..[hzTract] b \
                            INNER JOIN "+hifldtable+" a \
                            ON b.shape.STIntersects(a.shape) = 1")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE CensusTractID exception: {}".format((e))

        # Calculate number of flights...
        try:
            cursor.execute("UPDATE "+hifldtable+\
                           " SET [numflights] = [arrivals] + [departures]")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE numflights exception: {}".format((e))

        # Set Backup Power NULL to 0...
        try:
            cursor.execute("UPDATE "+hifldtable+\
                           " SET [BackupPower] =  \
                            (CASE WHEN [BackupPower] IS NULL THEN 0 \
                            ELSE [BackupPower]  END)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE numflights exception: {}".format((e))

        # Urban or Rural...
        try:
            cursor.execute("UPDATE a \
                            SET a.URBAN = b.UATYP \
                            FROM [CDMS]..[CDMS_CENSUSURBANAREA] b \
                            INNER JOIN "+hifldtable+" a \
                            ON b.shape.STIntersects(a.shape) = 1")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE Urban: {}".format((e))

        # Area...
        try:
            cursor.execute("UPDATE "+hifldtable+\
                           " SET [AreaSqft] =  [NUMPASSENGERS] * 0.5")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE numflights exception: {}".format((e))
            
        # Cost...
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
                
            # Get COM4 MeansCost value...
            cursorCDMS.execute("SELECT [MeansCost] \
                            FROM [CDMS]..[hzReplacementCost] \
                            WHERE [Occupancy] = 'COM4' ")
            rowsB = cursorCDMS.fetchall()
            for rowB in rowsB:
                Com4MeansCost = str(rowB.MeansCost)
                
            # Calculate Cost 1 (area * com4sqft replacement cost)...
            try:
                cursor.execute("UPDATE "+hifldtable+" \
                                   SET [COSTKUSD] = ("+Com4MeansCost+" * [AreaSqft] / 1000)")
                conn.commit()
            except Exception as e:
                print " cursor execute UPDATE CostKUSD 1: {}".format((e))

            # This next step will overwrite the cost from the previous step based on other attributes...
            # Calculate Cost 2 (urban/rural default; lowvaluehighpassenger; all * rsmeansnonresavg)...
            try:
                cursor.execute("UPDATE "+hifldtable+" \
                                   SET [COSTKUSD] = CASE \
                                   WHEN [NUMPASSENGERS] < 10000 AND [URBAN] = 'U' THEN 12600 * " + RSMeansNonResAvg + " \
                                   WHEN [NUMPASSENGERS] < 10000 AND [URBAN] = 'C' OR [URBAN] IS NULL THEN 5000 * " + RSMeansNonResAvg + " \
                                   WHEN [NUMPASSENGERS] > 10000 AND [COSTKUSD] < 12600 THEN 12600  * " + RSMeansNonResAvg + " \
                                   ELSE [COSTKUSD]  * " + RSMeansNonResAvg + "\
                                   END")
                conn.commit()
            except Exception as e:
                print " cursor execute UPDATE CostKUSD 2: {}".format((e))
        except Exception as e:
            print " cursor execute UPDATE CostKUSD: {}".format((e))


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
                           +" SET AddressTRUNC = \
                            (CASE WHEN LEN(ADDRESS)>40 THEN LEFT(ADDRESS,37) \
                            ELSE ADDRESS END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET CityTRUNC = \
                            (CASE WHEN LEN(CITY)>40 THEN CONCAT(LEFT(CITY,37),'...') \
                            ELSE CITY END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET OwnerTRUNC = \
                            (CASE WHEN LEN(OWNER)>25 THEN CONCAT(LEFT(OWNER,22),'...') \
                            ELSE OWNER END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET ContactTRUNC = \
                            (CASE WHEN LEN(CONTACT)>40 THEN CONCAT(LEFT(CONTACT,37),'...') \
                            ELSE CONTACT END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET PhoneNumberTRUNC = \
                            (CASE WHEN LEN(PHONENUMBER)>14 THEN RIGHT(PHONENUMBER,14) \
                            ELSE PHONENUMBER END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET UsageTRUNC = \
                            (CASE WHEN LEN(USAGE)>10 THEN LEFT(USAGE,10) \
                            ELSE USAGE END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET CommentTRUNC = \
                            (CASE WHEN LEN(COMMENT)>40 THEN CONCAT(LEFT(COMMENT,37),'...') \
                            ELSE COMMENT END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET [NumFlightsTRUNC] = \
                            (CASE WHEN [NUMFLIGHTS] > 32767 THEN 32767 \
                            ELSE [NUMFLIGHTS] END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET [NUMPASSENGERSTRUNC] = \
                            (CASE WHEN [NUMPASSENGERS] > 32767 THEN 32767 \
                            ELSE [NUMPASSENGERS] END)")
            conn.commit()
        except Exception as e:
            print " cursor execute TRUNC Fields to be under limit exception: {}".format((e))
except:
    print " exception Calculate hifld_PortFlty Fields"
print "Done"
print



print "Move data from the HIFLD staging table to the HAZUS tables."
try:
    for state in existingDatabaseList:
        print state
        hifldTable = "["+state+"]..[hifld_AirportFlty]"
        hzTable = "["+state+"]..[hzAirportFlty]"
        eqTable = "["+state+"]..[eqAirportFlty]"
            
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                            ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()

        # Remove HAZUS rows
        print " Remove HAZUS rows from hz"
        try:
             cursor.execute("TRUNCATE TABLE "+hzTable)
             conn.commit()
        except:
            print " cursor execute Delete HAZUS from hz exception"
        print " done"
        
        print " Remove hazus rows from eq"
        try:
            cursor.execute("TRUNCATE TABLE "+eqTable)
            conn.commit()
        except:
            print " cursor execute Delete HAZUS from eq exception"
        print " done"

        # Copy Rows from HIFLD to HAZUS hazard
        print " Copy rows from hifld to hz..."
        try:
            cursor.execute("INSERT INTO "+hzTable+" \
                            (Shape, \
                            AirportFltyId, \
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
                            Cost, \
                            Cargo, \
                            NumFlights, \
                            NumPassengers, \
                            BackupPower, \
                            Latitude, \
                            Longitude, \
                            Comment)\
                            \
                            SELECT \
                            Shape, \
                            AirportFltyID, \
                            TranspFcltyCLASS, \
                            CENSUSTRACTID, \
                            NameTRUNC, \
                            AddressTRUNC, \
                            CityTRUNC, \
                            STATE, \
                            ZIPCODE, \
                            OwnerTRUNC, \
                            ContactTRUNC, \
                            PhonenumberTRUNC, \
                            UsageTRUNC, \
                            YEARBUILT, \
                            COSTKUSD, \
                            CARGO, \
                            NumFlightsTRUNC, \
                            NumPassengersTRUNC, \
                            BACKUPPOWER, \
                            LATITUDE, \
                            LONGITUDE, \
                            CommentTRUNC \
                            \
                            FROM "+hifldTable+\
                            " WHERE AirportFltyId IS NOT NULL \
                            AND CensusTractId IS NOT NULL \
                            ORDER BY AirportFltyId ASC")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into hzRailwayBridges exception: {}".format((e))
        print " done"
     
        # Copy Rows from HIFLD to HAZUS earthquake
        print " Copy rows from hifld to eq..."
        try:
            cursor.execute("INSERT INTO "+eqTable+" \
                            (AirportFltyId, \
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
                            AirportFltyId, \
                            0, \
                            '', \
                            'DFLT', \
                            'LC', \
                            'D', \
                            0, \
                            0, \
                            5 \
                            FROM "+hifldTable+\
                            " WHERE AirportFltyId IS NOT NULL \
                            AND CensusTractId IS NOT NULL \
                            ORDER BY AirportFltyId ASC")
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


