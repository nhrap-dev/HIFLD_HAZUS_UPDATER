# NiyamIT
# COLIN LINDEMAN, GIS Developer
# Proof of Concept - HIFLD ElectricPowerFlty into HAZUS ElectricPowerFlty.
# Last Update: 2019-10-19
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
url = cfgParser.get("HIFLD OPEN DATA URLS", "ElectricPowerFlty_CSV_URL")
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
    tempCSVPath = os.path.join(tempDir, "ElectricPowerFlty.csv")
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


print "Create hifld Staging Table..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        try:
            if cursor.tables(table="hifld_ElectricPowerFlty", tableType="TABLE").fetchone():
                print " hifld table already exists, dropping table..."
                cursor.execute("DROP TABLE hifld_ElectricPowerFlty")
                conn.commit()
                print " done"
            else:
                print " hifld table doesn't exist"
            print " Creating hifld table..."
            createTable = "CREATE TABLE hifld_ElectricPowerFlty \
                            (SHAPE geometry, \
                            ElectricPowerFltyID varchar(8), \
                            IDseq int IDENTITY(1,1), \
                            UtilFcltyCLASS varchar(5), \
                            CENSUSTRACTID varchar(11),\
                            NAME varchar(200), \
                            NameTRUNC varchar(40), \
                            ADDRESS varchar(200), \
                            AddressTRUNC varchar(40), \
                            CITY varchar(200), \
                            CityTrunc varchar(40), \
                            STATE varchar(2),\
                            ZIPCODE varchar(13),\
                            ZIPTRUNC varchar(5), \
                            OWNER varchar(200), \
                            OwnerTRUNC varchar(25), \
                            CONTACT varchar(400), \
                            ContactTRUNC varchar(40), \
                            PHONENUMBER varchar(50), \
                            PhoneNumberTRUNC varchar(14), \
                            Capacity float, \
                            COSTKUSD numeric(38,8), \
                            LATITUDE float, \
                            LONGITUDE float, \
                            COMMENT varchar(400), \
                            CommentTRUNC varchar(40))"
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
    hifld_BusFlty_Columns = "NAME, \
                                            ADDRESS, \
                                            CITY, \
                                            STATE, \
                                            ZIPCODE, \
                                            OWNER,  \
                                            CONTACT, \
                                            PHONENUMBER, \
                                            Capacity, \
                                            LATITUDE, \
                                            LONGITUDE,  \
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
                if row["STATE"] == state and float(row["OPER_CAP"]) > 10:
                    RowCountCSV1 += 1
                    # This list order must match the order of the created table that it's being inserted into                 
                    sqlInsertData = "INSERT INTO ["+state+"]..[hifld_ElectricPowerFlty] ("\
                                    +hifld_BusFlty_Columns+") \
                                    VALUES \
                                    (?,?,?,?,?,\
                                    ?,?,?,?,?,\
                                    ?,?)"
                    try:
                        cursor.execute(sqlInsertData,
                                       [row["NAME"], \
                                        row["ADDRESS"], \
                                        row["CITY"], \
                                        row["STATE"], \
                                        row["ZIP"], \
                                        row["OPERATOR"], \
                                        row["WEBSITE"], \
                                        row["TELEPHONE"], \
                                        row["OPER_CAP"], \
                                        row["Y"], \
                                        row["\xef\xbb\xbfX"],\
                                        row["TYPE"]])
                        conn.commit()
                    except Exception as e:
                        print " cursor execute insertData CSV exception(NAME={}): {}".format(row["NAME"], (e))
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
        hifldtable = "["+state+"]..[hifld_ElectricPowerFlty]"

        # ElectricPowerFltyId (State abbreviation plus 6 digits eg WA123456,
        # this must be unique and will persist across four tables.
        # IDSeq should be unique, non null and int)
        try:
            cursor.execute("UPDATE "+hifldtable+" SET ElectricPowerFltyId = '"+state+\
                           "' + RIGHT('000000'+cast(IDseq as varchar(6)),6)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE ElectricPowerFltyId: {}".format((e))

        # UtilFcltyClass...
        try:
            cursor.execute("UPDATE "+hifldtable\
                           +" SET UtilFcltyClass = \
                            (CASE WHEN [CAPACITY] > 10 AND [CAPACITY] < 100 THEN 'EPPS' \
                            WHEN [CAPACITY] > 99 AND [CAPACITY] < 501 THEN 'EPPM' \
                            WHEN [CAPACITY] > 500 THEN 'EPPL' \
                            END)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE UtilFcltyClass: {}".format((e))

        # PHONENUMBER cleanup...
        try:
            cursor.execute("UPDATE "+hifldtable+" \
                                        SET [PHONENUMBER] = NULL \
                                        WHERE [PHONENUMBER] = 'NOT AVAILABLE' ")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE PHONENUMBER: {}".format((e))
            
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
                            SET [COSTKUSD] = (CASE \
                            WHEN [UtilFcltyClass] = 'EPPS' THEN (174000 * " + RSMeansNonResAvg + ") \
                            WHEN [UtilFcltyClass] = 'EPPM' THEN (875000 * " + RSMeansNonResAvg + ") \
                            WHEN [UtilFcltyClass] = 'EPPL' THEN (875000 * " + RSMeansNonResAvg + ") \
                            END)")
            conn.commit()
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
            cursor.execute("UPDATE "+hifldtable+" \
                            SET [ZIPTRUNC]  = \
                            (CASE WHEN [ZIPCODE] = 'NOT AVAILABLE' THEN NULL \
                            WHEN [ZIPCODE] > 5 THEN LEFT([ZIPCODE], 5) \
                            ELSE [ZIPCODE] END)")
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
                           +" SET CommentTRUNC = \
                            (CASE WHEN LEN(COMMENT)>40 THEN CONCAT(LEFT(COMMENT,37),'...') \
                            ELSE COMMENT END)")
            conn.commit()
        except Exception as e:
            print " cursor execute TRUNC Fields to be under limit exception: {}".format((e))
except:
    print " exception Calculate hifld Fields"
print "Done"
print



print "Move data from the HIFLD staging table to the HAZUS tables."
try:
    for state in existingDatabaseList:
        print state
        hifldTable = "["+state+"]..[hifld_ElectricPowerFlty]"
        hzTable = "["+state+"]..[hzElectricPowerFlty]"
        eqTable = "["+state+"]..[eqElectricPowerFlty]"
        flTable = "["+state+"]..[flElectricPowerFlty]"
            
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

        print " Remove hazus rows from fl"
        try:
            cursor.execute("TRUNCATE TABLE "+flTable)
            conn.commit()
        except:
            print " cursor execute Delete HAZUS from fl exception"
        print " done"

        # Copy Rows from HIFLD to HAZUS hazard
        print " Copy rows from hifld to hz..."
        try:
            cursor.execute("INSERT INTO "+hzTable+" \
                            (Shape, \
                            ElectricPowerFltyID, \
                            UtilFcltyClass, \
                            Tract, \
                            [Name], \
                            [Address], \
                            City, \
                            Statea, \
                            Zipcode, \
                            Owner, \
                            Contact, \
                            PhoneNumber, \
                            Capacity, \
                            Cost, \
                            Latitude, \
                            Longitude, \
                            Comment)\
                            \
                            SELECT \
                            Shape, \
                            ElectricPowerFltyID, \
                            UtilFcltyCLASS, \
                            CENSUSTRACTID, \
                            NameTRUNC, \
                            AddressTRUNC, \
                            CityTRUNC, \
                            [STATE], \
                            [ZIPTRUNC], \
                            OwnerTRUNC, \
                            ContactTRUNC, \
                            PhonenumberTRUNC, \
                            Capacity, \
                            COSTKUSD, \
                            LATITUDE, \
                            LONGITUDE, \
                            CommentTrunc \
                            \
                            FROM "+hifldTable+\
                            " WHERE ElectricPowerFltyID IS NOT NULL \
                            AND CENSUSTRACTID IS NOT NULL \
                            ORDER BY ElectricPowerFltyID ASC")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into hz exception: {}".format((e))
        print " done"

        # Copy Rows from HIFLD to HAZUS earthquake
        print " Copy rows from hifld to eq.."
        try:
            cursor.execute("INSERT INTO "+eqTable+" \
                            (ElectricPowerFltyID, \
                            eqBldgType, \
                            DesignLevel, \
                            Anchor, \
                            SoilType, \
                            LqfSusCat, \
                            LndSusCat, \
                            WaterDepth) \
                            \
                            SELECT \
                            ElectricPowerFltyID, \
                            'DFLT', \
                            'LC', \
                            0, \
                            'D', \
                            0, \
                            0, \
                            5 \
                            FROM "+hifldTable+\
                            " WHERE ElectricPowerFltyID IS NOT NULL \
                            AND CENSUSTRACTID IS NOT NULL \
                            ORDER BY ElectricPowerFltyID ASC")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into eq exception: {}".format((e))
        print " done"

        # Copy Rows from HIFLD to HAZUS flood
        print " Copy rows from hifld to fl.."
        try:
            cursor.execute("INSERT INTO "+flTable+" \
                            (ElectricPowerFltyID, \
                            UtilIndicator, \
                            FoundationType, \
                            EquipmentHt, \
                            FloodProtection, \
                            UtilDamageFnId) \
                            \
                            SELECT \
                            ElectricPowerFltyID, \
                            1, \
                            7, \
                            0, \
                            0, \
                            66 \
                            FROM "+hifldTable+\
                            " WHERE ElectricPowerFltyID IS NOT NULL \
                            AND CENSUSTRACTID IS NOT NULL \
                            ORDER BY ElectricPowerFltyID ASC")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into fl exception: {}".format((e))
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


