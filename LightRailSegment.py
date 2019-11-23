# NiyamIT
# COLIN LINDEMAN, GIS Developer
# Proof of Concept - HIFLD LightRailSegment into HAZUS LightRailSegment.
# Last Update: 2019-09-25
# Requirements:
#    Python 2.7, pyodbc
#    SQL Server 12.0.4237
#    ArcGIS 10.5
#    HAZUS be installed and config.ini updated with username and password;
#    See readme file.
#
# Tips on HAZUS DB
#    cdms_p_Counties has State and County names and FIPS


# Import necessary modules
import os
import tempfile
import urllib
import zipfile
from zipfile import ZipFile
import csv
import json
import pyodbc
import ConfigParser
import arcpy
arcpy.env.overwriteOutput = True


print "Read config.ini file..."
# User defined variables from .ini file...
# User needs to change config path
configPath = ".\config.ini"
cfgParser = ConfigParser.ConfigParser()
cfgParser.read(configPath)
url = cfgParser.get("HIFLD OPEN DATA URLS", "LightRailSegment_Shapefile_URL")
userDefinedServer = cfgParser.get("SQL SERVER", "ServerName")
UserName = cfgParser.get("SQL SERVER", "UserName")
Password = cfgParser.get("SQL SERVER", "Password")
userDefinedSDEConnectionFolder = cfgParser.get("SDE CONNECTION FOLDER", "Path")
possibleDatabaseListRaw = cfgParser.get("DATABASE LIST", "possibleDatabaseList")
possibleDatabaseList = []
for database in possibleDatabaseListRaw.split(","):
    possibleDatabaseList.append(database)
print "Done"
print


print "Download Shapefile..."
#  for example: r'C:\Users\User1\AppData\Local\Temp'
tempDir = tempfile.gettempdir()
tempZipFilePath = os.path.join(tempDir, "LightRailSegment.zip")
tempShapefileFolder = os.path.join(tempDir, "LightRailSegment")
try:
    urllib.urlretrieve(url, tempZipFilePath)
    print "Done"
except Exception as e:
    print " download zip file exception: {}".format((e))
print 
print "Unzip Shapefile..."
try:
    zf = ZipFile(tempZipFilePath)
    zf.extractall(tempShapefileFolder)
    zf.close()
    print "Done"
except Exception as e:
    print " unzip exception: {}".format((e))
# Define temp shapefile path...
# Name comes from the underlying data in the zipfile.
tempShapefilePath = os.path.join(tempShapefileFolder, "Fixed_Guideway_Transit_Links.shp")
print


print "Calculate length_geo for shapefile..."
try:
    arcpy.AddGeometryAttributes_management(Input_Features=tempShapefilePath, \
                                       Geometry_Properties="LENGTH_GEODESIC", \
                                       Length_Unit="KILOMETERS", \
                                       Area_Unit="", \
                                       Coordinate_System="PROJCS['USA_Contiguous_Equidistant_Conic',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Equidistant_Conic'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-96.0],PARAMETER['Standard_Parallel_1',33.0],PARAMETER['Standard_Parallel_2',45.0],PARAMETER['Latitude_Of_Origin',39.0],UNIT['Meter',1.0]]")
except Exception as e:
    print " calculate length_geo exception: {}".format((e))
print "Done"
print


print "Determine which of the 58 Databases exist..."
existingDatabaseList = []
for database in possibleDatabaseList:
    try:
        if database in ["AS", "IN", "OR"]:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database=_"+database+";UID="+UserName+";PWD="+Password
        else:
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
        if database in ["AS", "IN", "OR"]:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database=_"+database+";UID="+UserName+";PWD="+Password
        else:
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
print FIPSDatabaseList
print "Done"
print


##print "Create 'hifld_LightRailSegment' Staging Table..."
##try:
##    for state in existingDatabaseList:
##        print state
##        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
##                        ";Database="+state+";UID="+UserName+";PWD="+Password
##        conn = pyodbc.connect(connectString, autocommit=False)
##        cursor = conn.cursor()
##        try:
##            if cursor.tables(table="hifld_LightRailSegment", tableType="TABLE").fetchone():
##                print " hifld_LightRailSegment already exists, dropping table..."
##                cursor.execute("DROP TABLE hifld_LightRailSegment")
##                conn.commit()
##                print " done"
##            else:
##                print " hifld_LightRailSegment doesn't exist"
##            print " Creating hifld_LightRailSegment table..."
##            createTable = "CREATE TABLE hifld_LightRailSegment \
##                            (LightRailSegmentID varchar(8), \
##                            IDSEQ int IDENTITY(1,1), \
##                            SEGMENTCLASS varchar(5), \
##                            COUNTYFIPS varchar(11),\
##                            STATE varchar(2),\
##                            NAME varchar(200), \
##                            OWNER varchar(200), \
##                            LENGTHMETERS float, \
##                            NUMTRACKS int, \
##                            TRAFFIC int, \
##                            COSTKUSD numeric(38,8), \
##                            COMMENT varchar(4), \
##                            SHAPE geography)"
##            cursor.execute(createTable)
##            conn.commit()
##            print " done"
##        except Exception as e:
##            print " cursor execute createTable exception: {}".format((e))
##except Exception as e:
##    print " exception Staging Table: {}".format((e))
##print "Done"
##print


print "Copy Downloaded HIFLD Shapefile to SQL Staging Table..."
print "Drop the hifld_LightRailSegment table if it exists..."
try:
    for item in FIPSDatabaseList:
        state = item[0]
        print state
        if state in ["AS", "IN", "OR"]:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database=_"+state+";UID="+UserName+";PWD="+Password
        else:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        try:
            if cursor.tables(table="hifld_LightRailSegment", tableType="TABLE").fetchone():
                print " hifld_LightRailSegment exists, dropping table..."
                cursor.execute("DROP TABLE hifld_LightRailSegment")
                conn.commit()
                print " done"
        except Exception as e:
            print " cursor drop hifld_LightRailSegment exception: {}".format((e))
except Exception as e:
    print " delete hifld table sql server: {}".format((e))
print "Copy shapefile to sql server..."
try:
    for item in FIPSDatabaseList:
        state = item[0]
        print state
        statefips = item[1]
        if state in ["AS", "IN", "OR"]:
            outPath ="E:/HazusData/SDEDatabaseConnections/_"+state+".sde"
        else:
            outPath ="E:/HazusData/SDEDatabaseConnections//"+state+".sde"
        whereClause = "stfips = '"+statefips+"'"
        try:
            arcpy.FeatureClassToFeatureClass_conversion(in_features=tempShapefilePath, \
                                            out_path=outPath, \
                                            out_name="hifld_LightRailSegment", \
                                            where_clause=whereClause, \
                                            field_mapping='OBJECTID "OBJECTID" true true false 10 Long 0 10 ,First,#,Fixed_Guideway_Transit_Links,OBJECTID,-1,-1;\
                                                                    RECID "RECID" true true false 10 Long 0 10 ,First,#,Fixed_Guideway_Transit_Links,RECID,-1,-1;\
                                                                    STFIPS "STFIPS" true true false 80 Text 0 0 ,First,#,Fixed_Guideway_Transit_Links,STFIPS,-1,-1;\
                                                                    TR_TYPE "TR_TYPE" true true false 80 Text 0 0 ,First,#,Fixed_Guideway_Transit_Links,TR_TYPE,-1,-1;\
                                                                    TR_TYPE2 "TR_TYPE2" true true false 80 Text 0 0 ,First,#,Fixed_Guideway_Transit_Links,TR_TYPE2,-1,-1;\
                                                                    SYSTEM "SYSTEM" true true false 80 Text 0 0 ,First,#,Fixed_Guideway_Transit_Links,SYSTEM,-1,-1;\
                                                                    SYSTEM2 "SYSTEM2" true true false 80 Text 0 0 ,First,#,Fixed_Guideway_Transit_Links,SYSTEM2,-1,-1;\
                                                                    RTS_SRVD "RTS_SRVD" true true false 80 Text 0 0 ,First,#,Fixed_Guideway_Transit_Links,RTS_SRVD,-1,-1;\
                                                                    RTS_SRVD2 "RTS_SRVD2" true true false 80 Text 0 0 ,First,#,Fixed_Guideway_Transit_Links,RTS_SRVD2,-1,-1;\
                                                                    GRD_ELEV "GRD_ELEV" true true false 10 Long 0 10 ,First,#,Fixed_Guideway_Transit_Links,GRD_ELEV,-1,-1;\
                                                                    DIR "DIR" true true false 10 Long 0 10 ,First,#,Fixed_Guideway_Transit_Links,DIR,-1,-1;\
                                                                    UZA "UZA" true true false 80 Text 0 0 ,First,#,Fixed_Guideway_Transit_Links,UZA,-1,-1;\
                                                                    UACODE "UACODE" true true false 80 Text 0 0 ,First,#,Fixed_Guideway_Transit_Links,UACODE,-1,-1;\
                                                                    UACODE2 "UACODE2" true true false 80 Text 0 0 ,First,#,Fixed_Guideway_Transit_Links,UACODE2,-1,-1;\
                                                                    NTDID "NTDID" true true false 80 Text 0 0 ,First,#,Fixed_Guideway_Transit_Links,NTDID,-1,-1;\
                                                                    NTDID2 "NTDID2" true true false 80 Text 0 0 ,First,#,Fixed_Guideway_Transit_Links,NTDID2,-1,-1;\
                                                                    AMTRAK "AMTRAK" true true false 80 Text 0 0 ,First,#,Fixed_Guideway_Transit_Links,AMTRAK,-1,-1;\
                                                                    BEGSTA "BEGSTA" true true false 10 Long 0 10 ,First,#,Fixed_Guideway_Transit_Links,BEGSTA,-1,-1;\
                                                                    ENDSTA "ENDSTA" true true false 10 Long 0 10 ,First,#,Fixed_Guideway_Transit_Links,ENDSTA,-1,-1;\
                                                                    STATUS "STATUS" true true false 10 Long 0 10 ,First,#,Fixed_Guideway_Transit_Links,STATUS,-1,-1;\
                                                                    SOURCE "SOURCE" true true false 80 Text 0 0 ,First,#,Fixed_Guideway_Transit_Links,SOURCE,-1,-1;\
                                                                    Shape_Leng "Shape_Leng" true true false 24 Double 15 23 ,First,#,Fixed_Guideway_Transit_Links,Shape_Leng,-1,-1;\
                                                                    LENGTH_GEO "LENGTH_GEO" true true false 19 Double 0 0 ,First,#,Fixed_Guideway_Transit_Links,LENGTH_GEO,-1,-1', \
                                                                    config_keyword="")
        except Exception as e:
                print " exception featureclass to featureclass to sql server: {}".format((e))
except Exception as e:
    print " exception copy shapefile to sql server: {}".format((e))
print "Done"
print


print "Add hifld fields..."
try:
    for state in existingDatabaseList:
        print state
        if state in ["AS", "IN", "OR"]:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database=_"+state+";UID="+UserName+";PWD="+Password
        else:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        try:
            print " Adding fields to hifld_LightRailSegment table..."
            createTable = "ALTER TABLE hifld_LightRailSegment \
                            ADD LightRailSegmentID varchar(8), \
                            NameTRUNC varchar(40), \
                            OwnerTRUNC varchar(25), \
                            CENTERXDD numeric(38,8), \
                            CENTERYDD numeric(38,8), \
                            COUNTYFIPS varchar(5), \
                            COSTKUSD numeric(38,8), \
                            COMMENT varchar(40);"
            cursor.execute(createTable)
            conn.commit()
            print " done"
        except Exception as e:
            print " cursor execute alterTable exception: {}".format((e))
except Exception as e:
    print " exception Staging Table: {}".format((e))
print "Done"
print


print "Calculate hifld_LightRailSegment fields..."
try:
    for state in existingDatabaseList:
        print state
        if state in ["AS", "IN", "OR"]:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+";\
                        Database=_"+state+";UID="+UserName+";PWD="+Password
            hifldtable = "[_"+state+"]..[hifld_LightRailSegment]"
            stateSDE = "_" + state + ".sde"
            hzTract = "_" + state + ".dbo.hzTract"
        else:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+";\
                        Database="+state+";UID="+UserName+";PWD="+Password
            hifldtable = "["+state+"]..[hifld_LightRailSegment]"
            stateSDE = state + ".sde"
            hzTract = state + ".dbo.hzTract"
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        

        # LightRailSegmentId (State abbreviation plus 6 digits eg WA123456,
        # this must be unique and will persist across four tables.
        # IDSeq should be unique, non null and int)
        try:
            cursor.execute("UPDATE "+hifldtable+" \
                            SET [LightRailSegmentID] = '"+state+"' + RIGHT('000000'+cast([OBJECTID_1] as varchar(6)),6)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE LightRailSegmentID: {}".format((e))


        # Length in KM
        # Shape_Leng appears to be calculated in 3587 or 102100 (web mercator aux sphere) and represents meters. That should not be used.
##        try:
##            cursor.execute("UPDATE "+hifldtable+" \
##                            SET [LENGTHKM] = [Shape_Leng] / 1000")
##            conn.commit()
##        except Exception as e:
##            print " cursor execute UPDATE LengthKM: {}".format((e))

        # Create centerpoints using arcpy...
        # Create data connection...
##        try:
##            stateSDE = state + ".sde"
##            hzTract = state + ".dbo.hzTract"
##            checkFeatureClass =  os.path.join(userDefinedSDEConnectionFolder + stateSDE + hzTract)
##            # If db connection does not exist, Create db connection...
##            if arcpy.Exists(checkFeatureClass) == False:
##                arcpy.CreateDatabaseConnection_management(userDefinedSDEConnectionFolder,
##                                                                                      stateSDE,
##                                                                                      "SQL_SERVER",
##                                                                                      userDefinedServer,
##                                                                                      "DATABASE_AUTH",
##                                                                                      UserName,
##                                                                                      Password,
##                                                                                      "SAVE_USERNAME",
##                                                                                      state,
##                                                                                      "#",
##                                                                                      "TRANSACTIONAL",
##                                                                                      "sde.DEFAULT")
##        except Exception as e:
##            print " cursor execute create db connection: {}".format((e))
            
        # Arcpy create midpoint in in_memory workspace...
        try:
            if state in ["AS", "IN", "OR"]:
                LightrailSegment = "_" + state + ".dbo.hifld_LIGHTRAILSEGMENT"
            else:
                LightrailSegment = state + ".dbo.hifld_LIGHTRAILSEGMENT"
            LightrailSegmentMidPointScratch = r"in_memory\hifld_LIGHTRAILSEGMENTmidpoint"
            inputpath = os.path.join(userDefinedSDEConnectionFolder, stateSDE, LightrailSegment)
            outputpathScratch = LightrailSegmentMidPointScratch
            arcpy.GeneratePointsAlongLines_management(Input_Features = inputpath, \
                                                      Output_Feature_Class = outputpathScratch, \
                                                      Point_Placement = "PERCENTAGE", \
                                                      Distance = "", \
                                                      Percentage = "50", \
                                                      Include_End_Points = "")
        except Exception as e:
            print " cursor execute create centerpoints: {}".format((e))
        # Arcpy calculate midpoint x,y fields in scratch...
        try:
            arcpy.AddGeometryAttributes_management(LightrailSegmentMidPointScratch, "POINT_X_Y_Z_M")
        except Exception as e:
            print " cursor execute calculate scratch fields: {}".format((e))        
        # Arcpy copy from scratch to sql server...
        try:
            if state in ["AS", "IN", "OR"]:
                LightrailSegmentMidPoint = "_" + state + ".dbo.hifld_LightrailSegmentmidpoint"
            else:
                LightrailSegmentMidPoint = state + ".dbo.hifld_LightrailSegmentmidpoint"
            outputpath = os.path.join(userDefinedSDEConnectionFolder, stateSDE, LightrailSegmentMidPoint)
            arcpy.CopyFeatures_management(LightrailSegmentMidPointScratch, outputpath)
        except Exception as e:
            print " cursor execute copy from scratch to db: {}".format((e))
        
        # Assign censustractid to centerpoint and set centerpoint x,y ...
        try:
            if state in ["AS", "IN", "OR"]:
                cursor.execute("UPDATE a \
                            SET a.COUNTYFIPS = b.CountyFips \
                            FROM [_"+state+"]..[hzTract] b \
                            INNER JOIN "+LightrailSegmentMidPoint+" a \
                            ON b.shape.STIntersects(a.shape) = 1")
            else:
                cursor.execute("UPDATE a \
                            SET a.COUNTYFIPS = b.CountyFips \
                            FROM ["+state+"]..[hzTract] b \
                            INNER JOIN "+LightrailSegmentMidPoint+" a \
                            ON b.shape.STIntersects(a.shape) = 1")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE MIDPOINT CensusTractID exception: {}".format((e))

        # Join centerpoint to hifld table and set censustractid, CENTERXDD, CENTERYDD...
        try:
            cursor.execute("UPDATE a \
                            SET a.COUNTYFIPS = b.CountyFips, \
                            a.CENTERXDD = b.POINT_X, \
                            a.CENTERYDD = b.POINT_Y \
                            FROM "+LightrailSegmentMidPoint+" b \
                            INNER JOIN "+hifldtable+" a \
                            ON b.LightRailSegmentID = a.LightRailSegmentID")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE HIFLD CensusTractID exception: {}".format((e))

            
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
            rows2 = cursorCDMS.fetchall()
            for row2 in rows2:
                RSMeansNonResAvg = str(row2.RSMeansNonResAvg)
            # Update hifldtable...
            cursor.execute("UPDATE "+hifldtable+" \
                            SET [COSTKUSD] = ((1500 * [LENGTH_GEO]) * " + RSMeansNonResAvg + ")")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE CostKUSD: {}".format((e))


        # EARTHQUAKE SPECIFIC FIELDS
   
        # CONDITION DATA TO FIT WITHIN MAX LIMITS
        # Calculate the truncated fields
        try:
            cursor.execute("UPDATE "+hifldtable\
                           +" SET NameTRUNC = \
                            (CASE WHEN LEN(RTS_SRVD)>40 THEN CONCAT(LEFT(RTS_SRVD,37),'...') \
                            ELSE RTS_SRVD END)")
            conn.commit()
            cursor.execute("UPDATE "+hifldtable\
                           +" SET OwnerTRUNC = \
                            (CASE WHEN LEN(SYSTEM)>40 THEN CONCAT(LEFT(SYSTEM,37),'...') \
                            ELSE SYSTEM END)")
            conn.commit()
        except Exception as e:
            print " cursor execute TRUNC Fields to be under limit exception: {}".format((e))
except:
    print " exception Calculate hifld_LightRailSegmentFields"
print "Done"
print



print "Move data from the HIFLD staging table to the HAZUS tables."
try:
    for state in existingDatabaseList:
        print state
        if state in ["AS", "IN", "OR"]:
            hifldTable = "[_"+state+"]..[hifld_LightRailSegment]"
            hzTable = "[_"+state+"]..[hzLightRailSegment]"
            eqTable = "[_"+state+"]..[eqLightRailSegment]"
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                            ";Database=_"+state+";UID="+UserName+";PWD="+Password
        else:
            hifldTable = "["+state+"]..[hifld_LightRailSegment]"
            hzTable = "["+state+"]..[hzLightRailSegment]"
            eqTable = "["+state+"]..[eqLightRailSegment]"
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                            ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()

        # Remove TEMP table...
        print " Remove hifld temp table..."
        try:
            if cursor.tables(table="hifld_LightRailSegmentMIDPOINT", tableType="TABLE").fetchone():
                print " dropping hifld temp table..."
                cursor.execute("DROP TABLE hifld_LightRailSegmentMIDPOINT")
                conn.commit()
                print " done"
        except Exception as e:
            print " cursor drop hifld temp exception: {}".format((e))

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
                            LightRailSegId, \
                            SegmentClass, \
                            CountyFips, \
                            Name, \
                            Owner, \
                            Length, \
                            Cost, \
                            Comment) \
                            \
                            SELECT \
                            Shape, \
                            LightRailSegmentID, \
                            'LTR', \
                            COUNTYFIPS, \
                            NameTRUNC, \
                            OwnerTRUNC, \
                            [LENGTH_GEO], \
                            COSTKUSD, \
                            COMMENT \
                            \
                            FROM "+hifldTable+\
                            " WHERE LightRailSegmentID IS NOT NULL \
                            AND COUNTYFIPS IS NOT NULL \
                            ORDER BY LightRailSegmentID ASC")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into hz exception: {}".format((e))
        print " done"
        
        
        # Copy Rows from HIFLD to HAZUS earthquake
        print " Copy rows from hifld to eq..."
        try:
            cursor.execute("INSERT INTO "+eqTable+" \
                            (LightRailSegId) \
                            \
                            SELECT \
                            LightRailSegmentID \
                            \
                            FROM "+hifldTable+\
                            " WHERE LightRailSegmentID IS NOT NULL \
                            AND COUNTYFIPS IS NOT NULL \
                            ORDER BY LightRailSegmentID ASC")
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
####        hifldTable = "["+state+"]..[hifld_HighwayTunnel]"
####        # Drop Shape fields...
####        try:
####            cursor.execute("ALTER TABLE "+hifldTable+" DROP COLUMN Shape")
####            conn.commit()
####        except:
####            print " cursor execute DROP hifldTable Shape exception"
####except:
####    print " exception Clean up Shape field"

print "Big Done."


