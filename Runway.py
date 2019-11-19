# NiyamIT
# COLIN LINDEMAN, GIS Developer
# Proof of Concept - HIFLD RailwaySegments into HAZUS Runway.
# Last Update: 2019-10-08
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
configPath = "D:\Dropbox\NiyaMIT\Transportation Utility\config.ini"
cfgParser = ConfigParser.ConfigParser()
cfgParser.read(configPath)
url = cfgParser.get("HIFLD OPEN DATA URLS", "Runway_Shapefile_URL")
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
tempZipFilePath = os.path.join(tempDir, "Runway.zip")
tempShapefileFolder = os.path.join(tempDir, "Runway")
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
tempShapefilePath = os.path.join(tempShapefileFolder, "Runways.shp")
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


##print "Copy Downloaded HIFLD Shapefile to SQL Staging Table..."
##print "Drop the hifld table if it exists..."
##try:
##    for state in existingDatabaseList:
##        print state
##        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
##                        ";Database="+state+";UID="+UserName+";PWD="+Password
##        conn = pyodbc.connect(connectString, autocommit=False)
##        cursor = conn.cursor()
##        try:
##            if cursor.tables(table="hifld_Runway", tableType="TABLE").fetchone():
##                print " hifld table exists, dropping table..."
##                try:
##                    arcpy.DeleteFeatures_management()
##                except Exception as e:
##                    print " arcpy delete hifld table exception: {}".format((e))
####                cursor.execute("DROP TABLE hifld_Runway")
####                conn.commit()
##                print " done"
##        except Exception as e:
##            print " cursor drop hifld table exception: {}".format((e))
##except Exception as e:
##    print " delete hifld table sql server: {}".format((e))
##print "Copy shapefile to sql server..."
print "Copy Downloaded HIFLD Shapefile to SQL Staging Table..."
for state in existingDatabaseList:
    print state
    if state in ["AS", "IN", "OR"]:
        outPath ="E:/HazusData/SDEDatabaseConnections/_"+state+".sde"
    else:
        outPath ="E:/HazusData/SDEDatabaseConnections//"+state+".sde"
    outName="hifld_Runway"
    whereClause = "StateAbbv = '"+state+"'"
    FC = outPath + "." + outName
    stateSDE = state + ".sde"
    hzTract = state + ".dbo.hzTract"

##    print "Check if sde connections exists, if not create it..."
##    # If db connection does not exist, Create db connection...
##    if arcpy.Exists(FC) == False:
##        try:
##            arcpy.CreateDatabaseConnection_management(userDefinedSDEConnectionFolder,
##                                                                              stateSDE,
##                                                                              "SQL_SERVER",
##                                                                              userDefinedServer,
##                                                                              "DATABASE_AUTH",
##                                                                              UserName,
##                                                                              Password,
##                                                                              "SAVE_USERNAME",
##                                                                              state,
##                                                                              "#",
##                                                                              "TRANSACTIONAL",
##                                                                              "sde.DEFAULT")
##        except Exception as e:
##            print " cursor execute create db connection: {}".format((e))
    
    print "Delete hifld table if it exists, if yes delete it..."
    try:
        if arcpy.Exists(FC):
            arcpy.DeleteFeatures_management(FC)
    except Exception as e:
        print " arcpy delete hifld table exception: {}".format((e))
        
    print "Copy shapefile to sql server..."
    # Name Alias IsNullable Required Length Type Scale Precision MergeRule joinDelimiter DataSource OutputFieldName -1 -1
    try:
        arcpy.FeatureClassToFeatureClass_conversion(in_features=tempShapefilePath, \
                                        out_path=outPath, \
                                        out_name=outName, \
                                        where_clause=whereClause, \
                                        field_mapping='SiteNumber "SiteNumber" true true false 80 Text 0 0 ,First,#,Runways,SiteNumber,-1,-1;\
                                                                StateAbbv "StateAbbv" true true false 80 Text 0 0 ,First,#,Runways,StateAbbv,-1,-1;\
                                                                ID "ID" true true false 80 Text 0 0 ,First,#,Runways,ID,-1,-1;\
                                                                Length "Length" true true false 10 Long 0 10 ,First,#,Runways,Length_ft,-1,-1;\
                                                                Width "Width" true true false 10 Long 0 10 ,First,#,Runways,Width_ft,-1,-1;\
                                                                PCN "PCN" true true false 80 Text 0 0 ,First,#,Runways,PCN,-1,-1;\
                                                                LightsEdge "LightsEdge" true true false 80 Text 0 0 ,First,#,Runways,LightsEdge,-1,-1;\
                                                                LengthSour "LengthSour" true true false 80 Text 0 0 ,First,#,Runways,LengthSour,-1,-1;\
                                                                LengthSo_1 "LengthSo_1" true true false 80 Text 0 0 ,First,#,Runways,LengthSo_1,-1,-1;\
                                                                CapacitySi "CapacitySi" true true false 24 Double 15 23 ,First,#,Runways,CapacitySi,-1,-1;\
                                                                CapacityDu "CapacityDu" true true false 24 Double 15 23 ,First,#,Runways,CapacityDu,-1,-1;\
                                                                Capacity2D "Capacity2D" true true false 10 Long 0 10 ,First,#,Runways,Capacity2D,-1,-1;\
                                                                Capacity_1 "Capacity_1" true true false 10 Long 0 10 ,First,#,Runways,Capacity_1,-1,-1;\
                                                                ShapeSTLen "ShapeSTLen" true true false 24 Double 15 23 ,First,#,Runways,ShapeSTLen,-1,-1;\
                                                                Shape__Len "Shape__Len" true true false 80 Text 0 0 ,First,#,Runways,Shape__Len,-1,-1', \
                                                                config_keyword="")
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
            print " Adding fields to hifld table..."
            createTable = "ALTER TABLE hifld_Runway \
                            ADD NAME varchar(120), \
                            RUNWAYID varchar(8), \
                            CENSUSTRACTID varchar(11),\
                            CENTERXDD numeric(38,8), \
                            CENTERYDD numeric(38,8), \
                            COSTKUSD numeric(38, 8), \
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
    

print "Calculate hifld fields..."
try:
    for state in existingDatabaseList:
        print state
        if state in ["AS", "IN", "OR"]:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+";\
                        Database=_"+state+";UID="+UserName+";PWD="+Password
            hifldtable = "[_"+state+"]..[hifld_Runway]"
            stateSDE = "_" + state + ".sde"
            hzTract = "_" + state + ".dbo.hzTract"
        else:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+";\
                        Database="+state+";UID="+UserName+";PWD="+Password
            hifldtable = "["+state+"]..[hifld_Runway]"
            stateSDE = state + ".sde"
            hzTract = state + ".dbo.hzTract"
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        

        checkFeatureClass =  os.path.join(userDefinedSDEConnectionFolder, stateSDE, hzTract)

        # RailwaySegmentlId (State abbreviation plus 6 digits eg WA123456,
        # this must be unique and will persist across four tables.
        # IDSeq should be unique, non null and int)
        try:
            cursor.execute("UPDATE "+hifldtable+" SET RUNWAYID = \
            [StateAbbv] + RIGHT('000000'+cast([OBJECTID] as varchar(6)),6)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE RunwayId: {}".format((e))
            
        # Create centerpoints using arcpy...
        # Create data connection...
##        try:
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
                Runway = "_" + state + ".dbo.hifld_Runway"
            else:
                Runway = state + ".dbo.hifld_Runway"
            RunwayMidPointScratch = r"in_memory\hifld_Runwaymidpoint"
            inputpath = os.path.join(userDefinedSDEConnectionFolder, stateSDE, Runway)
            outputpathScratch = RunwayMidPointScratch
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
            arcpy.AddGeometryAttributes_management(RunwayMidPointScratch, "POINT_X_Y_Z_M")
        except Exception as e:
            print " cursor execute calculate scratch fields: {}".format((e))        
        # Arcpy copy from scratch to sql server...
        try:
            if state in ["AS", "IN", "OR"]:
                RunwayMidPoint = "[_"+state + "].dbo.hifld_Runwaymidpoint"
            else:
                RunwayMidPoint = "["+state + "].dbo.hifld_Runwaymidpoint"
            outputpath = os.path.join(userDefinedSDEConnectionFolder, stateSDE, RunwayMidPoint)
            arcpy.CopyFeatures_management(RunwayMidPointScratch, outputpath)
        except Exception as e:
            print " cursor execute copy from scratch to db: {}".format((e))

        # Assign censustractid to centerpoint and set centerpoint x,y ...
        try:
            if state in ["AS", "IN", "OR"]:
                cursor.execute("UPDATE a \
                            SET a.CENSUSTRACTID = b.tract \
                            FROM [_"+state+"]..[hzTract] b \
                            INNER JOIN "+RunwayMidPoint+" a \
                            ON b.shape.STIntersects(a.shape) = 1")
            else:
                cursor.execute("UPDATE a \
                            SET a.CENSUSTRACTID = b.tract \
                            FROM ["+state+"]..[hzTract] b \
                            INNER JOIN "+RunwayMidPoint+" a \
                            ON b.shape.STIntersects(a.shape) = 1")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE MIDPOINT CensusTractID exception: {}".format((e))

        # Join centerpoint to hifld table and set censustractid, CENTERXDD, CENTERYDD...
        try:
            cursor.execute("UPDATE a \
                            SET a.CENSUSTRACTID = b.CENSUSTRACTID, \
                            a.CENTERXDD = b.POINT_X, \
                            a.CENTERYDD = b.POINT_Y \
                            FROM "+RunwayMidPoint+" b \
                            INNER JOIN "+hifldtable+" a \
                            ON b.RUNWAYID = a.RUNWAYID")
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
                            SET [COSTKUSD] = ((CAST([Length] * [Width] AS BIGINT) * 95) / 1000) * " + RSMeansNonResAvg)
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE CostKUSD: {}".format((e))


        # EARTHQUAKE SPECIFIC FIELDS
   
        # CONDITION DATA TO FIT WITHIN MAX LIMITS

except Exception as e:
    print " exception Calculate hifld Fields: {}".format((e))
print "Done"
print



print "Move data from the HIFLD staging table to the HAZUS tables."
try:
    for state in existingDatabaseList:
        print state
        if state in ["AS", "IN", "OR"]:
            hifldTable = "[_"+state+"]..[hifld_Runway]"
            hzTable = "[_"+state+"]..[hzRunway]"
            eqTable = "[_"+state+"]..[eqRunway]"
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                                ";Database=_"+state+";UID="+UserName+";PWD="+Password
        else:
            hifldTable = "["+state+"]..[hifld_Runway]"
            hzTable = "["+state+"]..[hzRunway]"
            eqTable = "["+state+"]..[eqRunway]"
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                                ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()

        # Remove TEMP table...
        print " Remove hifld temp table..."
        try:
            if cursor.tables(table="hifld_RunwayMIDPOINT", tableType="TABLE").fetchone():
                print " dropping hifld temp table..."
                cursor.execute("DROP TABLE hifld_RunwayMIDPOINT")
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
                            RunwayId, \
                            TranspFcltyClass, \
                            Tract, \
                            Name, \
                            AirportID, \
                            RunwayLength, \
                            Cost, \
                            Latitude, \
                            Longitude, \
                            Comment)\
                            \
                            SELECT \
                            Shape, \
                            RunwayId, \
                            'ARW', \
                            CENSUSTRACTID, \
                            SiteNumber, \
                            ID, \
                            Length, \
                            COSTKUSD, \
                            CENTERYDD, \
                            CENTERXDD, \
                            COMMENT \
                            \
                            FROM "+hifldTable+\
                            " WHERE RunwayId IS NOT NULL \
                            AND CensusTractId IS NOT NULL \
                            ORDER BY RunwayId ASC")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into hz exception: {}".format((e))
        print " done"
      
        # Copy Rows from HIFLD to HAZUS earthquake
        print " Copy rows from hifld to eq..."
        try:
            cursor.execute("INSERT INTO "+eqTable+" \
                            (RunwayId, \
                            SoilType, \
                            LqfSusCat, \
                            LndSusCat, \
                            WaterDepth) \
                            \
                            SELECT \
                            RunwayId, \
                            'D', \
                            0, \
                            0, \
                            5 \
                            FROM "+hifldTable+\
                            " WHERE RunwayId IS NOT NULL \
                            AND CensusTractId IS NOT NULL \
                            ORDER BY RunwayId ASC")
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


