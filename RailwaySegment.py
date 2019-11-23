# NiyamIT
# COLIN LINDEMAN, GIS Developer
# Proof of Concept - HIFLD RailwaySegments into HAZUS RailwaySegment.
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
url = cfgParser.get("HIFLD OPEN DATA URLS", "RailwaySegment_Shapefile_URL")
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
tempZipFilePath = os.path.join(tempDir, "RailwaySegment.zip")
tempShapefileFolder = os.path.join(tempDir, "RailwaySegment")
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
tempShapefilePath = os.path.join(tempShapefileFolder, "Railroads.shp")
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


print "Copy Downloaded HIFLD Shapefile to SQL Staging Table..."
print "Drop the hifld table if it exists..."
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
            if cursor.tables(table="hifld_RailwaySegment", tableType="TABLE").fetchone():
                print " hifld_RailwaySegment exists, dropping table..."
                cursor.execute("DROP TABLE hifld_RailwaySegment")
                conn.commit()
                print " done"
        except Exception as e:
            print " cursor drop hifld_RailwaySegment exception: {}".format((e))
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
        whereClause = "stfips = '"+state+"'"
        try:
##            arcpy.FeatureClassToFeatureClass_conversion(in_features=tempShapefilePath, \
##                                            out_path=outPath, \
##                                            out_name="hifld_RailwaySegment", \
##                                            where_clause=whereClause, \
##                                            field_mapping='objectid "objectid" true true false 10 Long 0 10 ,First,#,Railroads,objectid,-1,-1;\
##                                                                        fraarcid "fraarcid" true true false 10 Long 0 10 ,First,#,Railroads,fraarcid,-1,-1;\
##                                                                        frfranode "frfranode" true true false 10 Long 0 10 ,First,#,Railroads,frfranode,-1,-1;\
##                                                                        tofranode "tofranode" true true false 10 Long 0 10 ,First,#,Railroads,tofranode,-1,-1;\
##                                                                        stfips "stfips" true true false 80 Text 0 0 ,First,#,Railroads,stfips,-1,-1;\
##                                                                        cntyfips "cntyfips" true true false 80 Text 0 0 ,First,#,Railroads,cntyfips,-1,-1;\
##                                                                        stcntyfips "stcntyfips" true true false 80 Text 0 0 ,First,#,Railroads,stcntyfips,-1,-1;\
##                                                                        stateab "stateab" true true false 80 Text 0 0 ,First,#,Railroads,stateab,-1,-1;\
##                                                                        country "country" true true false 80 Text 0 0 ,First,#,Railroads,country,-1,-1;\
##                                                                        fraregion "fraregion" true true false 10 Long 0 10 ,First,#,Railroads,fraregion,-1,-1;\
##                                                                        rrowner1 "rrowner1" true true false 80 Text 0 0 ,First,#,Railroads,rrowner1,-1,-1;\
##                                                                        rrowner2 "rrowner2" true true false 80 Text 0 0 ,First,#,Railroads,rrowner2,-1,-1;\
##                                                                        rrowner3 "rrowner3" true true false 80 Text 0 0 ,First,#,Railroads,rrowner3,-1,-1;\
##                                                                        trkrghts1 "trkrghts1" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts1,-1,-1;\
##                                                                        trkrghts2 "trkrghts2" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts2,-1,-1;\
##                                                                        trkrghts3 "trkrghts3" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts3,-1,-1;\
##                                                                        trkrghts4 "trkrghts4" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts4,-1,-1;\
##                                                                        trkrghts5 "trkrghts5" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts5,-1,-1;\
##                                                                        trkrghts6 "trkrghts6" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts6,-1,-1;\
##                                                                        trkrghts7 "trkrghts7" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts7,-1,-1;\
##                                                                        trkrghts8 "trkrghts8" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts8,-1,-1;\
##                                                                        trkrghts9 "trkrghts9" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts9,-1,-1;\
##                                                                        subdiv "subdiv" true true false 80 Text 0 0 ,First,#,Railroads,subdiv,-1,-1;\
##                                                                        yardname "yardname" true true false 80 Text 0 0 ,First,#,Railroads,yardname,-1,-1;\
##                                                                        passngr "passngr" true true false 80 Text 0 0 ,First,#,Railroads,passngr,-1,-1;\
##                                                                        stracnet "stracnet" true true false 80 Text 0 0 ,First,#,Railroads,stracnet,-1,-1;\
##                                                                        tracks "tracks" true true false 10 Long 0 10 ,First,#,Railroads,tracks,-1,-1;\
##                                                                        net "net" true true false 80 Text 0 0 ,First,#,Railroads,net,-1,-1;\
##                                                                        miles "miles" true true false 24 Double 15 23 ,First,#,Railroads,miles,-1,-1;\
##                                                                        shape_leng "shape_leng" true true false 24 Double 15 23 ,First,#,Railroads,shape_leng,-1,-1', \
##                                            config_keyword="")
                arcpy.FeatureClassToFeatureClass_conversion(in_features=tempShapefilePath, \
                                            out_path=outPath, \
                                            out_name="hifld_RailwaySegment", \
                                            where_clause=whereClause, \
                                            field_mapping='objectid "objectid" true true false 10 Long 0 10 ,First,#,Railroads,objectid,-1,-1;\
                                                                        fraarcid "fraarcid" true true false 10 Long 0 10 ,First,#,Railroads,fraarcid,-1,-1;\
                                                                        frfranode "frfranode" true true false 10 Long 0 10 ,First,#,Railroads,frfranode,-1,-1;\
                                                                        tofranode "tofranode" true true false 10 Long 0 10 ,First,#,Railroads,tofranode,-1,-1;\
                                                                        stfips "stfips" true true false 80 Text 0 0 ,First,#,Railroads,stfips,-1,-1;\
                                                                        cntyfips "cntyfips" true true false 80 Text 0 0 ,First,#,Railroads,cntyfips,-1,-1;\
                                                                        stcntyfips "stcntyfips" true true false 80 Text 0 0 ,First,#,Railroads,stcntyfips,-1,-1;stateab "stateab" true true false 80 Text 0 0 ,First,#,Railroads,stateab,-1,-1;country "country" true true false 80 Text 0 0 ,First,#,Railroads,country,-1,-1;fraregion "fraregion" true true false 80 Text 0 0 ,First,#,Railroads,fraregion,-1,-1;rrowner1 "rrowner1" true true false 80 Text 0 0 ,First,#,Railroads,rrowner1,-1,-1;rrowner2 "rrowner2" true true false 80 Text 0 0 ,First,#,Railroads,rrowner2,-1,-1;rrowner3 "rrowner3" true true false 80 Text 0 0 ,First,#,Railroads,rrowner3,-1,-1;trkrghts1 "trkrghts1" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts1,-1,-1;trkrghts2 "trkrghts2" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts2,-1,-1;trkrghts3 "trkrghts3" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts3,-1,-1;trkrghts4 "trkrghts4" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts4,-1,-1;trkrghts5 "trkrghts5" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts5,-1,-1;trkrghts6 "trkrghts6" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts6,-1,-1;trkrghts7 "trkrghts7" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts7,-1,-1;trkrghts8 "trkrghts8" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts8,-1,-1;trkrghts9 "trkrghts9" true true false 80 Text 0 0 ,First,#,Railroads,trkrghts9,-1,-1;subdiv "subdiv" true true false 80 Text 0 0 ,First,#,Railroads,subdiv,-1,-1;passngr "passngr" true true false 80 Text 0 0 ,First,#,Railroads,passngr,-1,-1;stracnet "stracnet" true true false 80 Text 0 0 ,First,#,Railroads,stracnet,-1,-1;tracks "tracks" true true false 10 Long 0 10 ,First,#,Railroads,tracks,-1,-1;carddirect "carddirect" true true false 80 Text 0 0 ,First,#,Railroads,carddirect,-1,-1;net "net" true true false 80 Text 0 0 ,First,#,Railroads,net,-1,-1;miles "miles" true true false 24 Double 15 23 ,First,#,Railroads,miles,-1,-1;km "km" true true false 24 Double 15 23 ,First,#,Railroads,km,-1,-1;timezone "timezone" true true false 80 Text 0 0 ,First,#,Railroads,timezone,-1,-1;im_rt_type "im_rt_type" true true false 80 Text 0 0 ,First,#,Railroads,im_rt_type,-1,-1;ds "ds" true true false 80 Text 0 0 ,First,#,Railroads,ds,-1,-1',\
                                            config_keyword="")
                
        except Exception as e:
            print " exception  featureclass to featureclass to sql server: {}".format((e))
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
            print " Adding fields to hifld_RailwaySegment table..."
            createTable = "ALTER TABLE hifld_RailwaySegment \
                            ADD Name varchar(120), \
                            NameTRUNC varchar(40), \
                            RAILWAYSEGID varchar(8), \
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
    

print "Calculate hifld_RailwaySegment fields..."
try:
    for state in existingDatabaseList:
        print state
        if state in ["AS", "IN", "OR"]:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+";\
                        Database=_"+state+";UID="+UserName+";PWD="+Password
            hifldtable = "[_"+state+"]..[hifld_RailwaySegment]"
        else:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+";\
                        Database="+state+";UID="+UserName+";PWD="+Password
            hifldtable = "["+state+"]..[hifld_RailwaySegment]"
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()

        # Name
##        try:
##            cursor.execute("UPDATE "+hifldtable+" \
##                            SET [Name] = LTRIM([subdiv] + ' ' + [yardname])")
##            conn.commit()
##        except Exception as e:
##            print " cursor execute UPDATE Name: {}".format((e))
        try:
            cursor.execute("UPDATE "+hifldtable+" \
                            SET [Name] = LTRIM([subdiv])")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE Name: {}".format((e))
            
        # RailwaySegmentlId (State abbreviation plus 6 digits eg WA123456,
        # this must be unique and will persist across four tables.
        # IDSeq should be unique, non null and int)
        try:
            cursor.execute("UPDATE "+hifldtable+" SET RailwaySegId = \
            [stateab] + RIGHT('000000'+cast([OBJECTID_1] as varchar(6)),6)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE RailwaySegmentId: {}".format((e))
            
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
                            SET [COSTKUSD] = ((1500 * ([miles] * 1.60934)) * " + RSMeansNonResAvg + ")")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE CostKUSD: {}".format((e))


        # EARTHQUAKE SPECIFIC FIELDS
   
        # CONDITION DATA TO FIT WITHIN MAX LIMITS
        # Calculate the truncated fields
        try:
            cursor.execute("UPDATE "+hifldtable\
                           +" SET NameTRUNC = \
                            (CASE WHEN LEN(NAME)>40 THEN CONCAT(LEFT(NAME,37),'...') \
                            ELSE NAME END)")
            conn.commit()
        except Exception as e:
            print " cursor execute TRUNC Fields to be under limit exception: {}".format((e))
except:
    print " exception Calculate hifld_RailwaySegmentFields"
print "Done"
print



print "Move data from the HIFLD staging table to the HAZUS tables."
try:
    for state in existingDatabaseList:
        print state
        if state in ["AS", "IN", "OR"]:
            hifldTable = "[_"+state+"]..[hifld_RailwaySegment]"
            hzTable = "[_"+state+"]..[hzRailwaySegment]"
            eqTable = "[_"+state+"]..[eqRailwaySegment]"
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                            ";Database=_"+state+";UID="+UserName+";PWD="+Password
        else:
            hifldTable = "["+state+"]..[hifld_RailwaySegment]"
            hzTable = "["+state+"]..[hzRailwaySegment]"
            eqTable = "["+state+"]..[eqRailwaySegment]"
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
                            RailwaySegId, \
                            SegmentClass, \
                            CountyFips, \
                            Name, \
                            Owner, \
                            Length, \
                            NumTracks, \
                            Traffic, \
                            Cost, \
                            Comment)\
                            \
                            SELECT \
                            Shape, \
                            RAILWAYSEGID, \
                            'RTR', \
                            stcntyfips, \
                            NameTRUNC, \
                            rrowner1, \
                            ([miles] * 1.60934), \
                            tracks, \
                            '', \
                            COSTKUSD, \
                            COMMENT \
                            \
                            FROM "+hifldTable+\
                            " WHERE RAILWAYSEGID IS NOT NULL \
                            AND cntyfips IS NOT NULL \
                            ORDER BY RAILWAYSEGID ASC")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into hz exception: {}".format((e))
        print " done"
        
        
        # Copy Rows from HIFLD to HAZUS earthquake
        print " Copy rows from hifld to eq..."
        try:
            cursor.execute("INSERT INTO "+eqTable+" \
                            (RAILWAYSEGID) \
                            \
                            SELECT \
                            RAILWAYSEGID \
                            \
                            FROM "+hifldTable+\
                            " WHERE RAILWAYSEGID IS NOT NULL \
                            AND cntyfips IS NOT NULL \
                            ORDER BY RAILWAYSEGID ASC")
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


