# NiyamIT
# COLIN LINDEMAN, GIS Developer
# Proof of Concept - HIFLD HighwayTunnel into HAZUS HighwayTunnel.
# Last Update: 2019-10-30
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
url = cfgParser.get("HIFLD OPEN DATA URLS", "HighwayTunnel_Shapefile_URL")
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
tempZipFilePath = os.path.join(tempDir, "Road_Tunnels.zip")
tempShapefileFolder = os.path.join(tempDir, "Road_Tunnels")
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
tempShapefilePath = os.path.join(tempShapefileFolder, "Road_Tunnels.shp")
print


print "Calculate length_geo for shapefile..."
try:
    arcpy.AddGeometryAttributes_management(Input_Features=tempShapefilePath, \
                                       Geometry_Properties="LENGTH_GEODESIC", \
                                       Length_Unit="METERS", \
                                       Area_Unit="", \
                                       Coordinate_System="PROJCS['North_America_Equidistant_Conic',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Equidistant_Conic'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-96.0],PARAMETER['Standard_Parallel_1',20.0],PARAMETER['Standard_Parallel_2',60.0],PARAMETER['Latitude_Of_Origin',40.0],UNIT['Meter',1.0]]")
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


##print "Create 'hifld_HighwayTunnel' Staging Table..."
##try:
##    for state in existingDatabaseList:
##        print state
##        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
##                        ";Database="+state+";UID="+UserName+";PWD="+Password
##        conn = pyodbc.connect(connectString, autocommit=False)
##        cursor = conn.cursor()
##        try:
##            if cursor.tables(table="hifld_HighwayTunnel", tableType="TABLE").fetchone():
##                print " hifld_HighwayTunnel already exists, dropping table..."
##                cursor.execute("DROP TABLE hifld_HighwayTunnel")
##                conn.commit()
##                print " done"
##            else:
##                print " hifld_HighwayTunnel doesn't exist"



print "Copy Downloaded HIFLD Shapefile to SQL Staging Table..."
print "Drop the hifld table if it exists..."
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
            if cursor.tables(table="hifld_HighwayTunnelTEMP", tableType="TABLE").fetchone():
                print " hifld exists, dropping table..."
                cursor.execute("DROP TABLE hifld_HighwayTunnelTEMP")
                conn.commit()
                print " done"
        except Exception as e:
            print " cursor drop hifld exception: {}".format((e))
except Exception as e:
    print " delete hifld table sql server: {}".format((e))
print "Copy shapefile to sql server..."
try:
    for state in existingDatabaseList:
        print state
        
##        print "Check if sde connections exists, if not create it..."
##        # If db connection does not exist, Create db connection...
##        if arcpy.Exists(FC) == False:
##            try:
##                arcpy.CreateDatabaseConnection_management(userDefinedSDEConnectionFolder,
##                                                                                  stateSDE,
##                                                                                  "SQL_SERVER",
##                                                                                  userDefinedServer,
##                                                                                  "DATABASE_AUTH",
##                                                                                  UserName,
##                                                                                  Password,
##                                                                                  "SAVE_USERNAME",
##                                                                                  state,
##                                                                                  "#",
##                                                                                  "TRANSACTIONAL",
##                                                                                  "sde.DEFAULT")
##            except Exception as e:
##                print " cursor execute create db connection: {}".format((e))
##        
##        print "Delete hifld table if it exists, if yes delete it..."
##        try:
##            if arcpy.Exists(FC):
##                arcpy.DeleteFeatures_management(FC)
##        except Exception as e:
##            print " arcpy delete hifld table exception: {}".format((e))

        if state in ["AS", "IN", "OR"]:
            outPath ="E:/HazusData/SDEDatabaseConnections/_"+state+".sde"
        else:
            outPath ="E:/HazusData/SDEDatabaseConnections//"+state+".sde"
        whereClause = "state = '"+state+"'"
        try:
            arcpy.FeatureClassToFeatureClass_conversion(in_features=tempShapefilePath, \
                                            out_path=outPath, \
                                            out_name="hifld_HighwayTunnelTEMP", \
                                            where_clause=whereClause, \
                                            field_mapping='OBJECTID "OBJECTID" true true false 10 Long 0 10 ,First,#,Road_Tunnels,OBJECTID,-1,-1;NGAID "NGAID" true true false 80 Text 0 0 ,First,#,Road_Tunnels,NGAID,-1,-1;NAME "NAME" true true false 80 Text 0 0 ,First,#,Road_Tunnels,NAME,-1,-1;ADDRESS "ADDRESS" true true false 80 Text 0 0 ,First,#,Road_Tunnels,ADDRESS,-1,-1;CITY "CITY" true true false 80 Text 0 0 ,First,#,Road_Tunnels,CITY,-1,-1;STATE "STATE" true true false 80 Text 0 0 ,First,#,Road_Tunnels,STATE,-1,-1;COUNTY "COUNTY" true true false 80 Text 0 0 ,First,#,Road_Tunnels,COUNTY,-1,-1;FIPS "FIPS" true true false 80 Text 0 0 ,First,#,Road_Tunnels,FIPS,-1,-1;DIRECTIONS "DIRECTIONS" true true false 175 Text 0 0 ,First,#,Road_Tunnels,DIRECTIONS,-1,-1;GEODATE "GEODATE" true true false 80 Text 0 0 ,First,#,Road_Tunnels,GEODATE,-1,-1;GEOHOW "GEOHOW" true true false 80 Text 0 0 ,First,#,Road_Tunnels,GEOHOW,-1,-1;NAICSCODE "NAICSCODE" true true false 80 Text 0 0 ,First,#,Road_Tunnels,NAICSCODE,-1,-1;NAICSDESCR "NAICSDESCR" true true false 80 Text 0 0 ,First,#,Road_Tunnels,NAICSDESCR,-1,-1;SOURCE "SOURCE" true true false 80 Text 0 0 ,First,#,Road_Tunnels,SOURCE,-1,-1;STATE_CODE "STATE_CODE" true true false 80 Text 0 0 ,First,#,Road_Tunnels,STATE_CODE,-1,-1;COUNTY1 "COUNTY1" true true false 80 Text 0 0 ,First,#,Road_Tunnels,COUNTY1,-1,-1;RTESIGNPRE "RTESIGNPRE" true true false 80 Text 0 0 ,First,#,Road_Tunnels,RTESIGNPRE,-1,-1;DESLEVSVC "DESLEVSVC" true true false 80 Text 0 0 ,First,#,Road_Tunnels,DESLEVSVC,-1,-1;LOCATION "LOCATION" true true false 80 Text 0 0 ,First,#,Road_Tunnels,LOCATION,-1,-1;QLATITUDE "QLATITUDE" true true false 80 Text 0 0 ,First,#,Road_Tunnels,QLATITUDE,-1,-1;QLONGITUDE "QLONGITUDE" true true false 80 Text 0 0 ,First,#,Road_Tunnels,QLONGITUDE,-1,-1;DETOURLGTH "DETOURLGTH" true true false 80 Text 0 0 ,First,#,Road_Tunnels,DETOURLGTH,-1,-1;TOLL "TOLL" true true false 80 Text 0 0 ,First,#,Road_Tunnels,TOLL,-1,-1;MAINTNCE "MAINTNCE" true true false 80 Text 0 0 ,First,#,Road_Tunnels,MAINTNCE,-1,-1;OWNER "OWNER" true true false 80 Text 0 0 ,First,#,Road_Tunnels,OWNER,-1,-1;CLASINVRTE "CLASINVRTE" true true false 80 Text 0 0 ,First,#,Road_Tunnels,CLASINVRTE,-1,-1;YEAR_BUILT "YEAR_BUILT" true true false 80 Text 0 0 ,First,#,Road_Tunnels,YEAR_BUILT,-1,-1;LNSONSTRUC "LNSONSTRUC" true true false 80 Text 0 0 ,First,#,Road_Tunnels,LNSONSTRUC,-1,-1;DESIGNLOAD "DESIGNLOAD" true true false 80 Text 0 0 ,First,#,Road_Tunnels,DESIGNLOAD,-1,-1;APPRRDWDTH "APPRRDWDTH" true true false 10 Long 0 10 ,First,#,Road_Tunnels,APPRRDWDTH,-1,-1;SVCONBRDG "SVCONBRDG" true true false 80 Text 0 0 ,First,#,Road_Tunnels,SVCONBRDG,-1,-1;MATLDESIGN "MATLDESIGN" true true false 80 Text 0 0 ,First,#,Road_Tunnels,MATLDESIGN,-1,-1;IRTEHORZCL "IRTEHORZCL" true true false 80 Text 0 0 ,First,#,Road_Tunnels,IRTEHORZCL,-1,-1;DECK "DECK" true true false 80 Text 0 0 ,First,#,Road_Tunnels,DECK,-1,-1;SUPERSTRUC "SUPERSTRUC" true true false 80 Text 0 0 ,First,#,Road_Tunnels,SUPERSTRUC,-1,-1;INSPCTDATE "INSPCTDATE" true true false 80 Text 0 0 ,First,#,Road_Tunnels,INSPCTDATE,-1,-1;STRAHNETHD "STRAHNETHD" true true false 80 Text 0 0 ,First,#,Road_Tunnels,STRAHNETHD,-1,-1;PARSTRDSGN "PARSTRDSGN" true true false 80 Text 0 0 ,First,#,Road_Tunnels,PARSTRDSGN,-1,-1;DIRTRAFFIC "DIRTRAFFIC" true true false 80 Text 0 0 ,First,#,Road_Tunnels,DIRTRAFFIC,-1,-1;HWYSYSIRTE "HWYSYSIRTE" true true false 80 Text 0 0 ,First,#,Road_Tunnels,HWYSYSIRTE,-1,-1;DSNNTINTWK "DSNNTINTWK" true true false 80 Text 0 0 ,First,#,Road_Tunnels,DSNNTINTWK,-1,-1;Status "Status" true true false 10 Long 0 10 ,First,#,Road_Tunnels,Status,-1,-1;Comments "Comments" true true false 80 Text 0 0 ,First,#,Road_Tunnels,Comments,-1,-1;Shape__Len "Shape__Len" true true false 24 Double 15 23 ,First,#,Road_Tunnels,Shape__Len,-1,-1', \
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
            print " Adding fields to hifld table..."
            createTable = "ALTER TABLE HIFLD_HIGHWAYTUNNELTEMP \
                            ADD NameTRUNC varchar(40), \
                            HIGHWAYTUNNELID varchar(8), \
                            CENSUSTRACTID varchar(11), \
                            OWNERTRUNC varchar(25), \
                            COSTKUSD numeric(38,8), \
                            CENTERXDD numeric(38,8), \
                            CENTERYDD numeric(38,8), \
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


print "Calculate hifld_HighwayTunnel fields..."
try:
    for state in existingDatabaseList:
        print state
        if state in ["AS", "IN", "OR"]:
           connectString = "Driver={SQL Server};Server="+userDefinedServer+";\
                        Database=_"+state+";UID="+UserName+";PWD="+Password
           hifldtable = "[_"+state+"]..[hifld_HighwayTunnelTEMP]"
        else:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+";\
                        Database="+state+";UID="+UserName+";PWD="+Password
            hifldtable = "["+state+"]..[hifld_HighwayTunnelTEMP]"
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()

        # HighwayTunnelId (State abbreviation plus 6 digits eg WA123456,
        # this must be unique and will persist across four tables.
        # IDSeq should be unique, non null and int)
        try:
            cursor.execute("UPDATE "+hifldtable+" SET HighwayTunnelId = '"+state+\
                           "' + RIGHT('000000'+cast(OBJECTID_1 as varchar(6)),6)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE HighwayTunnelId: {}".format((e))

        # Set yearbuilt 0, N/A to NULL...
        try:
            cursor.execute("UPDATE "+hifldtable+" \
                            SET YEAR_BUILT = (CASE  \
                            WHEN YEAR_BUILT = 'N/A' THEN NULL \
                            WHEN YEAR_BUILT = ' ' THEN NULL \
                            ELSE YEAR_BUILT END)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE year_built 0 to NULL exception: {}".format((e))

        # CENSUS TRACTS ID      
        # Set Geometry field SRID to 4326...
        try:
            cursor.execute("UPDATE "+hifldtable+" SET [SHAPE].STSrid = 4326")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE GEOMETRY SRID: {}".format((e))

##        # Update Length field in meters...
##        try:
##            cursor.execute("UPDATE "+hifldtable+" \
##                                    SET LENGTHMETERS = SHAPE.STLength()")
##            conn.commit()
##        except Exception as e:
##            print " cursor execute UPDATE length exception: {}".format((e))
            
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
                            SET [COSTKUSD] = ((60.9 * [LENGTH_GEO]) * " + RSMeansNonResAvg + ")")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE CostKUSD: {}".format((e))


        # EARTHQUAKE SPECIFIC FIELDS
   
        # CONDITION DATA TO FIT WITHIN MAX LIMITS
        # Calculate the truncated fields
        try:
            cursor.execute("UPDATE "+hifldtable\
                           +" SET NAMETRUNC = \
                            (CASE WHEN LEN(NAME)>40 THEN CONCAT(LEFT(NAME,37),'...') \
                            ELSE NAME END)")
            conn.commit()
            cursor.execute("UPDATE "+hifldtable\
                           +" SET OWNERTRUNC = \
                            (CASE WHEN LEN(OWNER)>25 THEN CONCAT(LEFT(OWNER,22),'...') \
                            ELSE OWNER END)")
            conn.commit()
        except Exception as e:
            print " cursor execute TRUNC Fields to be under limit exception: {}".format((e))

        # Create centerpoints using arcpy...
        # Create data connection...
##        try:
##            if state in ["AS", "IN", "OR"]:
##                stateSDE = "_" + state + ".sde"
##                hzTract = "_" + state + ".dbo.hzTract"
##            else:
##                stateSDE = state + ".sde"
##                hzTract = state + ".dbo.hzTract"
##            checkFeatureClass =  os.path.join(userDefinedSDEConnectionFolder + stateSDE + hzTract)
            # If db connection does not exist, Create db connection...
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
                HighwayTunnelTEMP = "_" + state + ".dbo.hifld_HighwayTunnelTEMP"
                stateSDE = "_" + state + ".sde"
            else:
                HighwayTunnelTEMP = state + ".dbo.hifld_HighwayTunnelTEMP"
                stateSDE = state + ".sde"
            HighwayTunnelMidPointScratch = r"in_memory\hifld_HighwayTunnel"
            inputpath = os.path.join(userDefinedSDEConnectionFolder, stateSDE, HighwayTunnelTEMP)
            outputpathScratch = HighwayTunnelMidPointScratch
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
            arcpy.AddGeometryAttributes_management(HighwayTunnelMidPointScratch, "POINT_X_Y_Z_M")
        except Exception as e:
            print " cursor execute calculate scratch fields: {}".format((e))        
        # Arcpy copy from scratch to sql server...
        try:
            if state in ["AS", "IN", "OR"]:
                HighwayTunnelMidPoint = "[_"+state + "].dbo.hifld_HighwayTunnel"
            else:
                HighwayTunnelMidPoint = "["+state + "].dbo.hifld_HighwayTunnel"
            outputpath = os.path.join(userDefinedSDEConnectionFolder, stateSDE, HighwayTunnelMidPoint)
            arcpy.CopyFeatures_management(HighwayTunnelMidPointScratch, outputpath)
        except Exception as e:
            print " cursor execute copy from scratch to db: {}".format((e))
        
        # Assign censustractid to centerpoint and set centerpoint x,y ...
        try:
            if state in ["AS", "IN", "OR"]:
                cursor.execute("UPDATE a \
                            SET a.CENSUSTRACTID = b.tract \
                            FROM [_"+state+"]..[hzTract] b \
                            INNER JOIN "+HighwayTunnelMidPoint+" a \
                            ON b.shape.STIntersects(a.shape) = 1")
            else:
                cursor.execute("UPDATE a \
                            SET a.CENSUSTRACTID = b.tract \
                            FROM ["+state+"]..[hzTract] b \
                            INNER JOIN "+HighwayTunnelMidPoint+" a \
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
                            FROM "+HighwayTunnelMidPoint+" b \
                            INNER JOIN "+hifldtable+" a \
                            ON b.HIGHWAYTUNNELID = a.HIGHWAYTUNNELID")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE HIFLD CensusTractID exception: {}".format((e))
            
except:
    print " exception Calculate hifld_HighwayTunnel Fields"
print "Done"
print



print "Move data from the HIFLD staging table to the HAZUS tables."
try:
    for state in existingDatabaseList:
        print state
        if state in ["AS", "IN", "OR"]:
            hifldTable = "[_"+state+"]..[hifld_HighwayTunnel]"
            hzTable = "[_"+state+"]..[hzHighwayTunnel]"
            eqTable = "[_"+state+"]..[eqHighwayTunnel]"
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                            ";Database=_"+state+";UID="+UserName+";PWD="+Password
        else:
             hifldTable = "["+state+"]..[hifld_HighwayTunnel]"
             hzTable = "["+state+"]..[hzHighwayTunnel]"
             eqTable = "["+state+"]..[eqHighwayTunnel]"
             connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                            ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()

        # Remove TEMP table...
        print " Remove hifld temp table..."
        try:
            if cursor.tables(table="hifld_HighwayTunnelTEMP", tableType="TABLE").fetchone():
                print " dropping hifld temp table..."
                cursor.execute("DROP TABLE hifld_HighwayTunnelTEMP")
                conn.commit()
                print " done"
        except Exception as e:
            print " cursor drop hifld temp exception: {}".format((e))
        
        # Remove HAZUS rows
        print " Remove HAZUS rows from hz..."
        try:
             cursor.execute("TRUNCATE TABLE "+hzTable)
             conn.commit()
        except Exception as e:
            print " cursor execute Delete HAZUS from hzRailwayBridge exception: {}".format((e))
        print " done"
        
        print " Remove hazus rows from eqRailwayBridge..."
        try:
            cursor.execute("TRUNCATE TABLE "+eqTable)
            conn.commit()
        except Exception as e:
            print " cursor execute Delete HAZUS from eq exception: {}".format((e))
        print " done"

        # Copy Rows from HIFLD to HAZUS hazard
        print " Copy rows from hifld to hz..."
        try:
            cursor.execute("INSERT INTO "+hzTable+" \
                            (Shape, \
                            HighwayTunnelId, \
                            TunnelClass, \
                            Tract, \
                            Name, \
                            Owner, \
                            Width, \
                            Length, \
                            YearBuilt, \
                            Traffic, \
                            Cost, \
                            Latitude, \
                            Longitude, \
                            Comment) \
                            \
                            SELECT \
                            Shape, \
                            HIGHWAYTUNNELID, \
                            'HDFLT', \
                            [CENSUSTRACTID], \
                            NAMETRUNC, \
                            ' ', \
                            0, \
                            LENGTH_GEO, \
                            YEAR_BUILT, \
                            0, \
                            COSTKUSD, \
                            POINT_Y, \
                            POINT_X, \
                            ' ' \
                            \
                            FROM "+hifldTable+ "\
                            WHERE HighwayTunnelId IS NOT NULL \
                            AND CensusTractId IS NOT NULL \
                            ORDER BY HighwayTunnelId ASC")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into hz exception: {}".format((e))
        print " done"
        
        
        # Copy Rows from HIFLD to HAZUS earthquake
        print " Copy rows from hifld to eq..."
        try:
            cursor.execute("INSERT INTO "+eqTable+" \
                            (HighwayTunnelId, \
                            SoilType, \
                            LqfSusCat, \
                            LndSusCat, \
                            WaterDepth) \
                            \
                            SELECT \
                            HighwayTunnelId, \
                            'D', \
                            0, \
                            0, \
                            5 \
                            FROM "+hifldTable+\
                            " WHERE HighwayTunnelId IS NOT NULL \
                            AND CensusTractId IS NOT NULL \
                            ORDER BY HighwayTunnelId ASC")
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


