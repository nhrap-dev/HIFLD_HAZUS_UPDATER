# NiyamIT
# COLIN LINDEMAN, GIS Developer
# Proof of Concept - HIFLD NaturalGasPipelines into HAZUS NaturalGasPipelines.
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
url = cfgParser.get("HIFLD OPEN DATA URLS", "NaturalGasPl_Shapefile_URL")
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
tempZipFilePath = os.path.join(tempDir, "NaturalGasPipelines.zip")
tempShapefileFolder = os.path.join(tempDir, "Natural_Gas_Pipelines")
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
tempShapefilePath = os.path.join(tempShapefileFolder, "Natural_Gas_Pipelines.shp")
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
       

# Downloaded shapefile intersect with db hzcounty then copy to sql server...
for state in existingDatabaseList:
    print state
    if state in ["AS", "IN", "OR"]:
        stateSDE = "_" + state + ".sde"
        hzCountytable = "_" + state + ".dbo.hzCounty"
    else:
        stateSDE = state + ".sde"
        hzCountytable = state + ".dbo.hzCounty"
    hzCountyPath =  os.path.join(userDefinedSDEConnectionFolder + os.sep + stateSDE + os.sep + hzCountytable)
##    # Create data connection...
##    print "checking arcpy connections to database..."
##    try:
##        # If db connection does not exist, Create db connection...
##        if arcpy.Exists(hzCountyPath) == False:
##            print " connection does not exist, creating..."
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
##                print " cursor Arcpy create db connection: {}".format((e))
##            print " done"
##    except Exception as e:
##        print " cursor execute create db connection: {}".format((e))
    # Drop hifld table if it exists (may need/want to switch to using arcpy to delete, if it can)...
    print "Drop hifld table if it exists..."
    try:
            if cursor.tables(table="hifld_NaturalGasPl", tableType="TABLE").fetchone():
                print " hifld table exists, dropping table..."
                cursor.execute("DROP TABLE hifld_NaturalGasPl")
                conn.commit()
                print " done"
    except Exception as e:
            print " cursor drop hifld table exception: {}".format((e))
    # Arcpy Intersect...
    print "Arcpy Intersect..."
    inFeatures = tempShapefilePath + " #;" + hzCountyPath + " #"
    if state in ["AS", "IN", "OR"]:
        outFeatureTable = "_" + state+".dbo.hifld_NaturalGasPl"
    else:
        outFeatureTable = state+".dbo.hifld_NaturalGasPl"
    outFeaturePath = os.path.join(userDefinedSDEConnectionFolder + os.sep + stateSDE + os.sep + outFeatureTable)
    outFeatureInMemory = "in_memory\hifld_NaturalGasPl"
    outPath = os.path.join(userDefinedSDEConnectionFolder + os.sep + stateSDE)
    try:
        arcpy.Intersect_analysis(in_features = inFeatures, \
                             out_feature_class = outFeatureInMemory, \
                             join_attributes="ALL", \
                             cluster_tolerance="-1 Unknown", \
                             output_type="LINE")
    except Exception as e:
        print " exception Arcpy Intersect: {}".format((e))

    # Length in kilometers (hifld data only exists in AK and contiguous states)
##    projectionAK = ""
##    projectionAS = ""
##    projectionHI = ""
##    projectionGU = ""
##    projectionMP = ""
##    projectionPR = ""
##    projectionVI = ""
##    projectionContigous = ""
    # Calculate the length in kilometers
    print "Arcpy AddGeometryAttributes Length_GeoDesic in Kilometers..."
    try:
        arcpy.AddGeometryAttributes_management(Input_Features=outFeatureInMemory, \
                                       Geometry_Properties="LENGTH_GEODESIC", \
                                       Length_Unit="KILOMETERS", \
                                       Area_Unit="", \
                                       Coordinate_System="PROJCS['USA_Contiguous_Equidistant_Conic',\
                                                                            GEOGCS['GCS_North_American_1983',\
                                                                            DATUM['D_North_American_1983',\
                                                                            SPHEROID['GRS_1980',6378137.0,298.257222101]],\
                                                                            PRIMEM['Greenwich',0.0],\
                                                                            UNIT['Degree',0.0174532925199433]],\
                                                                            PROJECTION['Equidistant_Conic'],\
                                                                            PARAMETER['False_Easting',0.0],\
                                                                            PARAMETER['False_Northing',0.0],\
                                                                            PARAMETER['Central_Meridian',-96.0],\
                                                                            PARAMETER['Standard_Parallel_1',33.0],\
                                                                            PARAMETER['Standard_Parallel_2',45.0],\
                                                                            PARAMETER['Latitude_Of_Origin',39.0],\
                                                                            UNIT['Meter',1.0]]")
    except Exception as e:
        print " exception Arcpy AddGeometryAttributes: {}".format((e))
    #Copy in_memory to sql
    print "Copy from in_memory to sql..."
    try:
        arcpy.FeatureClassToFeatureClass_conversion(in_features=outFeatureInMemory, \
                                                    out_path=outPath, \
                                                    out_name="hifld_NaturalGasPl", \
                                                    where_clause="", \
                                                    field_mapping='FID_Natura "FID_Natura" true true false 10 Long 0 10 ,First,#,intersect_county,FID_Natura,-1,-1;\
                                                                                FID_1 "FID_1" true true false 10 Long 0 10 ,First,#,intersect_county,FID_1,-1,-1;\
                                                                                TYPEPIPE "TYPEPIPE" true true false 80 Text 0 0 ,First,#,intersect_county,TYPEPIPE,-1,-1;\
                                                                                Operator "Operator" true true false 80 Text 0 0 ,First,#,intersect_county,Operator,-1,-1;\
                                                                                Shape_Leng "Shape_Leng" true true false 19 Double 0 0 ,First,#,intersect_county,Shape_Leng,-1,-1;\
                                                                                Shape__Len "Shape__Len" true true false 19 Double 0 0 ,First,#,intersect_county,Shape__Len,-1,-1;\
                                                                                test "test" true true false 13 Float 0 0 ,First,#,intersect_county,test,-1,-1;\
                                                                                FID__hzCou "FID__hzCou" true true false 10 Long 0 10 ,First,#,intersect_county,FID__hzCou,-1,-1;\
                                                                                CountyFips "CountyFips" true true false 5 Text 0 0 ,First,#,intersect_county,CountyFips,-1,-1;\
                                                                                CountyFi_1 "CountyFi_1" true true false 3 Text 0 0 ,First,#,intersect_county,CountyFi_1,-1,-1;\
                                                                                CountyName "CountyName" true true false 40 Text 0 0 ,First,#,intersect_county,CountyName,-1,-1;\
                                                                                State "State" true true false 2 Text 0 0 ,First,#,intersect_county,State,-1,-1;\
                                                                                StateFips "StateFips" true true false 2 Text 0 0 ,First,#,intersect_county,StateFips,-1,-1;\
                                                                                NumAggrTra "NumAggrTra" true true false 10 Long 0 10 ,First,#,intersect_county,NumAggrTra,-1,-1;\
                                                                                CountyLeng "CountyLeng" true true false 13 Float 0 0 ,First,#,intersect_county,CountyLeng,-1,-1;\
                                                                                LENGTH_GEO "LENGTH_GEO" true true false 19 Double 0 0 ,First,#,intersect_county,LENGTH_GEO,-1,-1', \
                                            config_keyword="")
    except Exception as e:
        print " exception Arcpy featureclass to featureclass: {}".format((e))
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
            createTable = "ALTER TABLE hifld_NaturalGasPl \
                            ADD NameTRUNC varchar(40), \
                            NaturalGasPlID varchar(8), \
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
    

print "Calculate hifld fields..."
try:
    for state in existingDatabaseList:
        print state
        if state in ["AS", "IN", "OR"]:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+";\
                        Database=_"+state+";UID="+UserName+";PWD="+Password
            hifldtable = "[_"+state+"]..[hifld_NaturalGasPl]"
        else:
            connectString = "Driver={SQL Server};Server="+userDefinedServer+";\
                        Database="+state+";UID="+UserName+";PWD="+Password
            hifldtable = "["+state+"]..[hifld_NaturalGasPl]"
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        
            
        # RailwaySegmentlId (State abbreviation plus 6 digits eg WA123456,
        # this must be unique and will persist across four tables.
        # IDSeq should be unique, non null and int)
        try:
            cursor.execute("UPDATE "+hifldtable+" SET NaturalGasPlID = \
            [state] + RIGHT('000000'+cast([OBJECTID] as varchar(6)),6)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE Id: {}".format((e))

            
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
                            SET [COSTKUSD] = ((683 * [LENGTH_GEO]) * " + RSMeansNonResAvg + ")")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE CostKUSD: {}".format((e))


        # EARTHQUAKE SPECIFIC FIELDS
   
        # CONDITION DATA TO FIT WITHIN MAX LIMITS
        # Calculate the truncated fields
        try:
            cursor.execute("UPDATE "+hifldtable\
                           +" SET NameTRUNC = \
                            (CASE WHEN LEN([Operator])>40 THEN CONCAT(LEFT([Operator],37),'...') \
                            ELSE [Operator] END)")
            cursor.execute("UPDATE "+hifldtable\
                           +" SET Comment = \
                            (CASE WHEN LEN([TYPEPIPE])>40 THEN CONCAT(LEFT([TYPEPIPE],37),'...') \
                            ELSE [TYPEPIPE] END)")
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
        if state in ["AS", "IN", "OR"]:
            hifldTable = "[_"+state+"]..[hifld_NaturalGasPl]"
            hzTable = "[_"+state+"]..[hzNaturalGasPl]"
            eqTable = "[_"+state+"]..[eqNaturalGasPl]"
            flTable = "[_"+state+"]..[flNaturalGasPl]"
            connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                                ";Database=_"+state+";UID="+UserName+";PWD="+Password
        else:
            hifldTable = "["+state+"]..[hifld_NaturalGasPl]"
            hzTable = "["+state+"]..[hzNaturalGasPl]"
            eqTable = "["+state+"]..[eqNaturalGasPl]"
            flTable = "["+state+"]..[flNaturalGasPl]"
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
                            NaturalGasPlId, \
                            PipelinesClass, \
                            CountyFips, \
                            Name, \
                            Diameter, \
                            PipeLength, \
                            Cost, \
                            Comment)\
                            \
                            SELECT \
                            Shape, \
                            NaturalGasPlId, \
                            'NGP2', \
                            CountyFips, \
                            NameTRUNC, \
                            0, \
                            LENGTH_GEO, \
                            COSTKUSD, \
                            COMMENT \
                            \
                            FROM "+hifldTable+\
                            " WHERE NaturalGasPlId IS NOT NULL \
                            AND CountyFips IS NOT NULL \
                            ORDER BY NaturalGasPlId ASC")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into hz exception: {}".format((e))
        print " done"
        
        
        # Copy Rows from HIFLD to HAZUS earthquake
        print " Copy rows from hifld to eq..."
        try:
            cursor.execute("INSERT INTO "+eqTable+" \
                            (NaturalGasPlId) \
                            \
                            SELECT \
                            NaturalGasPlId \
                            \
                            FROM "+hifldTable+\
                            " WHERE NaturalGasPlId IS NOT NULL \
                            AND CountyFips IS NOT NULL \
                            ORDER BY NaturalGasPlId ASC")
            conn.commit()
        except Exception as e:
            print " cursor execute Insert Into eq exception: {}".format((e))
        print " done"

        # Copy Rows from HIFLD to HAZUS flood
        print " Copy rows from hifld to fl..."
        try:
            cursor.execute("INSERT INTO "+flTable+" \
                            (NaturalGasPlId, \
                            IDUpperJunction, \
                            IDLowerJunction) \
                            \
                            SELECT \
                            NaturalGasPlId, \
                            0, \
                            0 \
                            \
                            FROM "+hifldTable+\
                            " WHERE NaturalGasPlId IS NOT NULL \
                            AND CountyFips IS NOT NULL \
                            ORDER BY NaturalGasPlId ASC")
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


