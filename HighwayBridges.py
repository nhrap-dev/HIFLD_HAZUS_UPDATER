# NiyamIT
# COLIN LINDEMAN, GIS Developer
# Proof of Concept - NBI Highway Bridge into HAZUS HighwayBridge.
# Last Update: 2019-08-01
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
url = cfgParser.get("HIFLD OPEN DATA URLS", "NullURL")
userDefinedServer = cfgParser.get("SQL SERVER", "ServerName")
UserName = cfgParser.get("SQL SERVER", "UserName")
Password = cfgParser.get("SQL SERVER", "Password")
possibleDatabaseListRaw = cfgParser.get("DATABASE LIST", "possibleDatabaseList")
possibleDatabaseList = []
for database in possibleDatabaseListRaw.split(","):
    possibleDatabaseList.append(database)
print "Possible Databases" + str(possibleDatabaseList)
print "Done"
print


##print "Download CSV's..."
##tempDir = tempfile.gettempdir()
###  for example: r'C:\Users\User1\AppData\Local\Temp'
### Download CSV
##try:
##    tempCSVPath = os.path.join(tempDir, "2018HwyBridgesDelimitedAllStates.txt")
##    csvFile = urllib.urlopen(url).read()
##    with open(tempCSVPath, "w") as fx:
##        fx.write(csvFile)
##    fx.close()
##except:
##    print " exception downloading csv"
##print "Done"
##print

##print "Download JSON..."
##tempDir = tempfile.gettempdir()
###  for example: r'C:\Users\User1\AppData\Local\Temp'
### Download json
##try:
##    tempJSONPath = os.path.join(tempDir, "2018HwyBridgesDelimitedAllStates.json")
##    jsonFile = urllib.urlopen(url).read()
##    with open(tempJSONPath, "w") as fx:
##        fx.write(jsonFile)
##    fx.close()
##except:
##    print " exception downloading json"
##print "Done"
##print

#TEMP until able to download data...
##tempCSVPath = r".\InputData\2018hwybronefiledel\2018HwyBridgesDelimitedAllStates.csv"
##tempCSVPath = r".\InputData\National_Bridge_Inventory_NBI_Bridges.csv"
tempCSVPath = r"E:\HazusData\SourceDownloads20191100\National_Bridge_Inventory_NBI_Bridges.csv"
print tempCSVPath
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
    except Exception as e:
        print " exception checking existing database: {}".format((e))
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
print FIPSDatabaseList
print "Done"
print


print "Create 'Hifld_HighwayBridge' Staging Table (hzHAZUS fields)..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        try:
            if cursor.tables(table="hifld_highwaybridgetemp", tableType="TABLE").fetchone():
                print " hifld_highwaybridgetemp exists, dropping table..."
                cursor.execute("DROP TABLE hifld_highwaybridgetemp")
                conn.commit()
                print " done"
        except Exception as e:
            print " cursor  drop hifld_highwaybridgetemp exception: {}".format((e))
        try:
            if cursor.tables(table="hifld_HighwayBridge", tableType="TABLE").fetchone():
                print " hifld_HighwayBridge already exists, dropping table..."
                cursor.execute("DROP TABLE hifld_HighwayBridge")
                conn.commit()
                print " done"
            else:
                print " hifld_HighwayBridge doesn't exist"
            print " Creating hifld_HighwayBridge table..."
            createTable = "CREATE TABLE hifld_HighwayBridge \
                            (HighwayBridgeID varchar(8), \
                            BridgeClass varchar(5), \
                            CensusTractID varchar(11), \
                            Name varchar(40), \
                            Owner varchar(25), \
                            BridgeType varchar(8), \
                            Width numeric(38,8), \
                            NumSpans smallint, \
                            NumSpansTrunc tinyint, \
                            Length int, \
                            MaxSpanLength numeric(38,8), \
                            SkewAngle numeric(38,8), \
                            SingleColumn int, \
                            SeatLength numeric(38,8), \
                            SeatWidth numeric(38,8), \
                            YearBuilt smallint, \
                            YearRemodeled smallint, \
                            PierType varchar(10), \
                            FoundationType varchar(1), \
                            ScourIndex varchar(1), \
                            Traffic int, \
                            TrafficIndex varchar(2), \
                            Condition varchar(3), \
                            CostKUSD numeric(38,8), \
                            Latitude numeric(38,8), \
                            Longitude numeric(38,8), \
                            Comment varchar(40))"
            cursor.execute(createTable)
            conn.commit()
            print " done"
        except:
            print " cursor execute createTable exception"
except:
    print " exception Staging Table"
print "Done"
print


print "ADD hifld_HighwayBridge fields..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        hifldtable = "["+state+"]..[hifld_HighwayBridge]"
        try:
            cursor.execute("ALTER TABLE "+hifldtable+" ADD \
                            OWNERCODE int, \
                            MAX_SPAN_LEN_MT_048 float, \
                            STRUCTURE_LEN_MT_049 float, \
                            MAIN_UNIT_SPANS_045 int, \
                            APPR_SPANS_046 int, \
                            STRUCTURE_KIND_043A int, \
                            STRUCTURE_TYPE_043B int, \
                            DECK_COND_058 varchar(1), \
                            SUPERSTRUCTURE_COND_059 varchar(1), \
                            SUBSTRUCTURE_COND_060 varchar(1),\
                            IDseq int IDENTITY(1,1), \
                            STATUS_NO_10YR_RULE int, \
                            BridgeAreaMeters float, \
                            YEAR_BUILT_027 smallint, \
                            Shape geometry, \
                            service_on_042a varchar(1), \
                            culvert_cond_062 varchar(1), \
                            structural_eval_067 varchar(1), \
                            deck_geometry_eval_068 varchar(1), \
                            undclrence_eval_069 varchar(1), \
                            waterway_eval_071 varchar(1), \
                            appr_road_eval_072 varchar(1),\
                            structure_number_008 varchar(100)")
            conn.commit()
        except Exception as e:
            print " cursor ALTER TABLE exception: {}".format((e))
except:
    print " exception ADD hifld_HighwayBridge fields"
print "Done"
print


print "Copy Downloaded HIFLD CSV to SQL Staging Table..."
try:
    # Define the (28) columns that data will be inserted into
    hifld_HighwayBridge_Columns = "HighwayBridgeID\
                                    ,Name\
                                    ,OWNERCODE\
                                    ,Width\
                                    ,NumSpans\
                                    ,MaxSpanLength\
                                    ,SkewAngle\
                                    ,YEAR_BUILT_027\
                                    ,YearRemodeled\
                                    ,ScourIndex\
                                    ,Traffic\
                                    ,Latitude\
                                    ,Longitude\
                                    ,MAX_SPAN_LEN_MT_048\
                                    ,STRUCTURE_LEN_MT_049\
                                    ,MAIN_UNIT_SPANS_045\
                                    ,APPR_SPANS_046\
                                    ,STRUCTURE_KIND_043A\
                                    ,STRUCTURE_TYPE_043B\
                                    ,DECK_COND_058\
                                    ,SUPERSTRUCTURE_COND_059\
                                    ,SUBSTRUCTURE_COND_060\
                                    ,STATUS_NO_10YR_RULE\
                                    ,SingleColumn\
                                    ,service_on_042a\
                                    ,culvert_cond_062\
                                    ,structural_eval_067\
                                    ,deck_geometry_eval_068\
                                    ,undclrence_eval_069\
                                    ,waterway_eval_071\
                                    ,appr_road_eval_072\
                                    ,structure_number_008"
    for item in FIPSDatabaseList:
        state = item[0]
        statefips = int(item[1])
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
                if int(row["state_code_001"]) == statefips:
                    
                    BRKEY = row["structure_number_008"]
                    structure_number_008_stripped = BRKEY.strip()
                    # if there are several records with funky ANSI, then lookup how it was addressed in essential facilities...

                    # This list order must match the order of the created table that it's being inserted into                 
                    sqlInsertData = "INSERT INTO ["+state+"]..[hifld_HighwayBridge] ("\
                                    +hifld_HighwayBridge_Columns+") VALUES \
                                    (?, ?, ?, ?, ?, \
                                    ?, ?, ?, ?, ?, \
                                    ?, ?, ?, ?, ?, \
                                    ?, ?, ?, ?, ?, \
                                    ?, ?, ?, ?, ?, \
                                    ?, ?, ?, ?, ?,\
                                    ?, ?)"

                    #NBI csv:
##                    try:
##                        cursor.execute(sqlInsertData, \
##                                       [row["STATE_CODE_001"], \
##                                        row["FEATURES_DESC_006A"], \
##                                        row["OWNER_022"], \
##                                        row["DECK_WIDTH_MT_052"], \
##                                        row["MAIN_UNIT_SPANS_045"], \
##                                        row["MAX_SPAN_LEN_MT_048"], \
##                                        row["DEGREES_SKEW_034"], \
##                                        row["YEAR_BUILT_027"], \
##                                        row["YEAR_RECONSTRUCTED_106"], \
##                                        row["SCOUR_CRITICAL_113"], \
##                                        row["ADT_029"], \
##                                        row["LAT_016"], \
##                                        row["LONG_017"], \
##                                        row["MAX_SPAN_LEN_MT_048"], \
##                                        row["STRUCTURE_LEN_MT_049"], \
##                                        row["MAIN_UNIT_SPANS_045"], \
##                                        row["STRUCTURE_KIND_043A"], \
##                                        row["STRUCTURE_TYPE_043B"], \
##                                        row["DECK_COND_058"], \
##                                        row["SUPERSTRUCTURE_COND_059"], \
##                                        row["SUBSTRUCTURE_COND_060"], \
##                                        row["STATUS_NO_10YR_RULE"]])
                    #HIFLD CSV
                    try:
                        try:
                            apprspans046 = int(row["appr_spans_046"])
                        except:
                            apprspans046 = 0
                        try:
                            mainunitspans045 = int(row["main_unit_spans_045"])
                        except:
                            mainunitspans045 = 0
                        featuresdesc006a = str(row["features_desc_006a"].decode("utf-8").encode("ascii", "ignore"))[1:-1]
                        cursor.execute(sqlInsertData, \
                                       [row["state_code_001"], \
                                        featuresdesc006a, \
                                        row["owner_022"], \
                                        row["deck_width_mt_052"], \
                                        mainunitspans045 + apprspans046, \
                                        row["max_span_len_mt_048"], \
                                        row["degrees_skew_034"], \
                                        row["year_built_027"], \
                                        row["year_reconstructed_106"], \
                                        row["scour_critical_113"], \
                                        row["adt_029"], \
                                        row["Y"], \
                                        row["﻿X"], \
                                        row["max_span_len_mt_048"], \
                                        row["structure_len_mt_049"], \
                                        row["main_unit_spans_045"], \
                                        row["appr_spans_046"],\
                                        row["structure_kind_043a"], \
                                        row["structure_type_043b"], \
                                        row["deck_cond_058"], \
                                        row["superstructure_cond_059"], \
                                        row["substructure_cond_060"], \
                                        '',\
                                        0,\
                                        row["service_on_042a"],\
                                        row["culvert_cond_062"],\
                                        row["structural_eval_067"],\
                                        row["deck_geometry_eval_068"],\
                                        row["undclrence_eval_069"],\
                                        row["waterway_eval_071"],\
                                        row["appr_road_eval_072"],\
                                        structure_number_008_stripped])
                    except Exception as e:
##                        print row["STRUCTURE_NUMBER_008"]
                        print " cursor execute insertData CSV exception: {}".format((e))
                        print row["objectid"]
                    conn.commit()
        except Exception as e:
            print " exception csv dict: {}".format((e))
except Exception as e:
    print " exception Copy Downloaded HIFLD CSV to Staging Table: {}".format((e))
print "Done"
print
        

print "Calculate HIFLD_HighwayBridge fields..."
try:
    for state in existingDatabaseList:
        print state
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()
        hifldtable = "["+state+"]..[hifld_HighwayBridge]"

        # HighwayBridgeId 
        try:
            cursor.execute("UPDATE "+hifldtable+" SET HighwayBridgeID = '"+state+\
                           "' + RIGHT('000000'+cast(IDseq as varchar(6)),6)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE HighwayBridgeID: {}".format((e))

        # BridgeType
        try:
            cursor.execute("UPDATE "+hifldtable+" SET BridgeType = CASE \
            WHEN STRUCTURE_TYPE_043B < 10 THEN CONCAT(STRUCTURE_KIND_043A, CONCAT('0', STRUCTURE_TYPE_043B)) \
            ELSE CONCAT(STRUCTURE_KIND_043A, STRUCTURE_TYPE_043B) END")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE BridgeType: {}".format((e))

        # Width 
        try:
            cursor.execute("UPDATE "+hifldtable+" SET Width = 9.1 WHERE Width = 0")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE Width: {}".format((e))

        # YearBuilt
        try:
            cursor.execute("UPDATE "+hifldtable+" SET [YearBuilt] = CASE \
                           WHEN [YearRemodeled] > 1900 AND [YearRemodeled] < 2019 THEN [YearRemodeled] \
                           ELSE [YEAR_BUILT_027] END")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE YearBuilt: {}".format((e))

        # YearRemodeled
        try:
            cursor.execute("UPDATE "+hifldtable+" SET [YearRemodeled] = CASE \
                           WHEN [YearRemodeled] < 1700 THEN 1900 \
                           ELSE [YearRemodeled] END")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE YearBuilt: {}".format((e))

        # BridgeClass
        if state == 'CA':
            try:
                cursor.execute("UPDATE "+hifldtable+" SET [BridgeClass] = CASE \
                            WHEN [YearBuilt] < 1975 AND [MaxSpanLength] > 150 THEN 'HWB1' \
                            WHEN [YearBuilt] >= 1975 AND [MaxSpanLength] > 150 THEN 'HWB2' \
                            WHEN [YearBuilt] < 1975 AND [NumSpans] = 1 THEN 'HWB3' \
                            WHEN [YearBuilt] >= 1975 AND [NumSpans] = 1 THEN 'HWB4' \
                            WHEN [BridgeType] >= 301 AND [BridgeType] <= 306 AND [YearBuilt] < 1975 AND [STRUCTURE_LEN_MT_049] < 20 THEN 'HWB25' \
                            WHEN [BridgeType] >= 402 AND [BridgeType] <= 410 AND [YearBuilt] < 1975 AND [STRUCTURE_LEN_MT_049] < 20 THEN 'HWB27' \
                            WHEN [BridgeType] >= 101 AND [BridgeType] <= 106 AND [YearBuilt] < 1975 THEN 'HWB6' \
                            WHEN [BridgeType] >= 101 AND [BridgeType] <= 106 AND [YearBuilt] >= 1975 THEN 'HWB7' \
                            WHEN [BridgeType] >= 205 AND [BridgeType] <= 206 AND [YearBuilt] < 1975 AND [SingleColumn] = 1 THEN 'HWB8'\
                            WHEN [BridgeType] >= 205 AND [BridgeType] <= 206 AND [YearBuilt] >= 1975 AND [SingleColumn] = 1 THEN 'HWB9'\
                            WHEN [BridgeType] >= 201 AND [BridgeType] <= 206 AND [YearBuilt] < 1975 THEN 'HWB10' \
                            WHEN [BridgeType] >= 201 AND [BridgeType] <= 206 AND [YearBuilt] >= 1975 THEN 'HWB11' \
                            WHEN [BridgeType] >= 301 AND [BridgeType] <= 306 AND [YearBuilt] < 1975 AND [STRUCTURE_LEN_MT_049] >= 20 THEN 'HWB13' \
                            WHEN [BridgeType] >= 301 AND [BridgeType] <= 306 AND [YearBuilt] >= 1975 THEN 'HWB14' \
                            WHEN [BridgeType] >= 402 AND [BridgeType] <= 410 AND [YearBuilt] < 1975 AND [STRUCTURE_LEN_MT_049] >= 20 THEN 'HWB15' \
                            WHEN [BridgeType] >= 402 AND [BridgeType] <= 410 AND [YearBuilt] >= 1975 THEN 'HWB16' \
                            WHEN [BridgeType] >= 501 AND [BridgeType] <= 506 AND [YearBuilt] < 1975 THEN 'HWB18' \
                            WHEN [BridgeType] >= 501 AND [BridgeType] <= 506 AND [YearBuilt] >= 1975 THEN 'HWB19' \
                            WHEN [BridgeType] >= 605 AND [BridgeType] <= 606 AND [YearBuilt] < 1975 AND [SingleColumn] = 1 THEN 'HWB20' \
                            WHEN [BridgeType] >= 605 AND [BridgeType] <= 606 AND [YearBuilt] >= 1975 AND [SingleColumn] = 1 THEN 'HWB21' \
                            WHEN [BridgeType] >= 601 AND [BridgeType] <= 607 AND [YearBuilt] < 1975 THEN 'HWB22' \
                            WHEN [BridgeType] >= 601 AND [BridgeType] <= 607 AND [YearBuilt] >= 1975 THEN 'HWB23' \
                            ELSE 'HWB28' END")
                conn.commit()
            except Exception as e:
                print " cursor execute UPDATE Bridgeclass: {}".format((e))
        else:
            try:
                cursor.execute("UPDATE "+hifldtable+" SET [BridgeClass] = CASE \
                            WHEN [YearBuilt] < 1990 AND [MaxSpanLength] > 150 THEN 'HWB1' \
                            WHEN [YearBuilt] >= 1990 AND [MaxSpanLength] > 150 THEN 'HWB2' \
                            WHEN [YearBuilt] < 1990 AND [NumSpans] = 1 THEN 'HWB3' \
                            WHEN [YearBuilt] >= 1990 AND [NumSpans] = 1 THEN 'HWB4' \
                            WHEN [BridgeType] >= 301 AND [BridgeType] <= 306 AND [YearBuilt] < 1990 AND [STRUCTURE_LEN_MT_049] < 20 THEN 'HWB24' \
                            WHEN [BridgeType] >= 402 AND [BridgeType] <= 410 AND [YearBuilt] >= 1990 AND [STRUCTURE_LEN_MT_049] < 20 THEN 'HWB26' \
                            WHEN [BridgeType] >= 101 AND [BridgeType] <= 106 AND [YearBuilt] < 1990 THEN 'HWB5' \
                            WHEN [BridgeType] >= 101 AND [BridgeType] <= 106 AND [YearBuilt] >= 1990 THEN 'HWB7' \
                            WHEN [BridgeType] >= 201 AND [BridgeType] <= 206 AND [YearBuilt] < 1990 THEN 'HWB10' \
                            WHEN [BridgeType] >= 201 AND [BridgeType] <= 206 AND [YearBuilt] >= 1990 THEN 'HWB11' \
                            WHEN [BridgeType] >= 301 AND [BridgeType] <= 306 AND [YearBuilt] < 1990 AND [STRUCTURE_LEN_MT_049] >= 20 THEN 'HWB12' \
                            WHEN [BridgeType] >= 301 AND [BridgeType] <= 306 AND [YearBuilt] >= 1990 THEN 'HWB14' \
                            WHEN [BridgeType] >= 402 AND [BridgeType] <= 410 AND [YearBuilt] < 1990 AND [STRUCTURE_LEN_MT_049] >= 20 THEN 'HWB15' \
                            WHEN [BridgeType] >= 402 AND [BridgeType] <= 410 AND [YearBuilt] >= 1990 THEN 'HWB16' \
                            WHEN [BridgeType] >= 501 AND [BridgeType] <= 506 AND [YearBuilt] < 1990 THEN 'HWB17' \
                            WHEN [BridgeType] >= 501 AND [BridgeType] <= 506 AND [YearBuilt] >= 1990 THEN 'HWB19' \
                            WHEN [BridgeType] >= 601 AND [BridgeType] <= 607 AND [YearBuilt] < 1990 THEN 'HWB22' \
                            WHEN [BridgeType] >= 601 AND [BridgeType] <= 607 AND [YearBuilt] >= 1990 THEN 'HWB23' \
                            ELSE 'HWB28' END")
                conn.commit()
            except Exception as e:
                print " cursor execute UPDATE Bridgeclass: {}".format((e))

        # Bridgeclass CA part II...
        if state == 'CA':
            try:
                cursor.execute("UPDATE table1 \
                                        SET table1.[BridgeClass] = LEFT(table2.[HAZUS_class_final], 5) \
                                        FROM "+hifldtable+" AS table1 \
                                        INNER JOIN [CDMS]..[CaltransMasterList_2019] AS table2 \
                                        ON table1.[structure_number_008] = table2.[BRKEY]")
                conn.commit()
            except Exception as e:
                print " cursor execute UPDATE CA Bridgeclass Part ii : {}".format((e))

        # CensusTractID
        try:
            cursor.execute("UPDATE "+hifldtable+\
                           " SET Shape = geometry::Point([LONGITUDE], [LATITUDE], 4326)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE Shape exception: {}".format((e))
        # Calculate Shape
        try:
            cursor.execute("UPDATE "+hifldtable+\
                           " SET Shape = geometry::Point([LONGITUDE], [LATITUDE], 4326)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE Shape exception: {}".format((e))
        # Calculate TractID field by spatial join (get closest)...
        try:
            cursor.execute("UPDATE a SET a.[CensusTractID] = b.[tract] \
                            FROM ["+state+"]..[hzTract] b \
                            INNER JOIN "+hifldtable+" a ON b.[shape].STIntersects(a.[shape]) = 1")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE CensusTractID exception: {}".format((e))

        # Calculate TractID for records that do not fall within an hzTract polygon...
        # Set IDseq to be constraint primary key clustered
        try:
            cursor.execute("ALTER TABLE "+hifldtable+\
                            " ADD CONSTRAINT "+state+"_hifld_highwaybridge_primarykey PRIMARY KEY CLUSTERED ([IDseq])")
            conn.commit()
        except Exception as e:
            print " cursor execute ALTER TABLE ADD CONSTRAINT PRIMARY KEY CLUSTERED exception: {}".format((e))
        # Get Bounding Box of STATE hzTract...
        #
        # Create Spatial Index...
##        spatialindexname = "hifld_HighwayBridge_shapespatialindex"
##        try:
##            cursor.execute("CREATE SPATIAL INDEX "+spatialindexname+"\
##                                    ON "+hifldtable+"([SHAPE]) \
##                                    WITH (BOUNDING_BOX = (xmin=-179, ymin=-89, xmax=179, ymax=89))")
##            conn.commit()
##        except Exception as e:
##            print " cursor execute CREATE SPATIAL INDEX on shape exception: {}".format((e))
        # Create Spatial Index...
##        spatialindexname = "hztract_shapespatialindex"
##        try:
##            cursor.execute("CREATE SPATIAL INDEX "+spatialindexname+"\
##                                    ON "+state+"..hzTract([Shape]) \
##                                    WITH (BOUNDING_BOX = (xmin=-179, ymin=-89, xmax=179, ymax=89))")
##            conn.commit()
##        except Exception as e:
##            print " cursor execute CREATE SPATIAL INDEX on shape exception: {}".format((e))
        # Find Spatial Index Name...
        try:
            connectStringspatialindex = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
            connspatialindex = pyodbc.connect(connectStringspatialindex, autocommit=False)
            cursorspatialindex = connspatialindex.cursor()
            cursorspatialindex.execute("select[name] \
                                    from \
                                    (select * from sys.indexes \
                                    where object_id = (select object_id from sys.objects where [name] = 'hzTract')) a \
                                    where [name] LIKE 'SHAPE%'")
            rowsspatialindex = cursorspatialindex.fetchall()
            for rowspatialindex in rowsspatialindex:
                spatialindexname = rowspatialindex.name
        except Exception as e:
            print " cursor execute Find Spatial Index Name exception: {}".format((e))
        # Create new table of hifld records that did not land in a censustract...
        tempSpatialTable = "["+state+"]..hifld_highwaybridgetemp"
        try:
            cursor.execute("SELECT [HighwayBridgeID], [Latitude], [Longitude], c.[Tract] into "+tempSpatialTable+" \
                                     FROM (SELECT [HighwayBridgeID] \
                                                  ,[CensusTractID] \
                                                  ,[Latitude] \
                                                  ,[Longitude] \
                                                  ,[Shape] \
                                                  FROM ["+state+"]..hifld_HighwayBridge \
                                                  WHERE [CensusTractID] IS NULL) a \
                                      CROSS APPLY ( \
                                            SELECT TOP 1 \
                                                       b.[Tract] \
                                            FROM ["+state+"].dbo.hzTract b WITH (index("+spatialindexname+")) \
                                            WHERE b.[Shape].STDistance(a.[Shape]) IS NOT NULL \
                                            ORDER BY b.[Shape].STDistance(a.[Shape]) ASC \
                                      ) c")
            conn.commit()
        except Exception as e:
            print " cursor execute Nearest Neighbor exception: {}".format((e))
        # Join and set censustractid...
        try:
            cursor.execute("UPDATE table1 \
                                SET table1.[CensusTractID] = table2.[Tract] \
                                FROM "+hifldtable+" AS table1 \
                                LEFT JOIN "+tempSpatialTable+" AS table2 \
                                ON table1.[HighwayBridgeID] = table2.[HighwayBridgeID] \
                                WHERE table1.[CensusTractID]  IS NULL")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE NN CensusTractID: {}".format((e))
        # Remove CensusTractID from records with Lat/Long of 0,0...
        try:
            cursor.execute("UPDATE "+hifldtable+" \
                           SET [CensusTractID] = NULL \
                           WHERE [Latitude] = 0 AND [Longitude] = 0")
            conn.commit()
        except Exception as e:
            print " cursor execute remove censustractid from lat long 0 0 exception: {}".format((e))
        # Delete temporary spatial table...



        # MIGHT NOT BE NEEDED. Name (remove whitespace and single qoutes)
        try:
            cursor.execute("UPDATE "+hifldtable+" SET [Name] = RTRIM(LTRIM([Name]))")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE Name: {}".format((e))

        # Single Column
        try:
            cursor.execute("UPDATE "+hifldtable+" \
                                    SET [SingleColumn] = 1 \
                                    WHERE [MAIN_UNIT_SPANS_045] + [APPR_SPANS_046] <= 1")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE Name: {}".format((e))
            
        # Owner
        try:
            cursor.execute("UPDATE table1 \
                                SET table1.Owner = \
                                (CASE WHEN LEN(table2.[Description]) > 25 THEN CONCAT(LEFT(table2.[Description],22),'...') \
                                ELSE table2.[Description] END) \
                                FROM "+hifldtable+" AS table1 \
                                LEFT JOIN [CDMS].[dbo].[HwyOwnerTable] as table2 \
                                ON [table1].[OWNERCODE] = [table2].[Code]")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE Owner: {}".format((e))

        # BridgeArea 
        try:
            cursor.execute("UPDATE "+hifldtable+" SET BridgeAreaMeters = STRUCTURE_LEN_MT_049 * Width")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE BridgeArea: {}".format((e))

        # Condition 
        try:
            cursor.execute("UPDATE "+hifldtable+" SET Condition = \
                                                CONCAT(DECK_COND_058, \
                                                SUPERSTRUCTURE_COND_059, \
                                                SUBSTRUCTURE_COND_060)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE Condition: {}".format((e))

        # CostKUSD
        if state == 'CA':
            try:
                cursor.execute("UPDATE table1 \
                                SET table1.[CostKUSD] = (table1.[BridgeAreaMeters] * table2.[CA_Cost_DollarSqM]) / 1000 \
                                FROM "+hifldtable+" AS table1 \
                                LEFT JOIN [CDMS].[dbo].[HwyCalTransCost] as table2 \
                                ON [table1].[BridgeClass] = [table2].[BridgeClass]")
                conn.commit()
            except Exception as e:
                print " cursor execute UPDATE CostKUSD CA: {}".format((e))
        else:
            try:
                connectStringCDMS = "Driver={SQL Server};Server="+userDefinedServer+\
                                ";Database=CDMS;UID="+UserName+";PWD="+Password
                connCDMS = pyodbc.connect(connectStringCDMS, autocommit=False)
                cursorCDMS = connCDMS.cursor()
                cursorCDMS.execute("SELECT StateAdjusted_CA \
                                FROM [CDMS]..[StateRSMeans] \
                                WHERE StateID = '" + state + "'")
                rows = cursorCDMS.fetchall()
                for row in rows:
                    StateAdjustedRSMeans = str(row.StateAdjusted_CA)

                cursor.execute("UPDATE table1 \
                                SET table1.[CostKUSD] = (table1.[BridgeAreaMeters] * " + StateAdjustedRSMeans + " * table2.[CA_Cost_DollarSqM]) / 1000 \
                                FROM "+hifldtable+" AS table1 \
                                LEFT JOIN [CDMS].[dbo].[HwyCalTransCost] as table2 \
                                ON [table1].[BridgeClass] = [table2].[BridgeClass]")
                conn.commit()
            except Exception as e:
                print " cursor execute UPDATE CostKUSD non-CA: {}".format((e))
        
        # Comment A
##        try:
##            cursor.execute("UPDATE "+hifldtable+" SET [Comment] = (CASE \
##                        WHEN [STATUS_NO_10YR_RULE] = 1 THEN 'Structurally deficient' \
##                        WHEN [STATUS_NO_10YR_RULE] = 2 THEN 'Functionally obsolete' \
##                        WHEN [STATUS_NO_10YR_RULE] = 0 THEN 'Not deficient' \
##                        END)")
##            conn.commit()
##        except Exception as e:
##            print " cursor execute UPDATE Comment: {}".format((e))

        # Comment B
##        try:
##            cursor.execute("UPDATE "+hifldtable+" SET [Comment] = \
##                           (CASE WHEN [YearRemodeled] <> 0 THEN 'HIFLD, Reconstructed ' + LTRIM(STR(YearRemodeled)) \
##                            ELSE 'HIFLD' END)")
##            conn.commit()
##        except Exception as e:
##            print " cursor execute UPDATE Comment: {}".format((e))

        # Comment C
        try:
            cursor.execute("UPDATE "+hifldtable+" SET [Comment] = (CASE \
                            WHEN [deck_cond_058] IN ('0','1','2','3','4') THEN 'Structurally Deficient' \
                            WHEN [superstructure_cond_059] IN ('0','1','2','3','4') THEN 'Structurally Deficient' \
                            WHEN [substructure_cond_060] IN ('0','1','2','3','4') THEN 'Structurally Deficient' \
                            WHEN [culvert_cond_062] IN ('0','1','2','3','4') AND RIGHT([service_on_042a], 2) = '19' THEN 'Structurally Deficient' \
                            WHEN [structural_eval_067] IN ('0','1','2') THEN 'Structurally Deficient' \
                            WHEN [waterway_eval_071] IN ('0','1','2') AND RIGHT([service_on_042a], 1) in ('0','5','6','7','8','9') THEN 'Structurally Deficient' \
                            \
                            WHEN [deck_geometry_eval_068] IN ('0','1','2','3') THEN 'Functionally Obsolete' \
                            WHEN [undclrence_eval_069] IN ('0','1','2','3') AND RIGHT([service_on_042a], 1) in ('0','1','2','4','6','7','8') THEN 'Functionally Obsolete' \
                            WHEN [appr_road_eval_072] IN ('0','1','2','3') THEN 'Functionally Obsolete' \
                            WHEN [structural_eval_067] = '3' THEN 'Functionally Obsolete' \
                            WHEN [waterway_eval_071] = '3' AND RIGHT([service_on_042a], 1) in ('0','5','6','7','8','9') THEN 'Functionally Obsolete' \
                            END)\
                            WHERE [STRUCTURE_LEN_MT_049] > 6.096 AND YEAR_BUILT_027 < 2010")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE Comment: {}".format((e))

        # EARTHQUAKE SPECIFIC FIELDS
        
        # FLOOD SPECIFIC FIELDS        
        
        # CONDITION DATA TO FIT WITHIN MAX LIMITS
        # Length (round up to integer for hazus)
        try:
            cursor.execute("UPDATE "+hifldtable+" SET Length = ROUND([STRUCTURE_LEN_MT_049], 0)")
            conn.commit()
        except Exception as e:
            print " cursor execute UPDATE Length: {}".format((e))
        
        # Calculate the truncated fields
        try:
            cursor.execute("UPDATE "+hifldtable\
                           +" SET NumSpansTrunc = \
                            (CASE WHEN NumSpans>250 THEN 250 \
                            ELSE NumSpans END)")
            conn.commit()
            cursor.execute("UPDATE "+hifldtable\
                           +" SET YearRemodeled = \
                            (CASE WHEN YearRemodeled = 0 THEN NULL \
                            ELSE YearRemodeled END)")
            conn.commit()
        except:
            print " cursor execute TRUNC Fields to be under 40 exception"
            
except Exception as e:
    print " exception Calculate hifld_HighwayBridge Fields: {}".format((e))
print "Done"
print


print "Move data from the HIFLD staging table to the HAZUS tables."
try:
    for state in existingDatabaseList:
        print state
        hifldTable = "["+state+"]..[hifld_HighwayBridge]"
        hzTable = "["+state+"]..[hzHighwayBridge]"
        flTable = "["+state+"]..[flHighwayBridge]"
        eqTable = "["+state+"]..[eqHighwayBridge]"
        
        connectString = "Driver={SQL Server};Server="+userDefinedServer+\
                        ";Database="+state+";UID="+UserName+";PWD="+Password
        conn = pyodbc.connect(connectString, autocommit=False)
        cursor = conn.cursor()

        # Remove TEMP table...
        print " Remove hifld temp table..."
        try:
            if cursor.tables(table="hifld_highwaybridgetemp", tableType="TABLE").fetchone():
                print " dropping hifld temp table..."
                cursor.execute("DROP TABLE hifld_highwaybridgetemp")
                conn.commit()
                print " done"
        except Exception as e:
            print " cursor drop hifld temp exception: {}".format((e))

        # Remove HZ* rows
        print " Remove HAZUS rows from hzHighwayBridge"
        try:
            cursor.execute("TRUNCATE TABLE "+hzTable)
            conn.commit()
        except:
            print " cursor execute Delete HAZUS from hzHighwayBridge exception"
        print " done"
        
        print " Remove hazus rows from flHighwayBridge"
        try:
            cursor.execute("TRUNCATE TABLE "+flTable)
            conn.commit()
        except:
            print " cursor execute Delete HAZUS from flHighwayBridge exception"
        print " done"
            
        print " Remove hazus rows from eqHighwayBridge"
        try:
            cursor.execute("TRUNCATE TABLE "+eqTable)
            conn.commit()
        except:
            print " cursor execute Delete HAZUS from eqHighwayBridge exception"
        print " done"

        # Copy Rows from HIFLD to HAZUS hazard
        print " Copy rows from hifld_HighwayBridge to hzHighwayBridge..."
        try:
            cursor.execute("INSERT INTO "+hzTable+"\
                            (Shape, \
                            HighwayBridgeId, \
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
                            Shape, \
                            HighwayBridgeID, \
                            BridgeClass, \
                            CensusTractID, \
                            Name, \
                            Owner, \
                            BridgeType, \
                            Width, \
                            NumSpansTrunc, \
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
                            CostKUSD, \
                            Latitude, \
                            Longitude, \
                            Comment \
                            \
                            FROM "+hifldTable+\
                            " WHERE HighwayBridgeID IS NOT NULL \
                            AND BridgeClass IS NOT NULL \
                            AND CensusTractId IS NOT NULL \
                            ORDER BY HighwayBridgeID ASC")
            conn.commit()
        except:
            print " cursor execute Insert Into hzHighwayBridge exception"
        print " done"
        
        # Copy Rows from HIFLD to HAZUS flood
        print " Copy rows from hifld_HighwayBridge to flHighwayBridge..."
        try:
            cursor.execute("INSERT INTO "+flTable+\
                            " (HighwayBridgeID, \
                                Elevation) \
                            SELECT \
                            HighwayBridgeID, \
                            0 \
                            FROM "+hifldTable+\
                            " WHERE HighwayBridgeID IS NOT NULL \
                            AND BridgeClass IS NOT NULL \
                            AND CensusTractId IS NOT NULL \
                            ORDER BY HighwayBridgeID ASC")
            conn.commit()
        except:
            print " cursor execute Insert Into flHighwayBridgeexception"
        print " done"
        
        # Copy Rows from HIFLD to HAZUS earthquake
        print " Copy rows from hifld_HighwayBridge to eqHighwayBridge..."
        try:
            cursor.execute("INSERT INTO "+eqTable+" \
                            (HighwayBridgeID, \
                            SoilType, \
                            LqfSusCat, \
                            LndSusCat, \
                            WaterDepth) \
                            \
                            SELECT \
                            HighwayBridgeID, \
                            'D', \
                            0, \
                            0, \
                            5\
                            FROM "+hifldTable+\
                            " WHERE HighwayBridgeID IS NOT NULL \
                            AND BridgeClass IS NOT NULL \
                            AND CensusTractId IS NOT NULL \
                            ORDER BY HighwayBridgeID ASC")
            conn.commit()
        except:
            print " cursor execute Insert Into eqHighwayBridge exception"
            print 
        print " done"
        
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


