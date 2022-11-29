import configparser
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql import SparkSession
import configparser
from pyspark.sql import functions as psf
from src.main.python.gkfunctions import read_schema

# Initiating spark session
spark = SparkSession.builder.appName("DataIngestAndRefine").master("local").getOrCreate()

# Reading the configs
config = configparser.ConfigParser()
config.read(r'../projectconfigs/config.ini')
inputlocation = config.get('paths', 'inputlocation')
# outputlocation = config.get('paths', 'outputlocation')
# holdFileSchemaFromConf = config.get('schema', 'holdFileSchema')
# landingFileSchemaFromConf = config.get('schema', 'landingFileSchema')
# landingFileSchema = read_schema(landingFileSchemaFromConf)
# holdFileSchema = read_schema(holdFileSchemaFromConf)

# Reading landing zone

landingFileSchema = StructType([
    StructField('Sale_ID', StringType(), True),
    StructField('Product_ID', StringType(), True),
    StructField('Quantity_Sold', IntegerType(), True),
    StructField('Vendor_ID', StringType(), True),
    StructField('Sale_Date', TimestampType(), True),
    StructField('Sale_Amount', DoubleType(), True),
    StructField('Sale_Currency', StringType(), True)
])

# Reading Landing Zone

landingFileDF = spark.read \
    .schema(landingFileSchema) \
    .option("delimiter", "|") \
    .csv(inputlocation)

landingFileDF.show()

    # .csv(inputlocation + "Sales_Landing/SalesDump" + currentDaySuffix)


# #Handling Dates
# today = datetime.now()
# yesterdayDate = today - timedelta(1)
# #DDMMYYYY
# currDaySuffix = "_" + today.strftime("%m%d%Y")
# prevDaySuffix = "_" + yesterdayDate.strftime("%m%d%Y")
# # print(currentDaySuffix)
# # print(previousDaySuffix)
# currentDaySuffix = "_05062020"
# previousDaySuffix = "_04062020"
#

#
# landingFileDF.createOrReplaceTempView("landingFileDF")
#
# # Reading previous hold data
# previousHoldDF = spark.read \
#     .schema(holdFileSchema) \
#     .option("delimiter", "|") \
#     .option("header", True) \
#     .csv(outputlocation + "Hold/HoldData" + previousDaySuffix)
#
# previousHoldDF.createOrReplaceTempView("previousHoldDF")
#
# refreshLandingData = spark.sql("Select a.Sale_ID,a.Product_ID, "
#                                "CASE "
#                                "WHEN (a.Quantity_Sold is NULL) THEN b.quantity_Sold "
#                                "ELSE a.Quantity_Sold "
#                                "END AS Quantity_Sold,"
#                                "CASE "
#                                "WHEN (a.Vendor_ID is NULL) THEN b.Vendor_ID "
#                                "ELSE a.Vendor_ID "
#                                "END AS Vendor_ID,"
#                                "a.Sale_Date, a.Sale_Amount, a.Sale_Currency "
#                                "from landingFileDF a left outer join previousHoldDF b "
#                                "ON a.Sale_ID = b.Sale_ID")
#
# # refreshLandingData.show()
# # landingFileDF.show()
# #
# validLandingDF = refreshLandingData.filter(psf.col("Quantity_Sold").isNotNull() & psf.col("Vendor_ID").isNotNull())
# validLandingDF.createOrReplaceTempView("validLandingDF")
# #
# releaseFromHold = spark.sql("Select vd.Sale_ID "
#                             "FROM validLandingDF vd INNER JOIN previousHoldDf phd "
#                             "ON vd.Sale_ID = phd.Sale_ID")
# releaseFromHold.createOrReplaceTempView("releaseFromHold")
#
# notReleaseFromHold = spark.sql("select * from previousHoldDF "
#                                "where Sale_ID NOT IN (SELECT Sale_ID FROM releaseFromHold)")
# #
# notReleaseFromHold.createOrReplaceTempView("notReleaseFromHold")
#
# invalidLandingDF = refreshLandingData\
#     .filter(psf.col("Quantity_Sold").isNull() | psf.col("Vendor_ID").isNull())\
#     .withColumn("Hold_Reason",psf
#                 .when(psf.col("Quantity_Sold").isNull(), "Qty sold missing")
#                 .otherwise(psf.when(psf.col("Vendor_ID").isNull(), "Vendor ID is missing")))\
#     .union(notReleaseFromHold)
#
#
# validLandingDF.write\
#     .mode("overwrite")\
#     .option("delimiter", "|")\
#     .option("header", True)\
#     .csv(outputlocation + "Valid/ValidData" + currDaySuffix)
#
# invalidLandingDF.write\
#     .mode("overwrite")\
#     .option("delimiter", "|")\
#     .option("header", True)\
#     .csv(outputlocation + "Hold/HoldData" + currDaySuffix)
#
#
# # invalidDF.show()
# # validDF.show()