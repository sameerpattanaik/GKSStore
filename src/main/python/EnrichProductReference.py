import configparser

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from src.main.python.gkfunctions import read_schema

spark = SparkSession.builder.appName("EnrichProductReference").master("local").getOrCreate()

# Fetching config file
config = configparser.ConfigParser()
config.read(r'../projectconfigs/config.ini')
inputLocation = config.get('paths', 'inputLocation')
outputLocation = config.get('paths', 'outputLocation')
landingSchemaFromConf = config.get('schema', 'landingFileSchema')

currDayZoneSuffix = "_09272022"
prevDayZoneSuffix = "_04062020"

# Reading the schema
validFileSchema = read_schema(landingSchemaFromConf)
productPriceReferenceSchema = StructType([
    StructField('Product_ID', StringType(), True),
    StructField('Product_Name', StringType(), True),
    StructField('Product_Price', IntegerType(), True),
    StructField('Product_Price_Currency', StringType(), True),
    StructField('Product_updated_date', TimestampType(), True)
])

# Reading valid data
validDataDF = spark.read \
    .schema(validFileSchema) \
    .option("delimiter", "|") \
    .option("header", True) \
    .csv(outputLocation + "Valid/ValidData" + currDayZoneSuffix)
validDataDF.createOrReplaceTempView("validData")

# Reading project reference
productPriceReferenceDF = spark.read\
    .schema(productPriceReferenceSchema)\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(inputLocation + "Products")
productPriceReferenceDF.createOrReplaceTempView("productPriceReferenceDF")

productEnrichedDF = spark.sql("Select a.Sale_ID, a.Product_ID, b.Product_Name, "
                              "a.Quantity_Sold, a.Vendor_ID, a.Sale_Date, "
                              "b.Product_Price * a.Quantity_Sold as Sale_Amount, "
                              "a.Sale_Currency "
                              "FROM validData a INNER JOIN productPriceReferenceDF b "
                              "ON a.Product_ID = b.Product_ID")
productEnrichedDF.write\
    .option("header", True)\
    .option("delimiter", "|")\
    .mode("overwrite")\
    .csv(outputLocation + "Enriched/SaleAmountEnrichment/SaleAmountEnriched" + currDayZoneSuffix)

