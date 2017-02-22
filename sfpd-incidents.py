from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.functions import *

csvSchema = StructType([StructField('IncidentNum',LongType(),True),
                       StructField('Category',StringType(),True),
                       StructField('Description',StringType(),True),
                       StructField('DayOfWeek',StringType(),True),
                       StructField('Date',StringType(),True),
                       StructField('Time',StringType(),True),
                       StructField('PdDistrict',StringType(),True),
                       StructField('Resolution',StringType(),True),
                       StructField('Address',StringType(),True),
                       StructField('Latitude',DoubleType(),True),
                       StructField('Longitude',DoubleType(),True),
                       StructField('Location',StringType(),True),
                       StructField('PdId',LongType(),True)])
                   
df = spark.read.csv('SFPD_Incidents_-_from_1_January_2003.csv', header=True, schema=csvSchema) 

#Concatenating Date and Time column
concatdf = df.select('IncidentNum','Category','Description','DayOfWeek',concat(col('Date'), lit(' '), col('Time')).alias('DateTime'),'PdDistrict','Resolution','Address','Latitude','Longitude','Location','PdId')
concatdf.printSchema()
from_pattern1 = 'MM/dd/yyyy hh:mm'

#converting the datatype of the new column to timestamp
dfTS = concatdf.withColumn('DateTS', unix_timestamp(concatdf['DateTime'], from_pattern1).cast('timestamp')).drop('DateTime')
dfTS.cache() #lazily cached
dfTS.count() #performing actions to cache the RDD dfTS
dfTS.columns
dfTs.createOrReplaceTempView('sfpd')
spark.catalog.cacheTable('sfpd')
spark.table('sfpd').count()
display(dfTS.select('Category').distinct())

#displays the total incidents recorded for each category
display(dfTS.select('Category').groupBy('Category').count().orderBy('count', ascending=False))

#displays the total incidents recorded per year
display(dfTS.groupBy(year('DateTS')).count().orderBy('year(DateTS)'))

#incidents recorded in the month of January 2017
display(dfTS.filter(year('DateTS') == '2017').filter(dayofyear('DateTS') <= 31).groupBy(dayofyear('DateTS')).count().orderBy('dayofyear(DateTS)'))

#day of week that records maximum incidents
display(dfTS.select('DayOfWeek').groupBy('DayOfWeek').count().orderBy('count',ascending=False))

#incidents recorded from noon to midnight
display(dfTS.filter(hour('DateTS') >= 12).groupBy(hour('DateTS')).count().orderBy('hour(DateTS)',ascending=True))

