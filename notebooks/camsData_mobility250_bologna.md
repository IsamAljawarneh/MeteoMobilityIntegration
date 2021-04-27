

```scala
%%configure -f
{
    "conf": {
        "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,harsha2010:magellan:1.0.5-s_2.11,com.esri.geometry:esri-geometry-api:1.2.1,commons-io:commons-io:2.6,org.apache.spark:spark-streaming_2.11:2.2.0,org.apache.spark:spark-sql_2.11:2.2.0",
        "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.11",
        "spark.dynamicAllocation.enabled": false
    }
}
```

    Starting Spark application



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>6</td><td>application_1619430760498_0010</td><td>spark</td><td>idle</td><td><a target="_blank" href="http://hn1-meteom.rczfdzygpd0udm3vfqzi0v2z5a.fx.internal.cloudapp.net:8088/proxy/application_1619430760498_0010/">Link</a></td><td><a target="_blank" href="http://wn2-meteom.rczfdzygpd0udm3vfqzi0v2z5a.fx.internal.cloudapp.net:30060/node/containerlogs/container_1619430760498_0010_01_000001/livy">Link</a></td><td>✔</td></tr></table>


    SparkSession available as 'spark'.



Current session configs: <tt>{u'kind': 'spark', u'conf': {u'spark.jars.packages': u'org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,harsha2010:magellan:1.0.5-s_2.11,com.esri.geometry:esri-geometry-api:1.2.1,commons-io:commons-io:2.6,org.apache.spark:spark-streaming_2.11:2.2.0,org.apache.spark:spark-sql_2.11:2.2.0', u'spark.dynamicAllocation.enabled': False, u'spark.jars.excludes': u'org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.11'}}</tt><br>



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>6</td><td>application_1619430760498_0010</td><td>spark</td><td>idle</td><td><a target="_blank" href="http://hn1-meteom.rczfdzygpd0udm3vfqzi0v2z5a.fx.internal.cloudapp.net:8088/proxy/application_1619430760498_0010/">Link</a></td><td><a target="_blank" href="http://wn2-meteom.rczfdzygpd0udm3vfqzi0v2z5a.fx.internal.cloudapp.net:30060/node/containerlogs/container_1619430760498_0010_01_000001/livy">Link</a></td><td>✔</td></tr></table>



```scala
/**
 * @Description: a spatial join based on Filter-refine approach for NYC taxicab data
 * @author: Isam Al Jawarneh
 * @date: 02/02/2019
 *last update: 14/04/2021
 */
```


```scala
sc.version
```

    res3: String = 2.2.0.2.6.3.84-1


```scala
import util.control.Breaks._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.ForeachWriter
import magellan._
import magellan.index.ZOrderCurve
import magellan.{Point, Polygon}

import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{collect_list, collect_set}
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable
import scala.concurrent.duration.Duration
import java.io.{BufferedWriter, FileWriter}
import org.apache.commons.io.FileUtils
import java.io.File
import scala.collection.mutable.ListBuffer
import java.time.Instant
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.sql.DataFrame
```

    import org.apache.spark.sql.DataFrame


```scala
val containerStorageName = "meteomobilitysparkdenis-2021-04-24t15-54-44-656z"
val storageAccountName = "meteomobilityhdistorage"
```

    storageAccountName: String = meteomobilityhdistorage


```scala
/////////////////////////////
/// Definition of schemas ///
/////////////////////////////
```


```scala
val aerosolDataSchema = StructType(Array(
    StructField("Latitude", DoubleType, false),
    StructField("Longitude", DoubleType, false),
    StructField("Value", DoubleType, false),
    StructField("dataDate", StringType, false),
    StructField("time", StringType, false),
    StructField("shortName", StringType, false)))
```

    aerosolDataSchema: org.apache.spark.sql.types.StructType = StructType(StructField(Latitude,DoubleType,false), StructField(Longitude,DoubleType,false), StructField(Value,DoubleType,false), StructField(dataDate,StringType,false), StructField(time,StringType,false), StructField(shortName,StringType,false))


```scala
val mobilityDataSchema = StructType(Array(
    StructField("Code", StringType, false),
    StructField("Timestamp", StringType, false),
    StructField("Value", DoubleType, false),
    StructField("Latitude", DoubleType, false),
    StructField("Longitude", DoubleType, false),
    StructField("Status", StringType, false),
    StructField("Other_Value", DoubleType, false),
    StructField("Other_Date", StringType, false)))
```

    mobilityDataSchema: org.apache.spark.sql.types.StructType = StructType(StructField(Code,StringType,false), StructField(Timestamp,StringType,false), StructField(Value,DoubleType,false), StructField(Latitude,DoubleType,false), StructField(Longitude,DoubleType,false), StructField(Status,StringType,false), StructField(Other_Value,DoubleType,false), StructField(Other_Date,StringType,false))


```scala
/////////////////////////////
///// Import Dataframes /////
/////////////////////////////
```


```scala
//"wasb[s]://<BlobStorageContainerName>@<StorageAccountName>.blob.core.windows.net/<path>"
val aerosolData = (spark.read.format("csv")
                        .option("header", "true")
                        .schema(aerosolDataSchema)
                        .csv("wasbs://" + containerStorageName + "@" + storageAccountName + ".blob.core.windows.net/data/cams_air_data/*")
                        .withColumn("timestamp", to_timestamp(concat($"dataDate", lit(" "), $"time"), "yyyyMMdd HHmm"))
                        .withColumn("Point", point($"Longitude",$"Latitude"))
                        .drop("Longitude", "Latitude", "dataDate", "time"))
```

    aerosolData: org.apache.spark.sql.DataFrame = [Value: double, shortName: string ... 2 more fields]


```scala
val mobilityData = (spark.read.format("csv")
                    .option("header", "true")
                    .option("delimiter", ";")
                    .schema(mobilityDataSchema)
                    .csv("wasbs://" + containerStorageName + "@" + storageAccountName + ".blob.core.windows.net/data/mobility_data/db250mila.csv")
                    .withColumn("Timestamp", to_timestamp($"Timestamp", "yyyy-MM-dd HH:mm:ss.SSS"))
                    .withColumn("point", point($"Longitude",$"Latitude"))
                    .drop("Status", "Other_Value", "Other_Date"))
```

    mobilityData: org.apache.spark.sql.DataFrame = [Code: string, Timestamp: timestamp ... 4 more fields]


```scala
val Row(minLon: Double, maxLon: Double) = mobilityData.agg(min("Longitude"), max("Longitude")).head
```

    minLon: Double = -75.684
    maxLon: Double = 140.115


```scala
val Row(minLat: Double, maxLat: Double) = mobilityData.agg(min("Latitude"), max("Latitude")).head
```

    minLat: Double = 35.052
    maxLat: Double = 59.9254


```scala
mobilityData.show()
```

    +--------+-------------------+-----+--------+---------+--------------------+
    |    Code|          Timestamp|Value|Latitude|Longitude|               point|
    +--------+-------------------+-----+--------+---------+--------------------+
    |20091560|2014-10-22 12:35:37| 33.0|   43.58|  13.5056|Point(13.5056, 43...|
    |20091561|2014-10-22 12:35:37| 42.0|   43.58|  13.5056|Point(13.5056, 43...|
    |20091562|2014-10-22 12:35:37| 36.0|   43.58|  13.5056|Point(13.5056, 43...|
    |20091563|2014-10-22 12:35:37| 37.5|   43.58|  13.5056|Point(13.5056, 43...|
    |20091564|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    |20091565|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    |20091566|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    |20091567|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    |20091568|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    |20091569|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    |20091570|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    |20091571|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    |20091572|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    |20091573|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    |20091574|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    |20091575|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    |20091576|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    |20091577|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    |20091578|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    |20091579|2014-10-22 12:35:37| 33.0|   43.58|  13.5057|Point(13.5057, 43...|
    +--------+-------------------+-----+--------+---------+--------------------+
    only showing top 20 rows


```scala

```


```scala
//////////////////
/// Geohashing ///
//////////////////
```


```scala
// a user defined function to get geohash from long/lat point 
val geohashUDF = udf{(curve: Seq[ZOrderCurve]) => curve.map(_.toBase32())}
```

    geohashUDF: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,ArrayType(StringType,true),Some(List(ArrayType(org.apache.spark.sql.types.ZOrderCurveUDT@61007700,true))))


```scala
val precision = 30
```

    precision: Int = 30


```scala
//getting plain data from CSV file (file with point Data Structure) and use UDF to get geohashes
val geohashedAerosolData = (aerosolData
                         .withColumn("index", $"point" index  precision)
                         .withColumn("geohashArray1", geohashUDF($"index.curve")))
val explodedGeohashedAerosolData = (geohashedAerosolData
                                 .explode("geohashArray1", "geohash")
                                 { a: mutable.WrappedArray[String] => a })
```

    warning: there was one deprecation warning; re-run with -deprecation for details
    explodedGeohashedAerosolData: org.apache.spark.sql.DataFrame = [Value: double, shortName: string ... 5 more fields]


```scala
explodedGeohashedAerosolData.show(2,false)
```

    +--------------+---------+-------------------+------------------+-------------------------------------------------------------------------------------------------------------------------------------------------+-------------+-------+
    |Value         |shortName|timestamp          |Point             |index                                                                                                                                            |geohashArray1|geohash|
    +--------------+---------+-------------------+------------------+-------------------------------------------------------------------------------------------------------------------------------------------------+-------------+-------+
    |1.287753193E-8|pm10     |2020-01-01 12:00:00|Point(11.3, 44.47)|[[ZOrderCurve(11.2939453125, 44.4671630859375, 11.304931640625, 44.47265625, 30, -4191437470107172864, 110001011101010100000101110101),Contains]]|[srbhcp]     |srbhcp |
    |2.210387251E-8|pm10     |2020-01-01 15:00:00|Point(11.3, 44.47)|[[ZOrderCurve(11.2939453125, 44.4671630859375, 11.304931640625, 44.47265625, 30, -4191437470107172864, 110001011101010100000101110101),Contains]]|[srbhcp]     |srbhcp |
    +--------------+---------+-------------------+------------------+-------------------------------------------------------------------------------------------------------------------------------------------------+-------------+-------+
    only showing top 2 rows


```scala
//getting plain data from CSV file (file with point Data Structure) and use UDF to get geohashes
val geohashedMobilityData = (mobilityData
                         .withColumn("index", $"point" index  precision)
                         .withColumn("geohashArray1", geohashUDF($"index.curve")))
val explodedGeohashedMobilityData = (geohashedMobilityData
                                 .explode("geohashArray1", "geohash")
                                 { a: mutable.WrappedArray[String] => a })
```

    warning: there was one deprecation warning; re-run with -deprecation for details
    explodedGeohashedMobilityData: org.apache.spark.sql.DataFrame = [Code: string, Timestamp: timestamp ... 7 more fields]


```scala
explodedGeohashedMobilityData.show(2,false)
```

    +--------+-------------------+-----+--------+---------+---------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+-------+
    |Code    |Timestamp          |Value|Latitude|Longitude|point                |index                                                                                                                                                 |geohashArray1|geohash|
    +--------+-------------------+-----+--------+---------+---------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+-------+
    |20091560|2014-10-22 12:35:37|33.0 |43.58   |13.5056  |Point(13.5056, 43.58)|[[ZOrderCurve(13.502197265625, 43.5772705078125, 13.51318359375, 43.582763671875, 30, -4191769556978499584, 110001011101001111010111110011),Contains]]|[sr9xgm]     |sr9xgm |
    |20091561|2014-10-22 12:35:37|42.0 |43.58   |13.5056  |Point(13.5056, 43.58)|[[ZOrderCurve(13.502197265625, 43.5772705078125, 13.51318359375, 43.582763671875, 30, -4191769556978499584, 110001011101001111010111110011),Contains]]|[sr9xgm]     |sr9xgm |
    +--------+-------------------+-----+--------+---------+---------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+-------+
    only showing top 2 rows


```scala
val rawBologna = (spark.read.format("magellan")
                  .option("type", "geojson")
                  .load("wasbs://" + containerStorageName + "@" + storageAccountName + ".blob.core.windows.net/data/geojson/Bologna_quartieri.geojson")
                  .select($"polygon", $"metadata"("NOMEQUART").as("Neighboorhood"))
                  )
val bologna = (rawBologna
               .withColumn("index", $"polygon" index  precision)
               .select($"polygon", $"index", $"Neighboorhood")
               .cache())
val zorderIndexedBologna = (bologna
                            .withColumn("index", explode($"index"))
                            .select("polygon", "index.curve", "index.relation","Neighboorhood")
                          )
val geohashedBologna = bologna.withColumn("geohashArray", geohashUDF($"index.curve"))
val explodedGeohashedBologna = geohashedBologna.explode("geohashArray", "geohash") { a: mutable.WrappedArray[String] => a }
explodedGeohashedBologna.count()
```

    res36: Long = 426


```scala
rawBologna.show()

```

    +--------------------+--------------+
    |             polygon| Neighboorhood|
    +--------------------+--------------+
    |magellan.Polygon@...|Borgo Panigale|
    |magellan.Polygon@...|        Navile|
    |magellan.Polygon@...|         Porto|
    |magellan.Polygon@...|          Reno|
    |magellan.Polygon@...|    San Donato|
    |magellan.Polygon@...| Santo Stefano|
    |magellan.Polygon@...|    San Vitale|
    |magellan.Polygon@...|     Saragozza|
    |magellan.Polygon@...|        Savena|
    +--------------------+--------------+


```scala
//joining geohashed trips with exploded geohashed neighborhood using filter-and-refine approach (.where($"point" within $"polygon") is refine --> using the brute force method ray casting for edge cases or false positives)
val aerosolDataInBologna = (explodedGeohashedBologna
                         .join(explodedGeohashedAerosolData,
                               explodedGeohashedBologna("geohash") === explodedGeohashedAerosolData("geohash"))
                         .where($"point" within $"polygon")
                        )
aerosolDataInBologna.show(3)
```

    +--------------------+--------------------+-------------+--------------------+-------+---------------+---------+-------------------+------------------+--------------------+-------------+-------+
    |             polygon|               index|Neighboorhood|        geohashArray|geohash|          Value|shortName|          timestamp|             Point|               index|geohashArray1|geohash|
    +--------------------+--------------------+-------------+--------------------+-------+---------------+---------+-------------------+------------------+--------------------+-------------+-------+
    |magellan.Polygon@...|[[ZOrderCurve(11....|    Saragozza|[srbhbd, srbhbe, ...| srbhcp|1.0493138802E-8|     pm10|2020-01-31 15:00:00|Point(11.3, 44.47)|[[ZOrderCurve(11....|     [srbhcp]| srbhcp|
    |magellan.Polygon@...|[[ZOrderCurve(11....|    Saragozza|[srbhbd, srbhbe, ...| srbhcp|1.7718040368E-8|     pm10|2020-01-31 12:00:00|Point(11.3, 44.47)|[[ZOrderCurve(11....|     [srbhcp]| srbhcp|
    |magellan.Polygon@...|[[ZOrderCurve(11....|    Saragozza|[srbhbd, srbhbe, ...| srbhcp|1.7840015687E-8|     pm10|2020-01-30 15:00:00|Point(11.3, 44.47)|[[ZOrderCurve(11....|     [srbhcp]| srbhcp|
    +--------------------+--------------------+-------------+--------------------+-------+---------------+---------+-------------------+------------------+--------------------+-------------+-------+
    only showing top 3 rows


```scala
aerosolDataInBologna.columns
```

    res40: Array[String] = Array(polygon, index, Neighboorhood, geohashArray, geohash, Value, shortName, timestamp, Point, index, geohashArray1, geohash)


```scala
val mobiltyDataInBologna = (explodedGeohashedBologna
                         .join(explodedGeohashedMobilityData,
                               explodedGeohashedBologna("geohash") === explodedGeohashedMobilityData("geohash"))
                         .where($"point" within $"polygon")
                        )
mobiltyDataInBologna.show(3)
```

    +--------------------+--------------------+-------------+--------------------+-------+--------+-------------------+------+--------+---------+--------------------+--------------------+-------------+-------+
    |             polygon|               index|Neighboorhood|        geohashArray|geohash|    Code|          Timestamp| Value|Latitude|Longitude|               point|               index|geohashArray1|geohash|
    +--------------------+--------------------+-------------+--------------------+-------+--------+-------------------+------+--------+---------+--------------------+--------------------+-------------+-------+
    |magellan.Polygon@...|[[ZOrderCurve(11....|    Saragozza|[srbhbd, srbhbe, ...| srbj1d|20091806|2014-10-22 12:36:30|  50.0| 44.4885|  11.3227|Point(11.3227, 44...|[[ZOrderCurve(11....|     [srbj1d]| srbj1d|
    |magellan.Polygon@...|[[ZOrderCurve(11....|    Saragozza|[srbhbd, srbhbe, ...| srbj1d|20091807|2014-10-22 12:36:30|  34.5| 44.4884|  11.3226|Point(11.3226, 44...|[[ZOrderCurve(11....|     [srbj1d]| srbj1d|
    |magellan.Polygon@...|[[ZOrderCurve(11....|    Saragozza|[srbhbd, srbhbe, ...| srbj1d|20091808|2014-10-22 12:36:30|28.062| 44.4885|  11.3226|Point(11.3226, 44...|[[ZOrderCurve(11....|     [srbj1d]| srbj1d|
    +--------------------+--------------------+-------------+--------------------+-------+--------+-------------------+------+--------+---------+--------------------+--------------------+-------------+-------+
    only showing top 3 rows


```scala
mobiltyDataInBologna.count()
```

    res44: Long = 135084


```scala

```


```scala

```


```scala

```


```scala

```


```scala

```


```scala

```


```scala

```
