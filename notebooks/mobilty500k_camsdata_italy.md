

```scala
%%configure -f
{
    "conf": {
        "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,harsha2010:magellan:1.0.5-s_2.11,com.esri.geometry:esri-geometry-api:1.2.1,commons-io:commons-io:2.6,org.apache.spark:spark-streaming_2.11:2.2.0,org.apache.spark:spark-sql_2.11:2.2.0",
        "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.11",
        "spark.dynamicAllocation.enabled": true,
        "spark.shuffle.service.enabled": true
    }
}

```


Current session configs: <tt>{u'kind': 'spark', u'conf': {u'spark.jars.packages': u'org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,harsha2010:magellan:1.0.5-s_2.11,com.esri.geometry:esri-geometry-api:1.2.1,commons-io:commons-io:2.6,org.apache.spark:spark-streaming_2.11:2.2.0,org.apache.spark:spark-sql_2.11:2.2.0', u'spark.dynamicAllocation.enabled': True, u'spark.shuffle.service.enabled': True, u'spark.jars.excludes': u'org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.11'}}</tt><br>



No active sessions.



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

    Starting Spark application



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>9</td><td>application_1619521572498_0009</td><td>spark</td><td>idle</td><td><a target="_blank" href="http://hn0-denis.oakn5vh0d33u3cgh5twveiaz3a.fx.internal.cloudapp.net:8088/proxy/application_1619521572498_0009/">Link</a></td><td><a target="_blank" href="http://wn2-denis.oakn5vh0d33u3cgh5twveiaz3a.fx.internal.cloudapp.net:30060/node/containerlogs/container_e01_1619521572498_0009_01_000001/livy">Link</a></td><td>✔</td></tr></table>


    SparkSession available as 'spark'.
    import org.apache.spark.sql.DataFrame


```scala
val containerStorageName = "denis-2021-04-27t09-50-33-745z"
val storageAccountName = "denishdistorage"
```

    storageAccountName: String = denishdistorage


```scala
/////////////////////////////
/// Definition of schemas ///
/////////////////////////////

val aerosolDataSchema = StructType(Array(
    StructField("Latitude", DoubleType, false),
    StructField("Longitude", DoubleType, false),
    StructField("Value", DoubleType, false),
    StructField("dataDate", StringType, false),
    StructField("time", StringType, false),
    StructField("shortName", StringType, false)))

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

//"wasb[s]://<BlobStorageContainerName>@<StorageAccountName>.blob.core.windows.net/<path>"
val aerosolData = (spark.read.format("csv")
                        .option("header", "true")
                        .schema(aerosolDataSchema)
                        .csv("wasbs://" + containerStorageName + "@" + storageAccountName + ".blob.core.windows.net/data/cams_air_data/cams_data_italy_right_dates.csv")
                        .withColumn("Timestamp", to_timestamp(concat($"dataDate", lit(" "), $"time"), "yyyyMMdd HHmm"))
                        .withColumn("Point", point($"Longitude",$"Latitude"))
                        .drop("dataDate", "time"))

val mobilityData = (spark.read.format("csv")
                    .option("header", "true")
                    .option("delimiter", ";")
                    .schema(mobilityDataSchema)
                    .csv("wasbs://" + containerStorageName + "@" + storageAccountName + ".blob.core.windows.net/data/mobility_data/db500mila.csv")
                    .withColumn("Timestamp", to_timestamp($"Timestamp", "yyyy-MM-dd HH:mm:ss.SSS"))
                    .withColumn("point", point($"Longitude",$"Latitude"))
                    .drop("Status", "Other_Value", "Other_Date"))

val minDate = mobilityData.select($"Timestamp").where($"Timestamp".isNotNull).orderBy(asc("Timestamp")).first().mkString(",")
val maxDate = mobilityData.select($"Timestamp").orderBy(desc("Timestamp")).first().mkString(",")
val datesString = f"""Min date: $minDate
Max date: $maxDate"""
print(datesString)
```

    Exception in thread Thread-146:
    Traceback (most recent call last):
      File "/usr/bin/anaconda/lib/python2.7/threading.py", line 801, in __bootstrap_inner
        self.run()
      File "/usr/bin/anaconda/lib/python2.7/threading.py", line 754, in run
        self.__target(*self.__args, **self.__kwargs)
      File "/usr/bin/anaconda/lib/python2.7/site-packages/sparkprogressindicator/sparkmonitorbackend.py", line 196, in _check_jobs
        self._send_msgs_for_fast_job(next_job)
      File "/usr/bin/anaconda/lib/python2.7/site-packages/sparkprogressindicator/sparkmonitorbackend.py", line 290, in _send_msgs_for_fast_job
        self._send_job_start(job)
      File "/usr/bin/anaconda/lib/python2.7/site-packages/sparkprogressindicator/sparkmonitorbackend.py", line 375, in _send_job_start
        stage = stage_dict[stageId]
    KeyError: 222
    


    Min date: 2014-10-22 12:35:37.0
    Max date: 2014-11-06 13:28:25.0


```scala
/////////////////////////////////////////////
/// Boundary coordinates for moblity data ///
/////////////////////////////////////////////

println("Longitude: " + mobilityData.agg(min("Longitude"), max("Longitude")).head.toString() + "\nLatitude: " + mobilityData.agg(min("Latitude"), max("Latitude")).head.toString())
```

    Longitude: [-75.684,140.115]
    Latitude: [35.052,59.9254]


```scala
//////////////////
/// Geohashing ///
//////////////////

// a user defined function to get geohash from long/lat point
val geohashUDF = udf{(curve: Seq[ZOrderCurve]) => curve.map(_.toBase32())}

val precision = 30
```

    precision: Int = 30


```scala
////////////////////////////
/// Geohash aerosol data ///
////////////////////////////

val geohashedAerosolData = (aerosolData
                            .withColumn("index", $"point" index  precision)
                            .withColumn("geohashArray1", geohashUDF($"index.curve")))
val explodedGeohashedAerosolData = (geohashedAerosolData
                                    .explode("geohashArray1", "geohash")
                                    { a: mutable.WrappedArray[String] => a })
explodedGeohashedAerosolData.printSchema()
```

    root
     |-- Latitude: double (nullable = true)
     |-- Longitude: double (nullable = true)
     |-- Value: double (nullable = true)
     |-- shortName: string (nullable = true)
     |-- Timestamp: timestamp (nullable = true)
     |-- Point: point (nullable = false)
     |-- index: array (nullable = false)
     |    |-- element: struct (containsNull = true)
     |    |    |-- curve: zordercurve (nullable = false)
     |    |    |-- relation: string (nullable = false)
     |-- geohashArray1: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- geohash: string (nullable = true)


```scala
/////////////////////////////
/// Geohash mobility data ///
/////////////////////////////


val geohashedMobilityData = (mobilityData
                         .withColumn("index", $"point" index  precision)
                         .withColumn("geohashArray1", geohashUDF($"index.curve")))
val explodedGeohashedMobilityData = (geohashedMobilityData
                                 .explode("geohashArray1", "geohash")
                                 { a: mutable.WrappedArray[String] => a })

explodedGeohashedMobilityData.printSchema()

```

    root
     |-- Code: string (nullable = true)
     |-- Timestamp: timestamp (nullable = true)
     |-- Value: double (nullable = true)
     |-- Latitude: double (nullable = true)
     |-- Longitude: double (nullable = true)
     |-- point: point (nullable = false)
     |-- index: array (nullable = false)
     |    |-- element: struct (containsNull = true)
     |    |    |-- curve: zordercurve (nullable = false)
     |    |    |-- relation: string (nullable = false)
     |-- geohashArray1: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- geohash: string (nullable = true)


```scala
/////////////////////////////////////////////////
/// Import and Geohash the polygon of Bologna ///
/////////////////////////////////////////////////

// The final schema is unified as:
// root
//  |-- polygon: polygon (nullable = true)
//  |-- index: array (nullable = false)
//  |    |-- element: struct (containsNull = true)
//  |    |    |-- curve: zordercurve (nullable = false)
//  |    |    |-- relation: string (nullable = false)
//  |-- Neighborhood: string (nullable = true)
//  |-- Province: string (nullable = true)
//  |-- Region: string (nullable = true)
//  |-- geohashArray: array (nullable = true)
//  |    |-- element: string (containsNull = true)
//  |-- geohash: string (nullable = true)

val rawBologna = (spark.read.format("magellan")
                  .option("type", "geojson")
                  .load("wasbs://" + containerStorageName + "@" + storageAccountName + ".blob.core.windows.net/data/geojson/Bologna_quartieri.geojson")
                  .select($"polygon", $"metadata"("NOMEQUART").as("Neighboorhood"))
                  )
val bologna = (rawBologna
               .withColumn("index", $"polygon" index  precision)
               .withColumn("Province", lit("Bologna"))
               .withColumn("Region", lit("Emilia-Romagna"))
               .select($"polygon", $"index", $"Neighboorhood", $"Province", $"Region")
               .cache())
val zorderIndexedBologna = (bologna
                            .withColumn("index", explode($"index"))
                            .select("polygon", "index.curve", "index.relation","Neighboorhood", "Province", "Region")
                          )
val geohashedBologna = bologna.withColumn("geohashArray", geohashUDF($"index.curve"))
val explodedGeohashedBologna = geohashedBologna.explode("geohashArray", "geohash") { a: mutable.WrappedArray[String] => a }
explodedGeohashedBologna.count()
```

    res142: Long = 426


```scala
explodedGeohashedBologna.select("*").where(explodedGeohashedBologna("geohash")==="spzvpt").show()
```

    +--------------------+--------------------+--------------+--------+--------------+--------------------+-------+
    |             polygon|               index| Neighboorhood|Province|        Region|        geohashArray|geohash|
    +--------------------+--------------------+--------------+--------+--------------+--------------------+-------+
    |magellan.Polygon@...|[[ZOrderCurve(11....|Borgo Panigale| Bologna|Emilia-Romagna|[spzvpt, spzvpw, ...| spzvpt|
    +--------------------+--------------------+--------------+--------+--------------+--------------------+-------+


```scala
///////////////////////////////////////////////
/// Import and Geohash the polygon of Italy ///
///////////////////////////////////////////////

// The final schema is unified as:
// root
//  |-- polygon: polygon (nullable = true)
//  |-- index: array (nullable = false)
//  |    |-- element: struct (containsNull = true)
//  |    |    |-- curve: zordercurve (nullable = false)
//  |    |    |-- relation: string (nullable = false)
//  |-- Neighborhood: string (nullable = true)
//  |-- Province: string (nullable = true)
//  |-- Region: string (nullable = true)
//  |-- geohashArray: array (nullable = true)
//  |    |-- element: string (containsNull = true)
//  |-- geohash: string (nullable = true)


val rawItaly = (spark.read.format("magellan")
                  .option("type", "geojson")
                  .load("wasbs://" + containerStorageName + "@" + storageAccountName + ".blob.core.windows.net/data/geojson/Italy_quartieri.geojson")
                  .select($"polygon",
                          $"metadata"("name").as("Neighborhood"),
                          $"metadata"("prov_name").as("Province"),
                          $"metadata"("reg_name").as("Region"))
                  )
val italy = (rawItaly
               .withColumn("index", $"polygon" index  precision)
               .select($"polygon", $"index", $"Neighborhood", $"Province", $"Region")
               .cache())
val zorderIndexedItaly = (italy
                            .withColumn("index", explode($"index"))
                            .select("polygon", "index.curve", "index.relation", "Neighborhood", "Province", "Region")
                          )
val geohashedItaly = italy.withColumn("geohashArray", geohashUDF($"index.curve"))
val explodedGeohashedItaly = geohashedItaly.explode("geohashArray", "geohash") { a: mutable.WrappedArray[String] => a }
explodedGeohashedItaly.count()
```

    res163: Long = 752793


```scala
explodedGeohashedItaly.select("Province").where($"Province" === "Bologna").show()
```

    +--------+
    |Province|
    +--------+
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    | Bologna|
    +--------+
    only showing top 20 rows


```scala
val explodedGeohashedItalyNoBologna = explodedGeohashedItaly.filter(col("Neighborhood") =!= "Bologna")
```

    explodedGeohashedItalyNoBologna: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [polygon: polygon, index: array<struct<curve:zordercurve,relation:string>> ... 5 more fields]


```scala
explodedGeohashedItalyNoBologna.printSchema()
```

    root
     |-- polygon: polygon (nullable = true)
     |-- index: array (nullable = false)
     |    |-- element: struct (containsNull = true)
     |    |    |-- curve: zordercurve (nullable = false)
     |    |    |-- relation: string (nullable = false)
     |-- Neighborhood: string (nullable = true)
     |-- Province: string (nullable = true)
     |-- Region: string (nullable = true)
     |-- geohashArray: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- geohash: string (nullable = true)


```scala
explodedGeohashedBologna.printSchema()

```

    root
     |-- polygon: polygon (nullable = true)
     |-- index: array (nullable = false)
     |    |-- element: struct (containsNull = true)
     |    |    |-- curve: zordercurve (nullable = false)
     |    |    |-- relation: string (nullable = false)
     |-- Neighboorhood: string (nullable = true)
     |-- Province: string (nullable = false)
     |-- Region: string (nullable = false)
     |-- geohashArray: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- geohash: string (nullable = true)


```scala
val allCities = explodedGeohashedItalyNoBologna.union(explodedGeohashedBologna)
```

    allCities: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [polygon: polygon, index: array<struct<curve:zordercurve,relation:string>> ... 5 more fields]


```scala
val rawTripsJoinedBSO = (explodedGeohashedMobilityData
                         .join(
                             allCities, 
                             explodedGeohashedMobilityData("geohash") === allCities("geohash"))
                         .select("point", "Neighborhood", "Province", "Region", "Timestamp", "Code", "Value", "Latitude", "Longitude")
                         .where($"point" within $"polygon").cache())
// val min_lat = rawTripsJoinedBSO.select($"Latitude").distinct.orderBy(asc("Latitude")).first().mkString(",").toDouble
// val max_lat = rawTripsJoinedBSO.select($"Latitude").distinct.orderBy(desc("Latitude")).first().mkString(",").toDouble
// val min_lon = rawTripsJoinedBSO.select($"Longitude").distinct.orderBy(asc("Longitude")).first().mkString(",").toDouble
// val max_lon = rawTripsJoinedBSO.select($"Longitude").distinct.orderBy(desc("Longitude")).first().mkString(",").toDouble
// val coordinates_string = f"""The coordinate extremes are:
// Longitude: ($min_lon, $max_lon)
// Latitude: ($min_lat, $max_lat)"""

```

    Exception in thread Thread-97:
    Traceback (most recent call last):
      File "/usr/bin/anaconda/lib/python2.7/threading.py", line 801, in __bootstrap_inner
        self.run()
      File "/usr/bin/anaconda/lib/python2.7/threading.py", line 754, in run
        self.__target(*self.__args, **self.__kwargs)
      File "/usr/bin/anaconda/lib/python2.7/site-packages/sparkprogressindicator/sparkmonitorbackend.py", line 192, in _check_jobs
        self._send_job_start(next_job)
      File "/usr/bin/anaconda/lib/python2.7/site-packages/sparkprogressindicator/sparkmonitorbackend.py", line 375, in _send_job_start
        stage = stage_dict[stageId]
    KeyError: 165
    


    Min date: 2014-10-22 12:35:37.0
    Max date: 2014-11-06 13:28:25.0


```scala
val minDate = rawTripsJoinedBSO.select("Timestamp").where($"Timestamp".isNotNull).orderBy(asc("Timestamp")).first().mkString(",")
val maxDate = rawTripsJoinedBSO.select("Timestamp").orderBy(desc("Timestamp")).first().mkString(",")
val datesString = f"""Min date: $minDate
Max date: $maxDate"""
print(datesString)
```

    Exception in thread Thread-145:
    Traceback (most recent call last):
      File "/usr/bin/anaconda/lib/python2.7/threading.py", line 801, in __bootstrap_inner
        self.run()
      File "/usr/bin/anaconda/lib/python2.7/threading.py", line 754, in run
        self.__target(*self.__args, **self.__kwargs)
      File "/usr/bin/anaconda/lib/python2.7/site-packages/sparkprogressindicator/sparkmonitorbackend.py", line 196, in _check_jobs
        self._send_msgs_for_fast_job(next_job)
      File "/usr/bin/anaconda/lib/python2.7/site-packages/sparkprogressindicator/sparkmonitorbackend.py", line 290, in _send_msgs_for_fast_job
        self._send_job_start(job)
      File "/usr/bin/anaconda/lib/python2.7/site-packages/sparkprogressindicator/sparkmonitorbackend.py", line 375, in _send_job_start
        stage = stage_dict[stageId]
    KeyError: 219
    


    Min date: 2014-10-22 12:35:37.0
    Max date: 2014-11-06 13:28:25.0


```scala
rawTripsJoinedBSO.select($"Longitude").distinct.orderBy(desc("Longitude")).first().mkString(",").toDouble
```

    res168: Double = 18.2276


```scala
///////////////////////////////////////////
/// Join CAMS data with the whole Italy ///
///////////////////////////////////////////
val rawCAMSJoinedBSO = (explodedGeohashedAerosolData
                         .join(
                             allCities, 
                             explodedGeohashedAerosolData("geohash") === allCities("geohash"))
                         .select("point", "Neighborhood", "Province", "Region", "Timestamp", "Value", "Latitude", "Longitude")
                         .where($"point" within $"polygon"))

val min_lat = rawCAMSJoinedBSO.select($"Latitude").distinct.orderBy(asc("Latitude")).first().mkString(",").toDouble
val max_lat = rawCAMSJoinedBSO.select($"Latitude").distinct.orderBy(desc("Latitude")).first().mkString(",").toDouble
val min_lon = rawCAMSJoinedBSO.select($"Longitude").distinct.orderBy(asc("Longitude")).first().mkString(",").toDouble
val max_lon = rawCAMSJoinedBSO.select($"Longitude").distinct.orderBy(desc("Longitude")).first().mkString(",").toDouble
val coordinates_string = f"""The coordinate extremes are:
Longitude: ($min_lon, $max_lon)
Latitude: ($min_lat, $max_lat)"""
print(coordinates_string)
```

    The coordinate extremes are:
    Longitude: (8.207, 17.958)
    Latitude: (37.979, 46.229)


```scala

```


```scala

```
