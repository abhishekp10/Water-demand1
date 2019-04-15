                package nl.rug.sc

                import com.datastax.spark.connector.SomeColumns
                import com.datastax.spark.connector.cql.{ClusteringColumn, ColumnDef, PartitionKeyColumn, RegularColumn, TableDef}
                import com.datastax.spark.connector.types.{IntType, TextType}
                import org.apache.spark.sql.{Dataset, Row, SparkSession}
                import org.apache.spark.SparkContext._
                import org.apache.spark.rdd.RDD
                import org.apache.spark.SparkConf

                import scala.collection.TraversableLike
                import scala.collection.generic.CanBuildFrom
                import org.apache.spark.streaming.{Seconds, StreamingContext}
                import org.apache.spark.streaming.dstream.{DStream, InputDStream}
                import org.apache.spark.streaming.kafka.KafkaUtils
                import kafka.serializer.StringDecoder
                import org.apache.spark.sql.types.StructType
                import com.datastax.spark.connector.cql.{ClusteringColumn, ColumnDef, PartitionKeyColumn, RegularColumn, TableDef}
                import com.datastax.spark.connector.types._
                import com.datastax.spark.connector.streaming._
                import spire.syntax.group




                /**
                  *
                  * Stream from Kafka
                  */

                case class RawWeatherData( loc: String, yyyy: Int, mm: Int, dd: Int,dr: Double, tg: Double, ug: Double)


                class SparkExample(sparkSession: SparkSession, pathToCsv: String) {
                  private val sparkContext = sparkSession.sparkContext
                  sparkSession.stop()
                  case class Weather( loc: String, yyyy: Int, mm: Int, dd: Int,dr: Double, tg: Double, ug: Double)
                  case class WeatherId(loc: String, yyyy: Int, mm: Int, dd: Int,dr: Double)
                  val spark = SparkSession.builder
                    .config("spark.master", "local[5]")
                    .getOrCreate()


                   spark.conf.set("spark.master", "local[5]")
                    spark.conf.set("spark.cassandra.connection.host", "127.0.0.1")
                  val sc= spark.sparkContext


                  //val sparkConf = new SparkConf().setAppName("Raw Weather")
                  //sparkConf.setIfMissing("spark.master", "local[5]")
                  //sparkConf.setIfMissing("spark.cassandra.connection.host", "127.0.0.1")




                  def testStream(): Unit = {
                    //def main(args: Array[String]) {

                    // update
                    // val checkpointDir = "./tmp"


                    //sparkSession.stop()
                    //val sparkConf = new SparkConf().setAppName("Raw Weather")
                    //sparkConf.setIfMissing("spark.master", "local[5]")
                    //   sparkConf.setIfMissing("spark.checkpoint.dir", checkpointDir)
                    //sparkConf.setIfMissing("spark.cassandra.connection.host", "127.0.0.1")


                    val ssc = new StreamingContext(sc, Seconds(3))

                    val kafkaTopicRaw = "weather"
                    val kafkaBroker = "127.0.01:9092"

                    val cassandraKeyspace = "dbks1"
                    val cassandraTableRaw = "knmi_w"
                    //val cassandraTableDailyPrecip = "daily_aggregate_precip"

                    //val lines = ssc.socketTextStream("127.0.0.1", 9042)


                    val datatable = ssc.cassandraTable("dbks1", "knmi_w")


                    //Create Kafka Stream with the Required Broker and Topic
                    def kafkaConsumeStream(ssc: StreamingContext): DStream[(String, String)] = {
                      val topicsSet = kafkaTopicRaw.split(",").toSet
                      val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBroker)
                      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
                        ssc, kafkaParams, topicsSet)
                    }

                    val lines = kafkaConsumeStream(ssc) //.map { event => event }






                    // Define columns
                    val loccol = new ColumnDef("loc", PartitionKeyColumn, IntType)
                    val yyyycol = new ColumnDef("yyyy", ClusteringColumn(0), TextType)
                    val mmcol = new ColumnDef("mm", RegularColumn, TextType)
                    val ddcol = new ColumnDef("dd", RegularColumn, TextType)
                    val drcol = new ColumnDef("dr", RegularColumn, TextType)
                    val tgcol = new ColumnDef("tg", RegularColumn, TextType)
                    val ugcol = new ColumnDef("ug", RegularColumn, TextType)
                    // Create table definition
                    val table = new TableDef("dbks1", "knmi_w", Seq(loccol), Seq(yyyycol), Seq(mmcol, ddcol, drcol, tgcol, ugcol))
                    // Map rdd into custom data structure and create table
                    val userdf = lines
                        .map { case (_, value) => value.split(',') }
                        .map(row => RawWeatherData(row(0).toString, row(1).toInt, row(2).toInt, row(3).toInt, row(4).toDouble, row(5).toDouble, row(6).toDouble))
                    //.map(_.split(",")).map(row => RawWeatherData(row(0).toString, row(1).toInt, row(2).toInt, row(3).toInt, row(4).toDouble, row(5).toDouble, row(6).toDouble))
                    userdf.saveToCassandra("dbks1", "knmi_w")

                    //lines.print
                    lines.foreachRDD(rdd =>
                        println("#####################rdd###################### " + rdd.first)

                    )

                    ssc.start()
                    ssc.awaitTermination() // Wait for the computation to terminate






                    ssc.stop()



                  }


/**


                case class Weather( loc: String, yyyy: Int, mm: Int, dd: Int,dr: Double, tg: Double, ug: Double)
                case class WeatherId(loc: String, yyyy: Int, mm: Int, dd: Int,dr: Double)
                //case class KeyValue(loc: String, yyyy: Int, mm: Int, dd: Int,pf: Double)

                class SparkExample(sparkSession: SparkSession, pathToCsv: String) {


                  private val sparkContext = sparkSession.sparkContext
                  sparkSession.stop()




                 //val conf = new SparkConf(true)
                 //   .set("spark.cassandra.connection.host","127.0.0.1") .setAppName("cassandra").setMaster("local[*]")





                  //  .set("spark.cassandra.connection.native.port", "9042")
                  //  .set("spark.cassandra.connection.rpc.port", "9160")
                  //val sc = new SparkContext(conf)

                  val spark = SparkSession.builder
                    .config("spark.master", "local")
                    .getOrCreate()

*/

                  def testExample(): Unit = {



                    val sc= spark.sparkContext

                    import spark.implicits._

                    val dataset = spark
                                  .read
                                  .format("org.apache.spark.sql.cassandra")
                                  .options(Map("table" -> "knmi_w", "keyspace" -> "dbks1"))
                                  .load()


                    dataset.show()




                    val dataset1 = dataset.collect()


                    val betaz = 0.01457
                    val betay = 0.00154
                    val betax = 0.000000098
                    val beta0 = 0.0008124



                    val rows = dataset1
                                            .map(line => Weather(
                                                  line.getAs[String]("loc"),
                                                  line.getAs[Int]("yyyy"),
                                                  line.getAs[Int]("mm"),
                                                  line.getAs[Int]("dd"),
                                                  line.getAs[Double]("dr"),
                                                  line.getAs[Double]("tg"),
                                                  line.getAs[Double]("ug")
                                                                ) )


                     val KNMI_idx = rows.zipWithIndex

                    val idx_row  = KNMI_idx.map{case (y,v) => (v,y)}

                    val idx_key = idx_row.map{case (y,v) => (y,(v.loc,v.yyyy,v.mm,v.dd))}

                    //idx_row.foreach(println)


                    //Predictive factor

                      val pred_factor   = idx_row
                                            .map{case(y,x) => (y,(( (x.dr * betaz) + (x.tg * betay)) + (x.ug * betaz) + beta0))}

                    //Logistic Function

                      val logistic_func = pred_factor
                                            .map{case(y,x)=> (y,(1 / (1+ math.log(x))))}




                    //logistic_func.foreach(println)

                      val result = for {

                        (k1,v1) <- idx_key
                        (k2,v2) <- logistic_func
                        if (k1 == k2)

                      } yield (v1,v2)

                    result.foreach(println)


                   // val saveresult = sc.parallelize(result).saveToCassandra("dbks1","knmi_r")







/**

                    //streaming logic starts here
                    import org.apache.spark.sql.types._


                    val myschema = StructType(Array(
                      StructField("Year",IntegerType),
                      StructField("Consumption",IntegerType)
                    ))



                    import spark.implicits._

                    val df1 = spark
                      .readStream
                      //.format("sep",",")
                      .schema(myschema)
                      .csv("/home/abhishekp10/Documents/labs/testdata")

                    val df = spark
                      .readStream
                      .format("kafka")
                      .option("kafka.bootstrap.servers", "localhost:9092")
                      .option("subscribe", "population")
                      .load()

                    df.selectExpr("CAST(key as STRING)","CAST(value AS  STRING)")
                        .as[(String,String)]




                    df.printSchema()




                    df.writeStream
                      .format("console")
                      //.outputMode("complete")
                      .option("truncate","false")
                      .start()
                      .awaitTermination()




                    //df1.show()
                    */

                  }





                    /**
                    val KNMI_rdd = sc.cassandraTable[Weather]("dbks1", "knmi_w")
                      .keyBy[(WeatherId) ]("loc", "yyyy", "mm", "dd")
                      .select("loc", "yyyy", "mm", "dd","dr","tg","ug")
                      .collect()


                    val data = sc.parallelize(KNMI_rdd)

                    data.collect.foreach(println)

                    val res = data.map(x => x) */






                  /**  val Table_count = KNMI_rdd.count()

                   val KNMI_data = sc.parallelize(1 to Table_count.toInt)
                                    .joinWithCassandraTable("dbks1","knmi_w")
                                     .select( "yyyy","mm","dd","dr","tg","ug")
                                     .map(x => x)
                      KNMI_data.foreach(println)
                    KNMI_rdd.groupByKey.toDebugString
                    KNMI_rdd.groupByKey().count

                    //val parr_rdd = sc.parallelize(List(KNMI_rdd))

                    //val parr = parr_rdd.toString()
                    //val delimited = parr.flatMap ( x => x.split(",")(0).toInt , x.split(",")(1))


                    //println(parr_rdd)

                     // KNMI_rdd.map(x => x)

                    //println(KNMI_rdd.first)
                    //println(KNMI_rdd.count) */


                    /**
                    val Table_count = KNMI_rdd.count()
                    val KNMI_idx = KNMI_rdd.zipWithIndex
                    val idx_key = KNMI_idx.map{case (k,v) => (v,k)}

                    var i = 0
                    var n : Int = Table_count.toInt


                    println(Table_count)

                    for ( i  <- 1 to n if i < n) {
                      //println(KNMI_rdd.first)
                      println(i)

                      //test_spark_rdd.foreach(println)

                      val Row = idx_key.lookup(i)

                      println(Row)


                      val firstRow = Row(0)



                      val yyyy_var = firstRow.get[Int]("yyyy")
                      val mm_var = firstRow.get[Double]("mm")
                      val dd_var = firstRow.get[Double]("dd")
                      val dr_var = firstRow.get[Double]("dr")
                      val tg_var = firstRow.get[Double]("tg")
                      val ug_var = firstRow.get[Double]("ug")
                      val loc_var = firstRow.get[String]("loc")



                      val pred_factor = (((0.15461 * tg_var) + (0.8954 * ug_var)) / ((0.0000451 * dr_var) + 0.0004487))




                      println(yyyy_var,mm_var,dd_var,loc_var)
                      //println(dr_var)
                      //println(tg_var)
                      //println(ug_var)
                      println(pred_factor)

                    }



                  } */
                  /**
                  /**
                    * An example using RDD's, try to avoid RDDs
                    */
                  def rddExample(): Unit = {
                    val data = List(1, 2, 3, 4, 5)
                    val rdd = sparkContext.parallelize(data)

                    rdd
                      .map(x => x + 1) // Increase all numbers by one (apply the transformation x => x + 1 to every item in the set)
                      .collect() // Collect the data (send all data to the driver)
                      .foreach(println) // Print each item in the list

                    printContinueMessage()
                  }

                  /**
                    * An example using Data Frames, improvement over RDD but Data Sets are preferred
                    */
                  def dataFrameExample(): Unit = {
                    import sparkSession.implicits._ // Data in dataframes must be encoded (serialized), these implicits give support for primitive types and Case Classes
                    import scala.collection.JavaConverters._

                    val schema = StructType(List(
                      StructField("number", IntegerType, true)
                    ))

                    val dataRow = List(1, 2, 3, 4, 5)
                      .map(Row(_))
                      .asJava

                    val dataFrame = sparkSession.createDataFrame(dataRow, schema)

                    dataFrame
                      .select("number")
                      .map(row => row.getAs[Int]("number")) // Dataframe only has the concept of Row, we need to extract the column "number" and convert it to an Int
                      .map(_ + 1) // Different way of writing x => x + 1
                      .collect()
                      .foreach(println)

                    dataFrame.printSchema() // Data frames and data sets have schemas

                    printContinueMessage()
                  }

                  /**
                    * An example using Data Sets, improvement over both RDD and Data Frames
                    */
                  def dataSetExample(): Unit = {
                    import sparkSession.implicits._ // Data in datasets must be encoded (serialized), these implicits give support for primitive types and Case Classes

                    val dataSet = sparkSession.createDataset(List(1, 2, 3, 4, 5))

                    dataSet
                      .map(_ + 1) // Different way of writing x => x + 1
                      .collect()
                      .foreach(println)

                    dataSet.printSchema()

                    printContinueMessage()
                  }

                  /**
                    * Advanced data set example using Scala's Case Classes for more complex data, note that the Case Class is defined at the top of this file.
                    */
                  def dataSetAdvancedExample(): Unit = {
                    import sparkSession.implicits._

                    val dataSet = sparkSession.createDataset(List(
                      Person(1, "Alice", 5.5),
                      Person(2, "Bob", 8.6),
                      Person(3, "Eve", 10.0)
                    ))

                    dataSet
                      .show() // Shows the table

                    printContinueMessage()

                    dataSet
                      .map(person => person.grade) // We transform the Person(int, string, double) to a double (Person => Double), extracting the person's grade
                      .collect() // Collect in case you want to do something with the data
                      .foreach(println)

                    printContinueMessage()

                    // Even cleaner is
                    dataSet
                      .select("grade")
                      .show()

                    dataSet.printSchema()

                    printContinueMessage()
                  }

                  /**
                    * In your case, you will be reading your data from a database, or (csv, json) file, instead of creating the data in code as we have previously done.
                    * We use a CSV containing 3200 US cities as an example data set.
                    */
                  def dataSetRealisticExample(): Unit = {
                    val dataSet = sparkSession.read
                      .option("header", "true") // First line in the csv is the header, will be used to name columns
                      .option("inferSchema", "true") // Infers the data types (primitives), otherwise the schema will consists of Strings only
                      .csv(pathToCsv) // Loads the data from the resources folder in src/main/resources, can also be a path on your storage device

                    dataSet.show() // Show first 20 results

                    printContinueMessage()

                    dataSet
                      .sort("name") // Sort alphabetically
                      .show()

                    printContinueMessage()

                    import sparkSession.implicits._ // For the $-sign notation

                    dataSet
                      .filter($"pop" < 10000) // Filter on column 'pop' such that only cities with less than 10k population remain
                      .sort($"lat") // Sort remaining cities by latitude, can also use $-sign notation here
                      .show()

                    printContinueMessage()

                    dataSet
                      .withColumn("name_first_letter", $"name".substr(0,1)) // Create a new column which contains the first letter of the name of the city
                      .groupBy($"name_first_letter") // Group all items based on the first letter
                      .count() // Count the occurrences per group
                      .sort($"name_first_letter") // Sort alphabetically
                      .show(26) // Returns the number of cities in the US for each letter of the alphabet, shows the first 26 results

                    printContinueMessage()

                    import org.apache.spark.sql.functions._ // For the round  (...) functionality

                    dataSet
                      .withColumn("pop_10k", (round($"pop" / 10000) * 10000).cast(IntegerType)) // Create a column which rounds the population to the nearest 10k
                      .groupBy("pop_10k") // Group by the rounded population
                      .count() // Count the occurences
                      .sort("pop_10k") // Sort from low to high
                      .show(100) // Returns the number of cities in 10k intervals

                    dataSet.printSchema()

                    printContinueMessage()
                  }

                  private def printContinueMessage(): Unit = {
                    println("Check your Spark web UI at http://localhost:4040 and press Enter to continue. [Press Backspace and Enter again if pressing enter once does not work]")
                    scala.io.StdIn.readLine()
                    println("Continuing, please wait....")
                  }*/

                }
