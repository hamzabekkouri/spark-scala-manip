import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType,FloatType}

object read_mesure_data
{
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("kafka_spark")
      .getOrCreate()

    val df = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.1.51:9092,192.168.1.52:9092,192.168.1.53:9092")
      .option("subscribe", "mesure")
      .option("startingOffsets", "earliest") // From starting
      .load()

    df.printSchema()

    //df.show(false)
    //org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start();;
    val data_temp = df.select(col("value").cast("string")).alias("csv").select("csv.*")
    val mesure = data_temp.selectExpr("split(value,',')[0] as mesure","split(value,',')[1] as datetime","split(value,',')[2] as mesure_value")
    val df2 = mesure.withColumn("year", year(mesure("datetime")).as("year"))
                    .withColumn("month", month(mesure("datetime")).as("month"))
                    .withColumn("day", dayofmonth(mesure("datetime")).as("day"))
                    .withColumn("hour", hour(mesure("datetime")).as("hour"))
                    .withColumn("minute", minute(mesure("datetime")).as("minute"))
                    .withColumn("second", second(mesure("datetime")).as("second"))

    val mesure_by_date = df2.groupBy("month","day","hour").agg(mean("mesure_value"))
    println(mesure.count())
    mesure.show(50)

    // val temp = mesure.filter(mesure("mesure") === "temperature")
    // val vibr = mesure.filter(mesure("mesure") === "vibration")
  }
}
