import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, sum}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object readStramKafka
{
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("kafka_spark")
      .getOrCreate()

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.1.51:9092,192.168.1.52:9092,192.168.1.53:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "latest") // latest used just for streaming | json string like """ {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}} """ | earliest for batch
      .option("failOnDataLoss",false)
      .load()

    val schema = new StructType()
      .add("id",IntegerType)
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("dob_year",IntegerType)
      .add("dob_month",IntegerType)
      .add("gender",StringType)
      .add("salary",IntegerType)
    val person = df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    // processing
    val group = person.groupBy("gender").agg(sum("salary").as("somme"))

    group.selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("checkpointLocation", "/opt/data/checkpoint")
      .outputMode("update")
      .option("kafka.bootstrap.servers", "192.168.1.51:9092,192.168.1.52:9092,192.168.1.53:9092")
      .option("topic", "test2")
      .start()
      .awaitTermination()
  }
}
