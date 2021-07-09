import jdk.jfr.internal.handlers.EventHandler.timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._

object date_manip
{
  def main(args: Array[String]): Unit =
  {
    val spark = SparkSession.builder().appName("max date").master("local[*]").getOrCreate()
    import spark.implicits._ // used to apply ToDF function or ToRDD function
    val df = spark.sparkContext.parallelize(List(("2016-04-06 16:36", 1234, 111, 1), ("2016-04-06 17:35", 1234, 111, 5))).toDF("datetime", "userId", "memberId", "value")
    val df2 = df.withColumn("datetime",col("datetime").cast(DateType)).groupBy("userId","memberId").agg(max("datetime").as("max_date"))
    df2.show()
  }
}
