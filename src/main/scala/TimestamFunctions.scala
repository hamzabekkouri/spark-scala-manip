import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TimestamFunctions {

  // Spark SQL provides built-in standard Date and Timestamp (includes date and time) Functions defines in DataFrame API,
  // these come in handy when we need to make operations on date and time. All these accept input as, Date type, Timestamp
  // type or String. If a String, it should be in a format that can be cast to date, such as yyyy-MM-dd and timestamp in yyyy-MM-dd HH:mm:ss.SSSS
  // and returns date and timestamp respectively; also returns null if the input data was a string that could not be cast to date and timestamp.
  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("TimestamFunctions")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.sqlContext.implicits._

    // We will see how to get the current date and convert date into a specific date format using date_format() with Scala example.
    // Below example parses the date and converts from ‘yyyy-dd-mm’ to ‘MM-dd-yyyy’ format.
    val df = Seq(("2019-01-23")).toDF("Input").select(current_date().as("current_date"), col("Input"), date_format(col("Input"), "MM-dd-yyyy").as("format"))
    // Below example converts string in date format ‘MM/dd/yyyy’ to a DateType ‘yyyy-MM-dd’ using to_date() with Scala example.
    val df2 = Seq(("04/13/2019")).toDF("Input").select( col("Input"), to_date(col("Input"), "MM/dd/yyyy").as("to_date"))
    // Below example returns the difference between two dates using datediff() with Scala example.
    import org.apache.spark.sql.functions._
    val df3 = Seq(("2019-01-23"),("2019-06-24"),("2019-09-20")).toDF("input").select( col("input"), current_date().as("current_date"), datediff(current_date(),col("input")).as("diff"))
    // Below example returns the months between two dates using months_between() with Scala language.
    val df4 = Seq(("2019-01-23"),("2019-06-24"),("2019-09-20")).toDF("date").select( col("date"), current_date(), datediff(current_date(),col("date")).as("datediff"), months_between(current_date(),col("date")).as("months_between"))
    // Below example truncates date at a specified unit using trunc() with Scala language.
    import org.apache.spark.sql.functions._
    val df5 = Seq(("2019-01-23"),("2019-06-24"),("2019-09-20")).toDF("input").select( col("input"), trunc(col("input"),"Month").as("Month_Trunc"), trunc(col("input"),"Year").as("Month_Year"), trunc(col("input"),"Month").as("Month_Trunc"))

    // add_months() , date_add(), date_sub()
    // Here we are adding and subtracting date and month from a given input.
    val df6 = Seq(("2019-01-23"),("2019-06-24"),("2019-09-20")).toDF("input")
      .select( col("input"),
        add_months(col("input"),3).as("add_months"),
        add_months(col("input"),-3).as("sub_months"),
        date_add(col("input"),4).as("date_add"),
        date_sub(col("input"),4).as("date_sub"))

    val df7 = Seq(("2019-01-23"),("2019-06-24"),("2019-09-20"))
      .toDF("input")
      .select( col("input"), year(col("input")).as("year"),
        month(col("input")).as("month"),
        dayofweek(col("input")).as("dayofweek"),
        dayofmonth(col("input")).as("dayofmonth"),
        dayofyear(col("input")).as("dayofyear"),
        next_day(col("input"),"Sunday").as("next_day"),
        weekofyear(col("input")).as("weekofyear")
      )

    // Spark TimeStamp Functions Examples
    // Below are most used examples of timestamp Functions

    /// 1 - current_timestamp() : Returns the current timestamp in spark default format yyyy-MM-dd HH:mm:ss
    val df8 = Seq((1)).toDF("seq")
    val curDate = df8.withColumn("current_date",current_date().as("current_date"))
                    .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))

    // to_timestamp() : Converts string timestamp to Timestamp type format.
    val dfDate = Seq(("07-01-2019 12 01 19 406"),
      ("06-24-2019 12 01 19 406"),
      ("11-16-2019 16 44 55 406"),
      ("11-16-2019 16 50 59 406")).toDF("input_timestamp")

    val dfDate2 = dfDate.withColumn("datetype_timestamp", to_timestamp(col("input_timestamp"),"MM-dd-yyyy HH mm ss SSS"))

    // hour(), Minute() and second()

    import org.apache.spark.sql.functions._
    val dftime = Seq(("2019-07-01 12:01:19.000"),
      ("2019-06-24 12:01:19.000"),
      ("2019-11-16 16:44:55.406"),
      ("2019-11-16 16:50:59.406")).toDF("input_timestamp")

    val df9 = dftime.withColumn("hour", hour(col("input_timestamp")))
      .withColumn("minute", minute(col("input_timestamp")))
      .withColumn("second", second(col("input_timestamp")))

    df9.show()
  }

}
