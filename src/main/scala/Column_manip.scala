import org.apache.spark.sql.Encoders.DATE
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Column_manip {

  def main(args: Array[String]): Unit = {

    val  spark = SparkSession.builder().appName("max date").master("local[*]").getOrCreate()

    // define data that is a sequence of row that contain some salaries of some people
    val simpleData = Seq(
      Row("James",34,"2006-01-01","true","M",3000.60),
      Row("Michael",33,"1980-01-10","true","F",3300.80),
      Row("Robert",37,"06-01-1992","false","M",5000.50))

    val simpleSchema = StructType(Array(
      StructField("firstName",StringType,true),
      StructField("age",IntegerType,true),
      StructField("jobStartDate",StringType,true),
      StructField("isGraduated", StringType, true),
      StructField("gender", StringType, true),
      StructField("salary", DoubleType, true)
    ))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData),schema = simpleSchema)

    // methode 1
    val df2 = df.withColumn("age",col("age").cast(StringType))
               .withColumn("isGraduated",col("isGraduated").cast(BooleanType))
              .withColumn("jobStartDate",col("jobStartDate").cast(DateType))
    //methode 2
    /*val cast_df = df.select(df.columns.map
     {
       case column@"age" => col(column).cast("string").as(column)
       case column@"salary" => col(column).cast("string").as(column)
       case column => col(column)
     }:_*)*/
    // methode 3
    val df3 = df2.selectExpr("cast(age as int) age",
      "cast(isGraduated as string) isGraduated",
      "cast(jobStartDate as string) jobStartDate")

    // methode 4 : We can also use SQL expression to change the spark DataFram column type.
    df3.createOrReplaceTempView("CastExample")
    val df4 = spark.sql("SELECT STRING(age),BOOLEAN(isGraduated), DATE(jobStartDate) from CastExample")

    df4.printSchema()
    df4.show(false)
  }

}
