import org.apache.spark.sql.{SaveMode, SparkSession}

object df_rep_coalesce {

  def main(args: Array[String]): Unit = {
    /* DataFrame */

    val spark:SparkSession = SparkSession.builder()
      .master("local[5]")
      .appName("df_repartition_coalesce")
      .getOrCreate()

    val df = spark.range(0,20)
    println(df.rdd.partitions.length)

    df.write.mode(SaveMode.Overwrite).csv("src/file/partition.csv")

    val df2 = df.repartition(6)
    println(df2.rdd.partitions.length)
    df2.write.mode(SaveMode.Overwrite).csv("src/file/partition_rep.csv")

    val df3 = df.coalesce(2)
    println(df3.rdd.partitions.length)
    df3.write.mode(SaveMode.Overwrite).csv("partition_coa.csv")

    val df4 = df.groupBy("id").count()
    println(df4.rdd.getNumPartitions)
  }
}
