import org.apache.spark.sql.SparkSession
object repartition_coelesce
{
  def main(args: Array[String]): Unit = {
    // overall
    // repartition : is used to increase or decrease the RDD, DataFrame, Dataset partitions
    // coalesce is used to only decrease the number of partitions in an efficient way.

    /* RDD */
    val spark:SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("repartition and coalesce")
      .getOrCreate()
    /* The above example provides local[5] as an argument to master() method meaning to run
     the job locally with 5 partitions.Though if you have just 2 cores on your system,
     it still creates 5 partition tasks. */
    val df = spark.range(0,20)
    println(df.rdd.partitions.length)

    // In RDD, you can create parallelism at the time of the creation of an RDD using parallelize(), textFile() and wholeTextFiles().
    val rdd = spark.sparkContext.parallelize(Range(0,20))
    println("From local[5]"+rdd.partitions.size)

    // distributes RDD into 6 partitions and the data is distributed
    val rdd1 = spark.sparkContext.parallelize(Range(0,25), 6)
    println("parallelize : "+rdd1.partitions.size)
    rdd1.saveAsTextFile("src/file/partition") // write data into 6 partitions

    val rddFromFile = spark.sparkContext.textFile("src/file/test.txt",10)
    println("TextFile : "+rddFromFile.partitions.size)

    // The below example decreases the partitions from 10 to 4 by moving data from all partitions
    // repartition re-distributes the data(as shown below) from all partitions which is full shuffle
    // leading to very expensive operation when dealing with billions and trillions of data.
    val rdd2 = rdd1.repartition(4)
    println("Repartition size : "+rdd2.partitions.size)
    rdd2.saveAsTextFile("src/file/repartirion")

    // coalesce() is used only to reduce the number of partitions.
    // coalesce is optimized or improved version of repartition()
    // the movement of the data across the partitions is lower using coalesce
    val rdd3 = rdd1.coalesce(4)
    println("Repartition size : "+rdd3.partitions.size)
    rdd3.saveAsTextFile("src/file/coalesce")
    // we will notice that partition 3 has been moved to 2
    // and Partition 6 has moved to 5,
    // resulting data movement from just 2 partitions.



  }

}
