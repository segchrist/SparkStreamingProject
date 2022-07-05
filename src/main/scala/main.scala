import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object main extends App {
  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("streaming")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")
  sparkSession.conf.set("spark.sql.shuffle.partitions","5")

  val vlibdf = sparkSession
    .read
    .format("csv")
    .option("inferSchema","true")
    .option("header","true")
    .option("delimiter", ";")
    .load("src/files/vlille-realtime.csv")

  // definir le schema pour le streaming
  val vlibSchema = vlibdf.schema

  val vlibStream = sparkSession.readStream
    .schema(vlibSchema)
    .option("maxFilesPerTrigger",1)
    .format("csv")
    .option("header","true")
    .option("delimiter", ";")
    .load("src/files/vlille-realtime.csv")

  println("V Lille is streaming: " + vlibStream.isStreaming)
  //vlibdf.show()
  println(vlibSchema)
  // Aggregation

  val velo_per_etat = vlibdf
    .select("etatConnexion","nbVelosDispo")
    .groupBy(
      col("etatConnexion")
    )
    .sum("nbVelosDispo").as("total_num")

  velo_per_etat.show(false)
  /*
  vlibdf.select(countDistinct("StockCode")).show(false)
  vlibdf.select(approx_count_distinct("StockCode", 0.02)).show(false)
  vlibdf.select(first("StockCode"), last("StockCode")).show(false)
  */

}
