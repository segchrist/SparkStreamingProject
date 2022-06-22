import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object type_and_connexion_analysis extends App {

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
    .load("src/files/vlille-realtime*.csv")

  println("V Lille is streaming: " + vlibStream.isStreaming)
  vlibdf.show()


  val veloDispoPerEtatConnexion = vlibStream
    .selectExpr(
      "type",
      "nbVelosDispo",
      "datemiseajour"
    )
    .groupBy(
      col("type"), window(col("datemiseajour"),"1 hour")
    )
    .sum("nbVelosDispo").as("total_num")

  veloDispoPerEtatConnexion.writeStream
    .format("memory") // memory = store in-memory table
    .queryName("connexionvelodispo_stream") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start()


  for (i <- 1 to 50 ) {
    sparkSession.sql(

      """
        |select * from connexionvelodispo_stream
        |order by `sum(nbVelosDispo)` DESC
        |""".stripMargin)
      .show(false)

    Thread.sleep(1000)
  }

}
