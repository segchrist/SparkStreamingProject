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
    .load("src/files/velib_renamed.csv")

  // definir le schema pour le streaming
  val vlibSchema = vlibdf.schema

  vlibdf.show(10)

  // definir Stream
  val vlibStream = sparkSession.readStream
    .schema(vlibSchema)
    .option("maxFilesPerTrigger",1)
    .format("csv")
    .option("header","true")
    .option("delimiter", ";")
    .load("src/files/velib_renamed*.csv")

  println("VLib is streaming: " + vlibStream.isStreaming)



  // Moyenne velos disponible, velos mecaniques, velos electriques par commune
  println("Moyenne velos disponible, velos mecaniques, velos electriques par commune")

  val NbAvailableBikesByMunicipality = vlibStream
    .selectExpr("Municipality",
      "TotalNumberOfBike",
      "AvailableMechanicBikes",
      "AvailableElectricBikes",
      "FreeParking")
    .groupBy(
      col("Municipality")
    )
    .sum()
  NbAvailableBikesByMunicipality.writeStream
    .format("console") // memory = sotre in-memory table
    .queryName("nbAvailableBikesByMunicipality_stream") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start()



//Top 5
  println("Top 5 velos disponible, velos mecaniques, velos electriques par commune")
  val Top5BikesByMunicipality = vlibStream
    .selectExpr("Municipality",
      "LastUpdate" ,"TotalNumberOfBike"
      )
    .groupBy(
      col("Municipality"), window(col("LastUpdate"), "1 hour")
    )
    .sum()
    .orderBy()

  Top5BikesByMunicipality.writeStream
    .format("console") // memory = sotre in-memory table
    .queryName("Top5BikesByMunicipality_stream") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start()

  /*for (i <- 1 to 5) {
    sparkSession.sql(
      """
        |select * from nbAvailableBikesByMunicipality_stream
        |""".stripMargin)
      .show(false)
    Thread.sleep(1000)
  }*/


  //Nb station par commune
  println("Nb stations par commune")
  val NbStationByMuni = vlibStream
    .selectExpr(  "Municipality"  )
    .groupBy(
      col("Municipality")
    )
    .count()


  NbStationByMuni.writeStream
    .format("memory") // memory = sotre in-memory table
    .queryName("NbStationByMuni_stream") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start()
  for (i <- 1 to 3) {
    sparkSession.sql(
      """
        |select * from NbStationByMuni_stream
      """.stripMargin)
      .show(false)
    Thread.sleep(1000)
  }

  // Aggregation
  /*
  vlibdf.select(count("StockCode")).show(false)
  vlibdf.select(countDistinct("StockCode")).show(false)
  vlibdf.select(approx_count_distinct("StockCode", 0.02)).show(false)
  vlibdf.select(first("StockCode"), last("StockCode")).show(false)
  */

}