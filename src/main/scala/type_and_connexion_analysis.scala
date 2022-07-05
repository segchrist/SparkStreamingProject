import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.streaming.Trigger

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
    .option("delimiter", ";")
    .option("header","true")
    .load("src/datas/Vlib*.csv")

  // definir le schema pour le streaming
//  val newColumns = Seq("ID","Name", "Working","Capacity","FreeParking","TotalNumberOfBike", "AvailableMechanicBikes","AvailableElectricBikes",
//    "AvailablePaymentTerminal","VelibPossibleReturn","LastUpdate","Location","Municipality","INSEECode")
  vlibdf.printSchema()
  val vlibStream = sparkSession.readStream
    .schema(vlibdf.schema)
    .option("maxFilesPerTrigger",1)
    .option("encoding","utf-8")
    .format("csv")
    .option("header","true")
    .option("delimiter", ";")
    .load("src/datas/Vlib*.csv")

  println("VLib is streaming: " + vlibStream.isStreaming)
  //vlibdf.show()

  val elBikepermunicipality = vlibStream
    .selectExpr(
      "Municipality",
      "AvailableElectricBikes",
      "TotalNumberOfBike",
      "LastUpdate"
    )
    .groupBy(
      col("Municipality"), window(col("LastUpdate"),"1 hour"),col("TotalNumberOfBike")
    )
    .sum("AvailableElectricBikes")
    .withColumnRenamed("sum(AvailableElectricBikes)","nbElectricBike")
    .orderBy(col("TotalNumberOfBike").desc)

  /*val newdf = elBikepermunicipality.selectExpr("Municipality" ,"ElecBikePercentage").where("Municipality == 'Paris'")
    newdf.show()*/
  val query =  elBikepermunicipality.writeStream
    .format("console") // memory = store in-memory table
    //.queryName("availableBikePerMunicipality") // the name of the in-memory table
    //.option("checkpointlocation","src/checkpoint")
    .outputMode("append") // complete = all the counts should be in the table
    .start()
  query.awaitTermination()


/*  for (i <- 1 to 50 ) {
    sparkSession.sql(

      """
        |select * from availableBikePerMunicipality
        |""".stripMargin)
      .show(false)

    Thread.sleep(1000)
  }*/


}
