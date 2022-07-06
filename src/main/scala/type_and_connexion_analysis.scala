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

  // NOMBRE MOYEN DE VELO DISPO PAR JOUR DANS CHAQUE COMMUNE
  /*val avgNbBikePerDay = vlibStream
    .selectExpr("Municipality", "TotalNumberOfBike", "LastUpdate")
    .groupBy(col("Municipality"), window(col("LastUpdate"), "1 day"))
    .avg("TotalNumberOfBike")
    .withColumn("AvgNbBike",round(col("avg(TotalNumberOfBike)"),1))
    .drop(col("avg(TotalNumberOfBike)"))
    .orderBy("AvgNbBike")*/

// NOMBRE TOTAL DE VELO DISPO PAR TRANCHE HORAIRE DE DEUX HEURES
 /* val availableBikes = vlibStream
    .selectExpr("Municipality", "TotalNumberOfBike", "LastUpdate")
    .groupBy(col("Municipality"), window(col("LastUpdate"), "2 hour","1 hour"))
    .sum("TotalNumberOfBike")
    .withColumnRenamed("Sum(TotalNumberOfBike)","AvailableBikes")
    .orderBy("AvailableBikes")*/

  // NOMBRE MOYEN DE VELO DISPO PAR JOUR DANS CHAQUE COMMUNE POUR CHAQUE TYPE DE VELO
/*  val avgNbBikePerTypePerDay = vlibStream
    .selectExpr("Municipality", "AvailableMechanicBikes", "LastUpdate","AvailableElectricBikes")
    .groupBy(col("Municipality"), window(col("LastUpdate"), "1 day"))
    .avg("AvailableMechanicBikes","AvailableElectricBikes")
    .withColumn("AvgNbMechBike",round(col("avg(AvailableMechanicBikes)"),1))
    .withColumn("AvgNbElecBike",round(col("avg(AvailableElectricBikes)"),1))
    .drop(col("avg(AvailableElectricBikes)"))
    .drop(col("avg(AvailableMechanicBikes)"))
    .orderBy("AvgNbElecBike","AvgNbMechBike")*/

  // NOMBRE TOTAL DE VELO DISPO PAR TRANCHE HORAIRE DE DEUX HEURES
  /* val availableBikes = vlibStream
     .selectExpr("Municipality", "TotalNumberOfBike", "LastUpdate")
     .groupBy(col("Municipality"), window(col("LastUpdate"), "2 hour","1 hour"))
     .sum("TotalNumberOfBike")
     .withColumnRenamed("Sum(TotalNumberOfBike)","AvailableBikes")
     .orderBy("AvailableBikes")*/

  //POURCENTAGE DE STATION EN FONCTIONNEMENT
//  val numStationPerCom = vlibStream
//    .selectExpr("ID","Municipality")
//    .groupBy("Municipality")
//    .count()
//    .withColumnRenamed("count","nbBikePerCom")
//  val StationPartByMuni = vlibStream
//    .selectExpr( "Working", "Municipality" )
//    .groupBy(
//      col("Municipality"),
//      col("Working")
//    )
//    .count()
//
//  val percentStationByMunic = StationPartByMuni
//    .join(numStationPerCom,"Municipality")
//    //.withColumn("percentWorkingStationByMunic",round(col("count")/col("nbBikePerCom"),1))

  //POURCENTAGE DE STATION AVEEC RETOUR POSSIBLE
  val StationPartWithReturnByMuni = vlibStream
    .selectExpr("Municipality", "VelibPossibleReturn" )
    .groupBy(
      col("VelibPossibleReturn"),
      col("Municipality")
    )
    .count()




  /*
  val ms_velomin = ms_velo
    .selectExpr("Municipality", "window", "nbbike")
    .groupBy("Municipality")
    .agg(min("nbbike"))*/

  StationPartWithReturnByMuni.writeStream
    .format("console") // memory = sotre in-memory table
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("truncate", "false")
    //.queryName("velo_dispo") // the name of the in-memory table
    .outputMode("complete")
    .start()
    .awaitTermination()
/*
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
    .format("console")
    .queryName("nbAvailableBikesByMunicipality_stream")
    .outputMode("complete")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()
    .awaitTermination()
*/

/*  val elBikepermunicipality = vlibStream
    .selectExpr(
      "Municipality",
      "AvailableElectricBikes",
      "TotalNumberOfBike",
      "LastUpdate"
    )
    .groupBy(
      col("Municipality"), window(col("LastUpdate"),"2 hours"),col("TotalNumberOfBike")
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
    .outputMode("complete") // complete = all the counts should be in the table
    .start()
  query.awaitTermination()*/


/*  for (i <- 1 to 50 ) {
    sparkSession.sql(

      """
        |select * from availableBikePerMunicipality
        |""".stripMargin)
      .show(false)

    Thread.sleep(1000)
  }*/


}
