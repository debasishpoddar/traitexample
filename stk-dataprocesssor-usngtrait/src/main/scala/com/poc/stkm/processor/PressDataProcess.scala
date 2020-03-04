package com.poc.stkm.processor

import com.poc.stkm.traits.CustomTrait
import com.poc.stkm.model.PressureData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame, Row }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._

class PressDataProcess extends CustomTrait{
  
   val appName="Pressure Data Processing"
   val pressData = "pressure_data"
  
  
  def processData(dataLocation: String){
    
    // Create Hive enabled Session
    val sparkSession = getHiveSparkSession(appName)
    
    // Read data from the HDFS location
    val inputData= readFile(sparkSession, dataLocation)
    
    //Cleanses Data retrieved from hdfs location
    val cleanedData = cleansingData(sparkSession, inputData)
    
    //Inserts Cleaned Dataset into hive
    insertData(sparkSession, cleanedData)
  
  
  
}
 
   
   
    def cleansingData(spark:SparkSession,inputData: DataFrame): Dataset[PressureData] = {

    // Data cleansing and data creation
    //TODO more data cleansing can be added here
    
    import spark.implicits._

    val splitData = inputData
      .map { case Row(s: String) => s.split("\\s+") }
      .map(x => PressureData(x(0).toInt, x(1).toInt,
        x(2).toInt,
        if (x(3) == "NaN") BigDecimal(0) else BigDecimal(x(3)),
        if (x(4) == "NaN") BigDecimal(0) else BigDecimal(x(4)),
        if (x(5) == "NaN") BigDecimal(0) else BigDecimal(x(5))))

    splitData

  }
  
 
  
   def insertData(spark:SparkSession,dataFile: Dataset[PressureData]){
     
     import spark.implicits._
     dataFile.createOrReplaceTempView(pressData)
     
     
     try {
      spark.sql(s"""
         |Insert into table STKLM_PRES_DATA
         | select year , 
         | month, 
         | day, 
         | pMorn, 
         | pNoon, 
         | pEvn
         | from pressure_data         
         """.stripMargin)
         println("Data insertion completed")
    
    } catch {
      case ex: Exception => {
        println("Data insertion failed")
      }
    }
     
    
  }
  
   
}