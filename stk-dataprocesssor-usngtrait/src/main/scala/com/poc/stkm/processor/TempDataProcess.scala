package com.poc.stkm.processor

import com.poc.stkm.traits.CustomTrait
import com.poc.stkm.model.TemperatureData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame, Row }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._

class TempDataProcess extends CustomTrait {
  
  val appName="Temperature Data Processing"
  val tempData = "temperature_data"
  
  
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
   
   
  def cleansingData(spark:SparkSession,inputData: DataFrame): Dataset[TemperatureData] = {

    // Data cleansing and data creation
    //TODO more data cleansing can be added here
    
    import spark.implicits._

    val splitData = inputData
                   .map{case Row(s: String) => s.split("\\s+")}
                   .map(x => TemperatureData(x(0).toInt, x(1).toInt,
                              x(2).toInt,
                              if(x(3)=="NaN") BigDecimal(0) else BigDecimal(x(3)),
                              if(x(4)=="NaN") BigDecimal(0) else BigDecimal(x(4)),
                              if(x(5)=="NaN") BigDecimal(0) else BigDecimal(x(5)),
                              if(x(6)=="NaN") BigDecimal(0) else BigDecimal(x(6)),
                              if(x(7)=="NaN") BigDecimal(0) else BigDecimal(x(7)),
                              if(x(8)=="NaN") BigDecimal(0) else BigDecimal(x(8))))

    splitData

  }
  
 
  
   def insertData(spark:SparkSession,dataFile: Dataset[TemperatureData]){
     
     import spark.implicits._
     dataFile.createOrReplaceTempView(tempData)
     
     
     try {
      spark.sql(s"""
         |Insert into table STKLM_TEMP_DATA
         | select year , 
         | month, 
         | day, 
         | pMorn, 
         | pNoon, 
         | pEvn
         | from temperature_data         
         """.stripMargin)
         println("Data insertion completed")
    
    } catch {
      case ex: Exception => {
        println("Data insertion failed")
      }
    }
     
    
  }
  
  
  
}