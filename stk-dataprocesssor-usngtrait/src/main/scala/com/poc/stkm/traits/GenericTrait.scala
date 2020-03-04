package com.poc.stkm.traits

import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame }



trait GenericTrait {
  
  val spark_warehouse_dir = "spark.sql.warehouse.dir"
  
  /**
  *  create Spark session without hive support
  *  @return SparkSession
  */
  def getSparkSession(appName: String): SparkSession = {
    SparkSession.builder
                .appName(appName)
                .master("local[*]")
                .getOrCreate()
  }
  
  /**
   *  Create Hive Enabled Spark session for Spark- Hive interaction
   *  @return SparkSession
   */
  def getHiveSparkSession(appName: String): SparkSession = {
    
    SparkSession.builder
                .enableHiveSupport()
                .master("local")
                .appName(appName)
                .config(spark_warehouse_dir, "/usr/app/warehouse/")
                .getOrCreate()
  }
  
  
  /**
  *  Read Datafile from the provided location of the file.
  *  @return DataFrame
  */
  
  def readFile(spark: SparkSession, filePath: String): DataFrame = {
    
    val dataFrame = spark.read
                         .text(filePath)
                         //.option(header_key, keepHeader)
                         
    dataFrame
  }
  
  /**
  *  Insert data in user provided database and table. 
  *  
  */
  def insertData():Unit={}
  
  
  
}