package com.poc.stkm.traits

import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame, Row }
import org.apache.spark.sql._

trait CustomTrait extends GenericTrait{
  
  def cleansingData():Unit={}
  
  def processData():Unit={}
  
  
}