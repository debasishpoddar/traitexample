package com.poc.stkm.test

import org.scalatest._

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import com.poc.stkm.traits.CustomTrait
import com.poc.stkm.processor.TempDataProcess


class TempDataTest extends FunSuite with CustomTrait with BeforeAndAfterEach {
  
  
  val appName="TemperatureTest"
  val temperatureFileLocation =Thread.currentThread().getContextClassLoader().getResource("stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt").toString();
  val tempdataprocess = new TempDataProcess
  
  // Test Case for Temperature Data
  test("Temperature Data Test"){
    
    val sparkSession = getSparkSession(appName)

    //Processing Data by passing value,upon completion the same wil be pushed to hive.
    val inputData= readFile(sparkSession, temperatureFileLocation)
    
    val cleanedData=tempdataprocess.cleansingData(sparkSession, inputData)
    
    println(cleanedData.show())
    
    tempdataprocess.insertData(sparkSession, cleanedData)
   
    assert("True".toLowerCase == "true")
  }  
  
  
}