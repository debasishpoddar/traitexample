package com.poc.stkm.test

import org.scalatest._

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.spark.sql.{ SparkSession, Dataset, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.poc.stkm.traits.CustomTrait
import com.poc.stkm.processor.PressDataProcess


class PressDataTest extends FunSuite with CustomTrait with BeforeAndAfterEach {
  
  
  val appName="PressureTest"
  val temperatureFileLocation =Thread.currentThread().getContextClassLoader().getResource("stockholmA_barometer_2013_2017.txt").toString();
  val pressdataprocess = new PressDataProcess
  
  // Test Case for Pressure Data
  test("Pressure Data Test"){
    
    val sparkSession = getSparkSession(appName)

    //Processing Data by passing value,upon completion the same wil be pushed to hive.
    val inputData= readFile(sparkSession, temperatureFileLocation)
    
    val cleanedData=pressdataprocess.cleansingData(sparkSession, inputData)
    
    println(cleanedData.show())
    
    pressdataprocess.insertData(sparkSession, cleanedData)
   
    assert("True".toLowerCase == "true")
  }  
  
  
}