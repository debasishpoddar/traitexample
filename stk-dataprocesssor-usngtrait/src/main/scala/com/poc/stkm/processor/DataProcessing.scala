package com.poc.stkm.processor

object DataProcessing {
  
  def main(args: Array[String]) {
    
    if (args.length == 0) {
     println("Please provide the temparature & pressure data file(s) location.")
      System.exit(1)
    }
    
    val tempdataprocess = new TempDataProcess
    val pressdataprocess = new PressDataProcess
    
    tempdataprocess.processData(args(0))
    pressdataprocess.processData(args(1))
    
  }
  
}