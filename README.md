Temperature & Barometer Data Processing


stk-evndata-processor : It is a spark job to process Temperature & Pressure Data through Scala and insert the same into Hive.

See the source code and internal documentation for further detail in the respective scala files:

Data Processing & Db Insertion Files : /src/main/scala/com/poc/stkm/processor/TempDataProcess.scala 
                                       /src/main/scala/com/poc/stkm/processor/PressDataProcess.scala
                                       /src/main/scala/com/poc/stkm/processor/DataProcessing.scala

Config Traits : /src/main/scala/com/poc/stkm/traits/CustomTrait.scala
                /src/main/scala/com/poc/stkm/traits/GenericTrait.scala

Temperature & Pressure Record Files : /src/main/scala/com/poc/stkm/model/PressureData.scala 
                                      /src/main/scala/com/poc/stkm/model/TemperatureData.scala

Test Cases : /src/test/scala/com/poc/stkm/test/PressDataTest.scala 
             /src/test/scala/com/poc/stkm/test/TempDataTest.scala

Resources : /src/test/resources/stockholmA_barometer_2013_2017.txt 
            /src/test/resources/stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt

Requirements :

1)Scala 
2)Spark 
3)Eclipse IDE 2017-18 with maven build enabled. 
4)Java JDK 1.8 
5)Should have JAVA_HOME set for the user in Environment variable.
