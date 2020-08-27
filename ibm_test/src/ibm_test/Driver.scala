package ibm_test

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j._

object Driver {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val logger = Logger.getLogger("DriverLogger")
  
  def main(args: Array[String]) {
  
    val spark = SparkSession.builder().appName("EmployeeAnalysis").getOrCreate()
    // added .config("spark.sql.warehouse.dir", "file:///C:/temp").master("local[*]") while running on windows using winutils
        
    logger.info("Starting the Application")
    
    logger.info("Setting Hadoop Configurations for COS")
    Utils.configureHadoopCOS(spark)
    logger.info("Hadoop Configurations for COS Completed")
    
    logger.info(s"Reading CSV file at ${ConnectionDetails.path}") 
    val df = Utils.readCSV(ConnectionDetails.path, spark)
    logger.info(s"Reading CSV file at ${ConnectionDetails.path} Completed")
    
    logger.info(s"Saving Data in DB2")
    Utils.saveDataToDB2(df)
    logger.info("Data Stored in DB2")
    
    logger.info(s"Reading Data from DB2")
    val newDf = Utils.readFromDB2(spark).cache()
    logger.info("Completed reading data from db2 and created the dataframe")
    
    logger.info(" Calculating Gender ratio as Male:Female in each department")
    val grDf = Utils.findGenderRatio(newDf)
    logger.info(s"Completed finding gender ratio, now writing it to ${ConnectionDetails.grPath}")
    Utils.writeDataToCOS(grDf,spark,ConnectionDetails.grPath)
    
    logger.info(" Calculating average salary in each department")
    val avgSalDf = Utils.findAverageSalary(df)
    logger.info(s"Completed finding average salary, now writing it to ${ConnectionDetails.avgSalPath}")
    Utils.writeDataToCOS(avgSalDf,spark,ConnectionDetails.avgSalPath)
    
    logger.info(" Calculating salary gap between males and female in each department")
    val gapDf = Utils.findSalaryGap(df)
    logger.info(s"Completed finding salary gap, now writing it to ${ConnectionDetails.salGapPath}")
    Utils.writeDataToCOS(gapDf,spark,ConnectionDetails.salGapPath)
    
    logger.info("Copleted All the tasks. Closing the spark session now")
    spark.close()
    
    
  }

}
