package ibm_test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.Logger

object Utils {

  val logger = Logger.getLogger("UtilsLogger")

  def readCSV(path: String, session: SparkSession): DataFrame = {
    session.read.option("inferSchema", "true").option("header", "true").csv(path)
  }

  def saveDataToDB2(df: DataFrame): Unit = {
    df.write.format("jdbc").mode("overwrite").option("url", ConnectionDetails.db2JdbcUrl)
      .option("user", ConnectionDetails.username)
      .option("password", ConnectionDetails.password)
      .option("dbtable", ConnectionDetails.db2Table)
      .option("driver", ConnectionDetails.db2DriverClass)
      .save()

  }

  def readFromDB2(spark: SparkSession): DataFrame = {
    spark.read.format("jdbc").option("url", ConnectionDetails.db2JdbcUrl)
      .option("user", ConnectionDetails.username)
      .option("password", ConnectionDetails.password)
      .option("dbtable", ConnectionDetails.db2Table)
      .option("driver", ConnectionDetails.db2DriverClass).load()
  }
  def findGenderRatio(df: DataFrame): DataFrame = {

    val newD = df.na.drop().groupBy("Department", "Gender").count
    val maleDf = newD.filter(lower(col("Gender")).equalTo("male")).withColumnRenamed("count", "male_count")
    val femaleDf = newD.filter(lower(col("Gender")).equalTo("female")).withColumnRenamed("count", "female_count")
    val joinDf = maleDf.join(femaleDf, Seq("Department"), "leftouter").withColumn("gender_ratio", (col("male_count") / col("female_count")))
      .select("Department", "gender_ratio")
    logger.info("Displaying result for Gender Ratio")
    joinDf.show(false)
    joinDf
  }

  def findAverageSalary(df: DataFrame): DataFrame = {
    val avgDf = df.withColumn("Salary", regexp_replace(df("Salary"), "\\$|,", "").cast(DoubleType)).groupBy("Department").agg(avg(col("Salary"))).withColumnRenamed("avg(Salary)", "average_salary")
    logger.info("Displaying result for Average Salary")
    avgDf.show(false)
    avgDf
  }

  def findSalaryGap(df: DataFrame): DataFrame = {
    val formattedDf = df.na.drop().withColumn("Salary", regexp_replace(df("Salary"), "\\$|,", "").cast(DoubleType))
    val filterDf = formattedDf.groupBy("Department", "Gender").avg("Salary").where(col("Department").isNotNull).where(col("Gender").isNotNull)
    val maleDf = filterDf.filter(lower(col("Gender")).equalTo("male")).where(col("Department").isNotNull).withColumnRenamed("avg(Salary)", "Salary")
    val femaleDf = filterDf.filter(lower(col("Gender")).equalTo("female")).where(col("Department").isNotNull).withColumnRenamed("avg(Salary)", "Salary")
    val joinedDf = maleDf.join(femaleDf, Seq("Department"), "leftouter")
      .withColumn("salary_gap", (maleDf("Salary") - femaleDf("Salary"))).select("Department", "salary_gap")
    logger.info("Displaying result for Salary Gap")
    joinedDf.show()
    joinedDf

  }

  def configureHadoopCOS(spark: SparkSession): Unit = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.stocator.scheme.list", "cos")
    hadoopConf.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    hadoopConf.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
    hadoopConf.set("fs.stocator.cos.scheme", "cos")
    hadoopConf.set(s"fs.cos.${ConnectionDetails.service}.endpoint", "https://s3.us.cloud-object-storage.appdomain.cloud")
    hadoopConf.set(s"fs.cos.${ConnectionDetails.service}.access.key", ConnectionDetails.accessKey)
    hadoopConf.set(s"fs.cos.${ConnectionDetails.service}.secret.key", ConnectionDetails.secretKey)

  }

  def writeDataToCOS(df: DataFrame, spark: SparkSession, path: String): Unit = {
    df.coalesce(1).write.format("parquet").mode("overwrite").save(path)
    logger.info(s"Data written at ${path}")
  }
}