package ibm_test

object ConnectionDetails {
  val accessKey = "refer-mail"
  val secretKey = "refer-mail"

  val service = "myCos"

  val bucketName = "candidate-exercise"
  val path = s"cos://${bucketName}.${service}/emp-data.csv"
  val outputPath = s"cos://${bucketName}.${service}"
  val grPath = s"${outputPath}/gender_ratio.parquet"
  val avgSalPath = s"${outputPath}/average_salary.parquet"
  val salGapPath = s"${outputPath}/salary_gap.parquet"
  
  val db2DriverClass = "com.ibm.db2.jcc.DB2Driver"
  val database = "BLUDB"
  val username = "tjc45762"
  val password = "rsd68^xzk32kct65"
  val db2JdbcUrl = s"jdbc:db2://dashdb-txn-sbox-yp-dal09-14.services.dal.bluemix.net:50000/${database}"
  val db2Table = "TJC45762.Employees_new"
 
  
}