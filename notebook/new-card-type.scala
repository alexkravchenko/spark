

object Cells {
  import org.apache.spark._
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.rdd._
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.SparkSession


  val parse = (l:String) => (l.substring(0, 1).trim(), l.substring(49, 73).trim(), l.substring(74,86).trim(),  l.substring(87,90).trim(), l.substring(117,128).trim(), l.substring(90,103).trim(), l.substring(143,148))
  val parseRec = (l:String) => (l.substring(0, 1), l.substring(0, 230))

  case class Transaction(ind: String, name: String, city: String,prov: String, account_id:String,  amount:BigDecimal, cardType:String)

  /* ... new cell ... */

  val dataDir ="/opt/data/"
  val oracleURL = "jdbc:oracle:thin:@host:1521:sid"
  val sybaseUser= "user"
  val sybasePass= "password"
  val cardTypeDim ="schema.card_type_dim"

  /* ... new cell ... */



  val warehouseLocation = "/opt/data/spark-warehouse"
  val spark = SparkSession
     .builder()
     .appName("Analysis")
     .config("spark.sql.warehouse.dir", warehouseLocation)
     .enableHiveSupport()
     .getOrCreate()

  /* ... new cell ... */

   val txnsDF = spark.read.textFile(dataDir)
             .map (parseRec)
             .filter(x=> x._1=="D")
             .map({case(s1,s2) => s2.substring(0, 150)})
             .map(parse)
             .map( {case (ind, name, city, prov, account_id, amount, cardType) => Transaction(ind, name, city, prov, account_id, BigDecimal(amount), cardType)} )
             .toDF()

  /* load driver from  repo */

  :local-repo /opt/repo

  :dp com.oracle % ojdbc6 % 11.2.0.3

  /* ... new cell ... */

    val cardTypeDimDF = spark.read
                        .format("jdbc")
                        .option("url", oracleURL)
                        .option("dbtable", cardTypeDim)
                        .option("user", sybaseUser)
                        .option("password", sybasePass)
                        .option("driver","oracle.jdbc.OracleDriver")
                        .load()

  /* find new values for types */

  txnsDF.createOrReplaceTempView("a")
  cardTypeDimDF.createOrReplaceTempView("b")
  val allTypes = spark.sql("select cardtype from a group by cardtype")
  allTypes.createOrReplaceTempView("c")
  val newRecords = spark.sql("select c.cardtype from c LEFT JOIN b ON c.cardtype=b.cardtype where b.cardtype is null").show()

  /* ... new cell ... */
}
