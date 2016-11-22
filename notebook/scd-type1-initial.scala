
object Cells {
  import org.apache.spark._
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.rdd._
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.SparkSession

  /* ... new cell ... */

  case class Transaction(ind: String, name: String, city: String,prov: String, account_id:String,  amount:BigDecimal)
  case class Merchant(name: String, city:String, prov:String)
  case class MerchantDim(id:Long, name: String, city:String, prov:String)

  val dataDir ="/opt/data"

  /* ... new cell ... */

  val warehouseLocation = "/opt/data/spark-warehouse"
  val spark = SparkSession
     .builder()
     .appName("Dim Build")
     .config("spark.sql.warehouse.dir", warehouseLocation)
     .enableHiveSupport()
     .getOrCreate()

  /* ... new cell ... */

  val parse = (l:String) => (l.substring(0, 1).trim(), l.substring(49, 73).trim(), l.substring(74,86).trim(),  l.substring(87,90).trim(), l.substring(117,128).trim(), l.substring(90,103).trim())
  val parseRec = (l:String) => (l.substring(0, 1), l.substring(0, 230))

  /* ... new cell ... */

   val tdtxns = spark.read.textFile(dataDir)
             .map (parseRec)
             .filter(x=> x._1=="D")
             .map({case(s1,s2) => s2.substring(0, 130)})
             .map(parse)
             .map( {case (ind, name, city, prov, account_id, amount) => Transaction(ind, name, city, prov, account_id, BigDecimal(amount))} )
             .toDF()

  /* ... new cell ...
     group by name,city, prov
  */

  tdtxns.createOrReplaceTempView("a")
  val results = spark.sql("select distinct name, city, prov from a")
  results.show()
  val list = results.rdd.map(r=>Merchant(r(0).asInstanceOf[String],r(1).asInstanceOf[String],r(2).asInstanceOf[String])).collect()

  /* ... new cell ...
     assign key to each record
  */

  sparkContext.getConf.toDebugString
  val seed = 1
  val items = sparkContext.parallelize(list)
  val items_and_ids = items.zipWithIndex()
  val items_and_ids_mapped = items_and_ids.map(x => (x._2+seed, x._1))
  val dim = items_and_ids_mapped.map( {case (k,v) => MerchantDim(k,v.name,v.city,v.prov)} ).toDF()


  /* ... new cell ... */

  dim.show()

  /* ... new cell ...
      load dim to parquet
  */

  dim.write.parquet("/opt/data/merchantDim.parquet")
}
