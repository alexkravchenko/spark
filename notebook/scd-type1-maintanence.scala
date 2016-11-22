
object Cells {
  import org.apache.spark._
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.rdd._
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.SparkSession

  val parse = (l:String) => (l.substring(0, 1).trim(), l.substring(49, 73).trim(), l.substring(74,86).trim(),  l.substring(87,90).trim(), l.substring(117,128).trim(), l.substring(90,103).trim())
  val parseRec = (l:String) => (l.substring(0, 1), l.substring(0, 230))

  case class Transaction(ind: String, name: String, city: String,prov: String, account_id:String,  amount:BigDecimal)
  case class Merchant(name: String, city:String, prov:String)
  case class MerchantDim(id:Long, name: String, city:String, prov:String)
  val dataDir ="/opt/data/td"

  /* ... new cell ... */



  val warehouseLocation = "/opt/data/spark-warehouse"
  val spark = SparkSession
     .builder()
     .appName("Dim Build")
     .config("spark.sql.warehouse.dir", warehouseLocation)
     .enableHiveSupport()
     .getOrCreate()

  /* ... new cell ...
     read new files
  */

   val newtxns = spark.read.textFile(dataDir)
             .map (parseRec)
             .filter(x=> x._1=="D")
             .map({case(s1,s2) => s2.substring(0, 130)})
             .map(parse)
             .map( {case (ind, name, city, prov, account_id, amount) => TDTransaction(ind, name, city, prov, account_id, BigDecimal(amount))} )
             .toDF()

  /* ... new cell ...
     group new records
  */

  newtxns.createOrReplaceTempView("a")
  spark.sql("select count(*) from a").show()
  val results = spark.sql("select distinct name, city, prov from a")
  val list = results.rdd.map(r=>Merchant(r(0).asInstanceOf[String],r(1).asInstanceOf[String],r(2).asInstanceOf[String])).toDF()

  /* ... new cell ...
    read dimension from parquet
  */

  val merchantDim = spark.read.parquet("/opt/data/merchantDim.parquet")

  /* ... new cell ...
     find new records for dimension
  */

  merchantDim.createOrReplaceTempView("b")
  list.createOrReplaceTempView("c")

  val newRecords = spark.sql("select c.name, c.city, c.prov from c LEFT JOIN b ON c.name=b.name and c.city=b.city and  c.prov=b.prov where b.id is null")

  /* ... new cell ...
        group records for append
  */

  val appendToDim = newRecords.rdd.map(r=>Merchant(r(0).asInstanceOf[String],r(1).asInstanceOf[String],r(2).asInstanceOf[String])).collect()
  appendToDim.size

  /* ... new cell ...
     get last key and build append
  */

  var start = spark.sql("select max(id) from b").map(r=>r(0).asInstanceOf[Long]).first()
  val seed = 1 + start
  val items = sparkContext.parallelize(appendToDim)
  val items_and_ids = items.zipWithIndex()
  val items_and_ids_mapped = items_and_ids.map(x => (x._2+seed, x._1))
  val dim = items_and_ids_mapped.map( {case (k,v) => MerchantDim(k,v.name,v.city,v.prov)} ).toDF()

  /* ... new cell ...
     debug
  */

  dim.show()

  /* ... new cell ...
     append to existing dimension
  */

  dim.write.mode("append").parquet("/opt/data/merchantDim.parquet")

  /* ... new cell ...
     debug
  */

  val merchantDim1 = spark.read.parquet("/opt/data/merchantDim.parquet")
  merchantDim1.createOrReplaceTempView("y")
  spark.sql("select count(*) from y").show()
}
