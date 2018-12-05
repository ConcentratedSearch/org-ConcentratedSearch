import java.time.format.DateTimeFormatter
import org.apache.commons.lang.time.DateUtils
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.spark.sql.functions.col
import com.lucidworks.spark.util.SolrSupport
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import java.io.FileWriter
import java.io.File
import spark.implicits._
import scala.util.Try
 
val zkhost = "192.168.3.X.XX:9983/lwfusion/3.1.3/solr"

val collectionId = "raw_signals"  // raw signal

var no_Of_Searchterms = 20000

var clickOpts = Map("zkhost" -> zkhost, "collection" -> collectionId, "query" -> "type_s:ProductView AND timestamp_tdt:[NOW/DAY-30DAY TO NOW/DAY]", 

"fields" -> "count_i,params.searchTerm_s,params.site_s")


var impressionclickOpts = Map("zkhost" -> zkhost, "collection" -> collectionId, "query" -> "type_s:ProductExposure AND timestamp_tdt:[NOW/DAY-30DAY TO NOW/DAY]", 

"fields" -> "params.searchTerm_s, params.duid_s, params.site_s, timestamp_tdt")


var TotalclickOpts = Map("zkhost" -> zkhost, "collection" -> collectionId, "query" -> "type_s:ProductView AND timestamp_tdt:[NOW/DAY-30DAY TO NOW/DAY]", 

"fields" -> "params.searchTerm_s,params.site_s,params.duid_s,timestamp_tdt")



def checkForPrice( ) : String => String = {
    (review: String) => {
      if (Try(review.toDouble).isSuccess) {
        null
      }
      else {
        review
      }
    }
  }

val isNumberUDF = udf( checkForPrice )


def checkForString( ) : String => Double = {
    (review: String) => {
      if (Try(review.toDouble).isSuccess) {
        review.toDouble
      }
      else {
        1
      }
    }
  }

val checkForStringUDF = udf( checkForString )

val ProductViews = spark.read.format("solr").options(clickOpts).load.toDF("PV_Count","Search_Term","concept").withColumn("Search_Term", isNumberUDF( col( "Search_Term") ) ).filter($"Search_Term".isNotNull).filter($"Search_Term" !== "non-search").groupBy("concept","Search_Term").agg(sum($"PV_Count").as("PV_Count")).filter($"PV_Count" < 50).sort($"PV_Count".desc).limit(no_Of_Searchterms).select("Search_Term","concept","PV_Count")

ProductViews.createOrReplaceTempView("ProductViews")

val TotalProductViews = spark.read.format("solr").options(TotalclickOpts).load.toDF("Search_Term","concept","Session_ID","timestamp").withColumn("Search_Term", isNumberUDF( col( "Search_Term") ) ).filter($"Search_Term".isNotNull).filter($"Search_Term" !== "non-search")

TotalProductViews.createOrReplaceTempView("TotalProductViews")

val Join_PV_Total_PV = spark.sql("SELECT * FROM ProductViews LEFT JOIN TotalProductViews ON ProductViews.Search_Term = TotalProductViews.Search_Term AND ProductViews.concept = TotalProductViews.concept").select("TotalProductViews.Search_Term","TotalProductViews.concept","Session_ID","timestamp")

Join_PV_Total_PV.createOrReplaceTempView("Join_PV_Total_PV")

val impressions = spark.read.format("solr").options(impressionclickOpts).load.toDF("Consequitive_Search_Term","Session_ID","concept","timestamp_imp").filter($"Consequitive_Search_Term".isNotNull).filter($"Consequitive_Search_Term" !== "non-search").withColumn( "Consequitive_Search_Term", isNumberUDF( col( "Consequitive_Search_Term") ) ).filter($"Consequitive_Search_Term".isNotNull).filter($"concept" === "BBB")

impressions.createOrReplaceTempView("impressions")

val match_impressions_productviews = spark.sql("SELECT * FROM Join_PV_Total_PV LEFT JOIN impressions ON Join_PV_Total_PV.Session_ID = impressions.Session_ID AND Join_PV_Total_PV.concept = impressions.concept").select("timestamp_imp","timestamp","Join_PV_Total_PV.concept","Join_PV_Total_PV.Session_ID","Search_Term","Consequitive_Search_Term").filter($"concept" === "BBB").filter($"Consequitive_Search_Term".isNotNull)

val diff_secs_col = col("timestamp_imp").cast("long") - col("timestamp").cast("long")

val inter_match = match_impressions_productviews.withColumn( "diff_secs", diff_secs_col ).filter($"diff_secs">=0)

val w1 = Window.partitionBy($"Session_ID").orderBy($"diff_secs".desc)

val summary_term_set = inter_match.withColumn("counts",lit(1.0)).select("concept","Search_Term","Consequitive_Search_Term","counts").groupBy("concept","Search_Term","Consequitive_Search_Term").agg(sum($"counts").as("counts")).filter($"Consequitive_Search_Term" !== "non-search").filter($"Search_Term" !== $"Consequitive_Search_Term").withColumn("Consequitive_Search_Term", isNumberUDF( col( "Consequitive_Search_Term") ) ).withColumn("counts", checkForStringUDF( col( "counts") ) ).filter($"counts" > 1).filter($"Consequitive_Search_Term".isNotNull).select("concept","Search_Term","Consequitive_Search_Term","counts")

val temp_term_set = summary_term_set.select("concept","Search_Term","counts").groupBy("concept","Search_Term").agg(sum($"counts").as("Total_counts")).withColumn("Total_counts", checkForStringUDF( col( "Total_counts") ) ).select("concept","Search_Term","Total_counts")

temp_term_set.createOrReplaceTempView("temp_term_set")

summary_term_set.createOrReplaceTempView("summary_term_set")

val ST_CST_temp = spark.sql("SELECT * FROM summary_term_set LEFT JOIN temp_term_set ON summary_term_set.Search_Term = temp_term_set.Search_Term AND summary_term_set.concept = temp_term_set.concept").select("summary_term_set.concept","summary_term_set.Search_Term","Consequitive_Search_Term","counts","Total_counts")

def takePercentage( ) : (Double,Double) => Double = {
    (counts: Double, Total_counts:Double) => {
      (counts / Total_counts) * 100
    }
  }

val takePercentageUDF = udf( takePercentage )

val ST_CST = ST_CST_temp.filter($"concept" === "BBB").sort($"Search_Term".asc,$"counts".desc).withColumn("Percentage", takePercentageUDF( col( "counts"), col("Total_counts") ) ).filter($"Search_Term" !== $"Consequitive_Search_Term").filter($"Percentage" > 10).filter($"Percentage" !== 100).filter($"counts" > 40).limit(no_Of_Searchterms).select("concept","Search_Term","Consequitive_Search_Term","counts","Total_counts","Percentage")

ST_CST.coalesce(1).write.format("csv").option("header", "true").save("/u01/test/ST_CST_12_03/")
