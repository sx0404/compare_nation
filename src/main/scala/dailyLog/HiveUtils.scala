package  dailyLog
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

object HiveUtils {

  val apppMap = collection.immutable.HashMap("kika" -> "78472ddd7528bcacc15725a16aeec190",
    "pro" -> "4e5ab3a6d2140457e0423a28a094b1fd",
    "lite" -> "34c0ab0089e7a42c8b5882e1af3d71f9",
    "ikeyboard" -> "e2934742f9d3b8ef2b59806a041ab389",
    "hifont" -> "df31bd097babc7cdc13625e8fbc20a1a")

  def insertToHive(hc: HiveContext, sc: SparkContext, time1: String, totalLog: Array[(String, Int, Int, Int, Int, Int, Int, Double, Int, Double, Int, Int, Double, Double, Double, Int, Double, Int, Int, Int, Int)]) = {

    val time_item = time1.split(" ")

    val date1 = time_item(0).toInt
    val hour1 = time_item(1).toInt

    val resultList: ArrayBuffer[((String, String, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Double, String, Int, Int))] = new ArrayBuffer()
    for (log0 <- totalLog) {

      val item = log0._1.split("\t")
      val appproduct = item(0)

      // convert appProduct to app_key
      if (apppMap.contains(appproduct)) {
        val appKey = apppMap.get(appproduct).get

        val sdk_version = item(1)
        val oid = item(2)
        val country = item(3)

        val sdk_success_count = log0._2
        val sdk_fail_count = log0._3
        val show_count = log0._4
        val click_count = log0._5
        val install_count = log0._6

        val show_success_count = log0._7
        val show_success_avgtime = log0._8
        val show_fail_count = log0._9
        val show_fail_avgtime = log0._10

        val nginx_success_count = log0._11
        val nginx_fail_count = log0._12
        val go_max_time = log0._13
        val go_min_time = log0._14
        val go_avg_time = log0._15
        val callback_count = log0._16
        val callback_revunue = log0._17

        val server_show_count = log0._18
        val server_show_user_count = log0._19
        val server_click_count = log0._20
        val server_click_user_count = log0._21

        val outputTemp = (sdk_version, oid, country, sdk_success_count, sdk_fail_count,
          nginx_success_count, nginx_fail_count,
          show_count, show_success_count, show_fail_count,
          server_show_count, server_show_user_count, click_count, server_click_count, server_click_user_count,
          install_count, callback_count, callback_revunue, appKey, date1, hour1)

        resultList += outputTemp
      }
    }

    val fullPath = "hdfs:///gql/test/"

    HDFS.removeFile(fullPath)

    val rdd0 = sc.parallelize(resultList)

    rdd0.coalesce(1, true).saveAsTextFile(fullPath)

    println("saveToHdfs complete")

    //    hc.sql("CREATE EXTERNAL TABLE IF NOT EXISTS ad_log_daily (sdk_version STRING, oid STRING, country STRING,sdk_request_success INT,sdk_request_fail INT,nginx_request_success INT,nginx_request_fail INT,sdk_show INT,sdk_show_count_success INT,sdk_show_count_fail INT,server_show INT,server_show_user INT,sdk_click INT,server_click INT,server_click_user INT,sdk_install INT,callback_install INT,callback_revenue STRING,app_key STRING) PARTITIONED BY (cdate BIGINT,hour INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TextFile LOCATION '/default/ad_log_daily'")
    //
    //    hc.sql("LOAD DATA INPATH 'hdfs:///gql/test/' OVERWRITE INTO TABLE ad_log_daily PARTITION (cdate='" + date1 + "', hour='" + hour1 + "')")
    //
    //    hc.sql("insert into ad_log_daily select sdk_version, oid, country,sdk_request_success,sdk_request_fail,nginx_request_success,nginx_request_fail,sdk_show,sdk_show_count_success,sdk_show_count_fail,server_show,server_show_user,sdk_click,server_click,server_click_user,sdk_install,callback_install,callback_revenue,app_key from ad_log_daily")

    //    val cmd = "hive -e 'LOAD DATA INPATH \"hdfs:///gql/test/\" OVERWRITE INTO TABLE ad_log_daily PARTITION (cdate=\"" + date1 + "\", hour=\"" + hour1 + "\")'"

    val cmd = "sh /home/guiquanli/load_ad.sh " + date1 + " " + hour1

    println(cmd)

    cmd.!

    println("load data complete")
  }
}
