/**
  * Created by SX_H on 2016/5/16.
  */
import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object guanggao {

  val appkey2Product = Map("e2934742f9d3b8ef2b59806a041ab389" -> "ikeyboard",
    "34c0ab0089e7a42c8b5882e1af3d71f9" -> "lite",
    "df31bd097babc7cdc13625e8fbc20a1a" -> "hifont",
    "78472ddd7528bcacc15725a16aeec190" -> "kika",
    "4e5ab3a6d2140457e0423a28a094b1fd" -> "pro")

  val oid_guanggaowei = Map("kika_home" -> "kika_themerec_facebook_ad",
    "kika_emoji" -> "kika_keyboard_emoji_ucenter_icon_ads",
    "kika_settingicon" -> "kika_keyboard_menu_ad",
    "pro_home" -> "pro_themerec_facebook_ad",
    "pro_emoji" -> "pro_keyboard_emoji_ucenter_icon_ads",
    "setting-icon" -> "pro_keyboard_menu_ad",
    "ikey_tuijianye" -> "ikey_themerec_facebook_ad",
    "ikey_icon" -> "ikeyboard_keyboard_menu_ad",
    "lite_kaipinguanggao" -> "lite_splash_Interstitial_ad",
    "lite_tuijianye" -> "lite_themerec_facebook_ad",
    "lite_icon" -> "lite_keyboard_menu_ad"
  )
  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration
    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)
    val timeInterval = args(1)
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)
    hadoopConf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)

    val result_sum = get_sum(sc)
    write_csv(result_sum)

   // val shell = ""
  }

  def get_sum(sc:SparkContext)= {
    val sqlContext = new SQLContext(sc)
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://172.31.28.109:3306/koala_test?user=suxin&password=suxin123",
        "dbtable" -> "ad_data_hourly",
        "driver" -> "com.mysql.jdbc.Driver"
      )
    ).load()
    val sql_str = "SELECT day,oid,app_key,SUM(clicks),SUM(impressions),SUM(installs) FROM ad where app_key='e2934742f9d3b8ef2b59806a041ab389' or app_key = '34c0ab0089e7a42c8b5882e1af3d71f9' or app_key='78472ddd7528bcacc15725a16aeec190' or app_key='4e5ab3a6d2140457e0423a28a094b1fd' GROUP BY `day`,oid"

    jdbcDF.registerTempTable("ad")
    val jdbc = jdbcDF
      .sqlContext.sql(sql_str)
      .map{x =>
        (x(0).toString, x(1).toString, x(2).toString, x(3).toString,x(4).toString,x(5).toString)
      }
      .collect()
    jdbc
  }

  def write_csv(result:Array[(String,String,String,String,String,String)]): Unit ={
    val calendar =  Calendar.getInstance()
    val date = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime())
    val path = "mail_jianglong/" + date + ".csv"
    val wr = new OutputStreamWriter(new FileOutputStream(new File(path)), "UTF8")
    val writers = new BufferedWriter(wr)
    writers.write("day" + "," + "guangaowei" + "," +"app_name" + "," +"sum(clicks)" + "," + "sum(impretions)" + "," + "sum(installs)" + "\r\n")
    for (item <- result) {
      println("sx" + item)
      val guanggaowei = oid_guanggaowei(item._2)
      val app_name = appkey2Product(item._3)
      writers.write(item._1 + "," + guanggaowei + "," + app_name + "," + item._4 + "," + item._5 +"," + item._6 +"\r\n");
    }
    writers.close()
  }
}

