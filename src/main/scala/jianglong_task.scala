/**
  * Created by SX_H on 2016/5/16.
  */
import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object jianglong_task {
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
    val sql_str = "SELECT ad_id,day,SUM(clicks),SUM(impressions),SUM(installs) FROM `ad` where ad_id = 2854005 GROUP BY `day`"

    jdbcDF.registerTempTable("ad")
    val jdbc = jdbcDF
      .sqlContext.sql(sql_str)
      .map{x =>
        (x(0).toString, x(1).toString, x(2).toString, x(3).toString,x(4).toString)
      }
      .collect()
    jdbc
  }

  def write_csv(result:Array[(String,String,String,String,String)]): Unit ={
    val calendar =  Calendar.getInstance()
    val date = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime())
    val path = "mail_jianglong/" + date + ".csv"
    val wr = new OutputStreamWriter(new FileOutputStream(new File(path)), "UTF8")
    val writer = new BufferedWriter(wr)
    writer.write("ad_id" + "," + "day" + "," + "sum(clicks)" + "," + "sum(impretions)" + "," + "sum(installs)" + "\r\n")
    for (item <- result) {
      writer.write(item._1 + "," + item._2 + "," + item._3 + "," + item._4 + ","  + item._5 + "\r\n");
    }
  }
}

