/**
  * Created by SX_H on 2016/5/16.
  */
import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

object test4 {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration
    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)
    hadoopConf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)

    val log_fullpath = "hdfs:///sx/index.log.17"
    val sever_fulldata = tongji(log_fullpath, sc)
    //val meta_dailydata = compare_nation(meta_dailypath, sc)

    //meta_dailydata.repartition(1).saveAsTextFile("hdfs:///sx/word2/")
  }

  def tongji(path: String, sc: SparkContext) = {
    val data = sc.textFile(path)
        .cache()
    val data_total = data.count().toFloat
    val data_chuli = data.map{
      x =>
        val item = parse_log(x)
        item
    }
      .cache()
    val gaid_percent = data_chuli.filter{case (gaid,aid) =>
      if(gaid == "") false else true
    }.count().toFloat / data_total
    val aid_percent = data_chuli.filter{case (gaid,aid) =>
      if(aid == "") false else true
    }.count().toFloat / data_total
    println("sx "+"gaid_percent="+gaid_percent+"aid_percent:"+aid_percent)
  }

 def parse_log(log:String)={
   val name = log.split("&")
   var gaid,aid = ""

   for (item <- name) {
     if (item.contains("gaid=")) {
       val haha = item.split("=")
       if (haha.length == 2){
       gaid = haha(1)
       }
     }else if (item.contains("aid=")) {
       val haha = item.split("=")
       if (haha.length == 2){
         aid = haha(1)
       }
     }else {}
   }
   (gaid,aid)
 }
}

