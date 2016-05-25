/**
  * Created by SX_H on 2016/5/16.
  */
import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

object test3 {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration
    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)
    hadoopConf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)

    //val path:String = "s3n://word.txt"
    //s3://emojikeyboardlite/service_full/20160521/
    //s3://emojikeyboardlite/meta/20160521/
    val log_fullpath = "s3n:/mei-ad-log/open_ad_tracking/tracking-success.log.2016052513"

    val sever_fulldata = jisuan(log_fullpath, sc)
    //val meta_dailydata = compare_nation(meta_dailypath, sc)

    val path = ("result/click_appkey.txt","result/click_sdk_version,txt","result/show_appkey.txt","result/show_sdk_version,txt")
      wirte_text(path._1,sever_fulldata._1)
    wirte_text(path._2,sever_fulldata._2)
    wirte_text(path._3,sever_fulldata._3)
    wirte_text(path._4,sever_fulldata._4)

    //meta_dailydata.repartition(1).saveAsTextFile("hdfs:///sx/word2/")
  }

  def wirte_text(path:String,key:Array[((String,String),Float)]) {
    val writer = new PrintWriter(new File(path))
    for (item <- key) {
      writer.write(item + "\n")
    }
    writer.close()
  }

  def jisuan(path: String, sc: SparkContext) = {
    val data = sc.textFile(path)
      .filter { x =>
        (x.contains("/click/") || x.contains("/show/"))
      }
      .map{x =>
        val log_result = fenxi_log(x)
        (log_result._1,log_result._2,log_result._3,log_result._4,log_result._5)
      }.cache()

   val click = data.filter{ case (leibie, oid, strategy_name, app_key, sdk_version) =>
      if (leibie == "click") true else false
   }.cache()
    val click_num = click.count().toFloat
    val click_app_key = click.map{case (leibie, oid, strategy_name, app_key, sdk_version) =>
      ((leibie,app_key),1)
    }.reduceByKey(_+_)
      .map{x =>
        val percent_appkey = x._1._2.toFloat / click_num
        (x._1,percent_appkey)
      }.collect().sortBy(_._2)
    val click_sdk_version = click.map{case (leibie, oid, strategy_name, app_key, sdk_version) =>
      ((leibie,sdk_version),1)
    }.reduceByKey(_+_)
      .map{x =>
        val percent_sdk_version = x._1._2.toFloat / click_num
        (x._1,percent_sdk_version)
      }.collect().sortBy(_._2)
    click.unpersist()

    val show = data.filter{ case (leibie, oid, strategy_name, app_key, sdk_version) =>
      if (leibie == "show") true else false
    }.cache()
    val show_num = show.count().toFloat
    val show_app_key = show.map{case (leibie, oid, strategy_name, app_key, sdk_version) =>
      ((leibie,app_key),1)
    }.reduceByKey(_+_)
      .map{x =>
        val percent_appkey = x._1._2.toFloat / click_num
        (x._1,percent_appkey)
      }.collect().sortBy((_._2))
    val show_sdk_version = show.map{case (leibie, oid, strategy_name, app_key, sdk_version) =>
      ((leibie,sdk_version),1)
    }.reduceByKey(_+_)
      .map{x =>
        val percent_sdk_version = x._1._2.toFloat / click_num
        (x._1,percent_sdk_version)
      }.collect().sortBy(_._2)
    show.unpersist()

    data.unpersist()



   //data.unpersist()




//      .filter { case (deviceuid, nation, country) =>
//        if (nation == country ) {
//          true
//        } else {
//          false
//        }
//      }
    (click_app_key,click_sdk_version,show_app_key,show_sdk_version)
  }

 def fenxi_log(log:String)={
   val name = log.split("&")
   var leibie,oid,strategy_name,app_key,sdk_version = ""
   if (log.contains("/click/")) {
     leibie = "click"
   }else if (log.contains("/show/")) {
     leibie = "show"
   }
   for (item <- name) {
     if (item.contains("app_key=")) {
       val haha = item.split("=")
       app_key = haha(1)
     }else if (item.contains("oid=")) {
       val haha = item.split("=")
       oid = haha(1)
     }else if (item.contains("strategy_name=")) {
       val haha = item.split("=")
       strategy_name = haha(1)
     }else if (item.contains("sdk_version=")) {
       val haha = item.split("=")
       sdk_version = haha(1)
     }else {}
   }
   (leibie,oid,strategy_name,app_key,sdk_version)
 }
}
