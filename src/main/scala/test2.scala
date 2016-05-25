/**
  * Created by SX_H on 2016/5/16.
  */
import org.apache.spark.{SparkConf, SparkContext}

object test2 {
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
    val sever_fullpath = "s3n://emojikeyboardlite/service_full/20160521/full-r-00055"
    val meta_dailypath = "s3n://emojikeyboardlite/meta/20160521/metatab-r-00015"

    val sever_fulldata = percent_language(sever_fullpath, sc)
    //val meta_dailydata = compare_nation(meta_dailypath, sc)


    val path1 = "hdfs:///sx/percent_country/"
    val path2 = "hdfs:///sx/percent_lg/"
    HDFS.removeFile(path1)
    HDFS.removeFile(path2)

    sever_fulldata._1.repartition(1).saveAsTextFile(path1)
    sever_fulldata._1.repartition(1).saveAsTextFile(path2)
    //meta_dailydata.repartition(1).saveAsTextFile("hdfs:///sx/word2/")
  }

  def percent_language(path: String, sc: SparkContext) = {


    val ip2country = loadIP2COUNTRY(sc)
    val ip2Lc = sc.broadcast(ip2country)

    println("sx"+path)
    val user = sc.textFile(path)
      .map { x =>
        val item = x.split("\t")
        val deviceuid = item(0)
        val nation = item(6).toUpperCase
        val lg = item(10)
        val ip = get_ip(item(15))

        val ipL = ip2Long(ip)


        val iptable = ip2Lc.value

        val country = findCountry(iptable, ipLong = ipL)
//        (deviceuid, nation, country)
        (deviceuid,nation,lg,country)
      }
      .cache()

    val total_user = user.map{ case (duid,nation,lg,country) =>
      (duid)
    }.distinct().count().toFloat

      val country_user = user.map{
        case (duid,nation,lg,country) =>
          (duid,country)
      }.distinct()
        .map { case (duid, country) =>
          (country,1)
        }.reduceByKey(_+_)
      .map{x =>
        val result = x._2.toFloat / total_user
        (x._1,result)
      }.sortByKey().sortBy(_._2)

      val lg_user = user.map{
        case (duid,nation,lg,country) =>
          (duid,lg)
      }.distinct()
      .map{ case (duid,lg) =>
        (lg,1)
      }.reduceByKey(_+_)
      .map{x =>
       val jieguo = x._2.toFloat / total_user
        (x._1,jieguo)
      }.collect().sortBy(_._2)


    user.unpersist()




//      .filter { case (deviceuid, nation, country) =>
//        if (nation == country ) {
//          true
//        } else {
//          false
//        }
//      }
    (country_user,lg_user)
  }

  def ip2Long(ipAddress: String) = {
    var ipLong = 0L
    val ip = ipAddress.split("\\.")
    if (ip.length == 4) {

      val position1 = ipAddress.indexOf(".");
      val position2 = ipAddress.indexOf(".", position1 + 1);
      val position3 = ipAddress.indexOf(".", position2 + 1);

      //将每个.之间的字符串转换成整型
      val ip0 = ipAddress.substring(0, position1).toLong;
      val ip1 = ipAddress.substring(position1 + 1, position2).toLong;
      val ip2 = ipAddress.substring(position2 + 1, position3).toLong;
      val ip3 = ipAddress.substring(position3 + 1).toLong;
      ipLong = (ip0 << 24) + (ip1 << 16) + (ip2 << 8) + ip3
    }
    ipLong
  }

  def get_ip(str: String) = {
    val item = str.split("&")
    var ip = ""
    for (k <- item) {
      val v = k.split("=")
      if (v(0) == "ip") {
        ip = v(1)
      }
    }
    ip
  }

  def findCountry(ip2country: Array[(String, Long, Long)], ipLong: Long) = {
    //find the right country
    var right_country = ""
    var flag = false
    var start_index0 = 0
    var end_index0 = ip2country.length.toInt - 1
    var search_index0 = (end_index0 + start_index0) / 2
    while ((!flag) && (end_index0 - start_index0) >= 0) {
      val item = (ip2country) (search_index0) //(String, Long, Long) state, startip, endip
      val anchor_startip = item._2
      val anchor_endip = item._3
      val delta_left = ipLong - anchor_startip
      val delta_right = ipLong - anchor_endip

      //on
      if (delta_left == 0 || delta_right == 0) {
        right_country = item._1
        flag = true
      }


      //between
      else if (delta_left > 0 && delta_right < 0) {
        right_country = item._1
        flag = true
      }


      //left
      else if (delta_left > 0 && delta_right > 0) {
        start_index0 = search_index0 + 1
        search_index0 = (end_index0 + start_index0) / 2
      }

      //right
      else
      //          if(delta_left < 0 && delta_right <0)
      {
        end_index0 = search_index0 - 1
        search_index0 = (end_index0 + start_index0) / 2
      }
    }

    right_country.toUpperCase()

  }

  def loadIP2COUNTRY(sc: SparkContext) = {

    val tmp = sc.textFile("s3n://xinmei-dataanalysis/ip-country.ref")
      .map { x =>
        val a = x.split("\t")
        val ip = a(0).split("\\/")(0).split("\\.").map(_.toLong)
        val vali = a(0).split("\\/")(1).toLong
        val start = (ip(0) << 24) + (ip(1) << 16) + (ip(2) << 8) + ip(3)
        val end = start ^ ~(-1L << (32L - vali))
        val nation = a(1)
        (start, (end, nation))
      }
      .filter(_._2._2.length() > 0)
      //      .filter{x => countryMap.contains(x._2._2.toUpperCase())} //added by Gao Yuan. because of Tony Ma
      .sortBy(_._1, true, 1)
      .map {
        case (start, (end, nation)) =>
          (nation, start, end)
      }
      .collect()

    tmp
  }
}

