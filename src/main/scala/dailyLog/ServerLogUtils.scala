package  dailyLog
import java.io.File

import org.apache.spark.SparkContext

import scala.io.Source

object ServerLogUtils {

  val apppProductMap = collection.immutable.HashMap("78472ddd7528bcacc15725a16aeec190" -> "kika",
    "4e5ab3a6d2140457e0423a28a094b1fd" -> "pro",
    "34c0ab0089e7a42c8b5882e1af3d71f9" -> "lite",
    "e2934742f9d3b8ef2b59806a041ab389" -> "ikeyboard",
    "df31bd097babc7cdc13625e8fbc20a1a" -> "hifont")

  val showFilePath = "/home/pubsrv/logs/spark/hive/show/"

  val clickFilePath = "/home/pubsrv/logs/spark/hive/click/"

  var serverMap: Map[(String, String, String, String), (Int, Int, Set[String], Set[String])] = Map()

  def init() = {
    serverMap = Map()
  }

  def filter_show_log(timeInterval: String) = {
    val res0: File = new File(showFilePath + "showTmp_log_daily.txt" + "." + timeInterval)

    println("show-log:" + res0.getName)

    for (line <- Source.fromFile(res0).getLines()) {
      val showList: List[String] = line.split(" ").toList

      if (showList.nonEmpty && showList.size == 4) {
        val country = showList.head.replace("country", "")
        var oid = showList.apply(1).replace("oid", "")
        var appKey = showList.apply(2).replace("app_key", "")
        val sdkVersion = showList.apply(3).replace("sdk_ver", "")

        if (oid == "") {
          oid = "unknown"
        }

        if (appKey == "") {
          appKey = "unknown"
        }

        val key = (appKey, sdkVersion, oid, country.toUpperCase)

        if (serverMap.contains(key)) {
          var value = serverMap.get(key).get
          val showCount = value._1 + 1
          val showDuids = value._3 + showList.apply(4)

          val newValue = (showCount, value._2, showDuids, value._4)

          serverMap.updated(key, newValue)
        } else {
          val showDuids: Set[String] = Set() + showList.apply(4)

          val newValue = (1, 0, showDuids, Set[String]())

          serverMap += (key -> newValue)
        }
      }
    }
  }

  def filter_click_log(timeInterval: String) = {
    val res1: File = new File(clickFilePath + "clickTmp_log_daily.txt" + "." + timeInterval)

    println("click-log:" + res1.getName)

    for (line1 <- Source.fromFile(res1).getLines()) {
      val clickList: List[String] = line1.split(" ").toList

      if (clickList.nonEmpty && clickList.size == 4) {
        val country = clickList.head.replace("country", "")
        var oid = clickList.apply(1).replace("oid", "")
        var appKey = clickList.apply(2).replace("app_key", "")
        val sdkVersion = clickList.apply(3).replace("sdk_ver", "")

        if (oid == "") {
          oid = "unknown"
        }

        if (appKey == "") {
          appKey = "unknown"
        }

        var key = (appKey, sdkVersion, oid, country.toUpperCase)

        if (serverMap.contains(key)) {
          var value = serverMap.get(key).get
          val clickCount = value._2 + 1
          val clickDuids = value._4 + clickList.apply(4)

          val newValue = (value._1, clickCount, value._3, clickDuids)

          serverMap.updated(key, newValue)
        } else {
          val clickDuids: Set[String] = Set() + clickList.apply(4)

          val newValue = (0, 1, Set[String](), clickDuids)

          serverMap += (key -> newValue)
        }
      }
    }
  }

  def getServerLog(timeInterval: String, sc: SparkContext) = {
    init()
    filter_show_log(timeInterval)
    println("show-log filter complete")
    filter_click_log(timeInterval)
    println("click-log filter complete")

    val serverLog = ServerLogUtils.serverMap.map {
      x =>
        val key = x._1
        val value = x._2

        val appName = apppProductMap.get(key._1).get

        val showCount = value._1
        val clickCount = value._2
        val serverShowUser = value._3.size
        val serverClickUser = value._4.size

        (appName + "\t" + key._2 + "\t" + key._3 + "\t" + key._4, (showCount, clickCount, serverShowUser, serverClickUser))

    }
    sc.parallelize(serverLog.map { case (k, v) => (k, v) }.toSeq)
  }


}
