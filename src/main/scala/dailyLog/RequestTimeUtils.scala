package  dailyLog
import org.apache.spark.SparkContext

/**
 * Created by gqlxj1987 on 3/25/16.
 */
object RequestTimeUtils {

  def go_request_time(timeInterval: String, oid_list: Array[String], sc: SparkContext) = {

    //data from yanyu
    //      #go$
    val goLogPath = "s3n://xinmei-ad-log/goapi_server/goapi.go*." + timeInterval

    val go_log = sc.textFile(goLogPath)
      .filter { x => x.contains("#go$") && x.contains(" $go#") }
      .filter { x =>
      x.contains("sdkversion") && x.contains("country") && x.contains("appkey") && x.contains("getadresource") && x.contains("time")
    }
      .map { x =>
      x.replace("#go$ ", "").replace(" $go#", "")
    }
      .distinct()
      .filter { x =>
      val allstr = x.replaceAll("= ", "=")
      val item = allstr.split(" ")
      item.length >= 7
    }
      .map { x =>

      val allstr = x.replaceAll("= ", "=")
      val item = allstr.split(" ")
      val time = item(1).substring(5).toFloat
      val country = item(2).substring(8)
      val oid = item(3).substring(4)
      val appkey = item(4).substring(7)
      val sdkversion = item(6).substring(11)
      val app_name = AppUtils.getProductByAppKey(appkey)

      ((app_name, sdkversion, oid, country), time)
    }
      .groupByKey()
      .filter { x => oid_list.contains(x._1._3) }
      .map { case ((app_name, sdkversion, oid, country), time) =>

      var max_time = 0.0
      var min_time = 1000.0
      var avg_time = 0.0

      for (i <- time) {
        avg_time = avg_time + i
        if (max_time < i)
          max_time = i
        if (min_time > i) {
          min_time = i
        }
      }

      avg_time = avg_time / time.size

      (app_name + "\t" + sdkversion + "\t" + oid + "\t" + country, (max_time, min_time, avg_time))

    }

    go_log

  }

}
