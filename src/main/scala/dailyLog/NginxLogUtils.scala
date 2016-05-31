package  dailyLog
import java.net.URLDecoder

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.SparkContext

/**
 * Created by gqlxj1987 on 3/25/16.
 */
object NginxLogUtils {

  def filter_nginx_log(sc: SparkContext, timeInterval: String, ip2nation: Map[String, String]) = {

    val nginxLogPath = "s3n://xinmei-ad-log/goapi_nginx/goapi_log.nginx*" + timeInterval
    println("gyy-log: nginxLogPath " + nginxLogPath)

    val nginxLog = sc.textFile(nginxLogPath)
      .filter { x => x.contains("/api/getADResource") && x.contains("\t") }
      .filter { x => (x.split("\t")).length > 10 }
      .map { x =>
      val item = x.split("\t")
      val ip = item(0)
      (item(4), item(5), ip)
    }
      .map { case (request, status, ip) =>
      try {
        (URLDecoder.decode(request, "UTF-8"), status, ip)
      } catch {
        case _: Throwable =>
          ("Error", status, ip)
      }
    }
      .filter { x => x._1 != "Error" }
      .map { x =>
      val decode_url = x._1

      //map ip to country:
      val country = IpUtils.ipToCountry(x._3, ip2nation)

      (decode_url, x._2, country)

    }
      .filter { x =>
      x._1.contains("req={") && x._1.contains("}")
    }
      .map { x =>
      val start_index = x._1.indexOf("{")
      val end_index = x._1.indexOf("}")

      val json = x._1.substring(start_index, end_index + 1)

      (json, x._2, x._3)
    }
      .filter { x =>
      try {
        val om = new ObjectMapper()
        val jn = om.readTree(x._1)
        true
      } catch {
        case _: Throwable => false
      }

    }
      .map { x =>

      val om = new ObjectMapper()
      val jn = om.readTree(x._1)

      var app_name = "unknown"


      if (x._1.contains("app_key")) {
        val app_key = jn.get("app_key").textValue()

        app_name = AppUtils.getProductByAppKey(app_key)
      }


      var obj_id = "unknown"
      if (x._1.contains("obj_id")) {
        obj_id = jn.get("obj_id").textValue()
        if (obj_id == "")
          obj_id = "unknown"
      }

      var sdk_version = "unknown"
      if (x._1.contains("sdk_version")) {
        sdk_version = jn.get("sdk_version").textValue()
        if (sdk_version == "")
          sdk_version = "unknown"
      }

      val returnCode = {
        if (x._2.equals("200"))
          (1, 0)
        // fail
        else
          (0, 1)
      }

      (app_name + "\t" + sdk_version + "\t" + obj_id + "\t" + x._3, returnCode)
    }
      .reduceByKey {
      (a: (Int, Int), b: (Int, Int)) =>
        (a._1 + b._1, a._2 + b._2)
    }

    nginxLog

  }
}
