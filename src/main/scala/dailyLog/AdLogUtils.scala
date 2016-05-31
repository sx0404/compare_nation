package  dailyLog
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/**
 * Created by gqlxj1987 on 3/25/16.
 */
object AdLogUtils {

  def filter_ad_log(sc: SparkContext, timeInterval: String, ip2nation: Map[String, String], oid_list: Array[String]) = {

    //ad log part
    val adLogPath = "s3n://xinmei-ad-log/ad/ad.log.skip*" + timeInterval
    println("gyy-log: pathHere - " + adLogPath)

    val effective_ttp = ArrayBuffer("AD_GETADRESOURCE=true", "AD_GETADRESOURCE=false", "show", "install", "click", "show_image")

    //data format: (app_key, oid, getADResource/show/click/install)
    val log = sc.textFile(adLogPath)
      .filter { x => x.contains("\"ttp\":") }
      .filter { x => {
          val item = x.split("\t")
          item.length == 6
        }
      }
      .distinct()
      .map { x =>

        val item = x.split("\t")
        val app_key = item(2)
        val ipInt = item(4).toLong
        var json_data = item(5)
        val index = json_data.indexOf("{")
        if (index != 0) {
          json_data = json_data.substring(index)
        }
        //convert int_ip to ip address
        val ip_address = (new StringBuilder()).append(((ipInt >> 24) & 0xff)).append('.')
          .append((ipInt >> 16) & 0xff).append('.').append(
            (ipInt >> 8) & 0xff).append('.').append((ipInt & 0xff))
          .toString

        //convert ip address to country
        val country = IpUtils.ipToCountry(ip_address, ip2nation)
        val appProduct = AppUtils.getProductByAppKey(app_key)
        (appProduct, json_data, country)
      }
      .filter { x =>
        try {
          val om = new ObjectMapper()
          val jn = om.readTree(x._2)
          true
        } catch {
          case _: Throwable => false
        }

      }
      .filter { x => x._2.contains("ttp") }
      .map {
        case (appProduct, json_data, country) =>

          val om = new ObjectMapper()

          val jn = om.readTree(json_data)

          val ttp = jn.get("ttp").textValue()

          var sdk_version = "unknown"
          var oid = "unknown"
          var user_see_not = ((0, 0), (0, 0))

          if (json_data.contains("\"extra\"")) {
            val extra_json = jn.get("extra")
            if (json_data.contains("\"sdk_version\":")) {
              sdk_version = extra_json.get("sdk_version").textValue()
            }

            if (json_data.contains("\"userhavesee\":\"false\"")) {
              if (json_data.contains("\"durationTime\":")) {
                val duration = extra_json.get("durationTime").textValue().toInt
                user_see_not = ((0, 0), (1, duration))
              }
            }

            if (json_data.contains("\"userhavesee\":\"true\"")) {
              if (json_data.contains("\"durationTime\":")) {
                val duration = extra_json.get("durationTime").textValue().toInt

                user_see_not = ((1, duration), (0, 0))
              }
            }
          }

          if (json_data.contains("\"oid\":")) {
            oid = jn.get("oid").textValue()

          }

          (appProduct, sdk_version, oid, country, ttp, user_see_not)

      }
      .filter { x => effective_ttp.contains(x._5) && oid_list.contains(x._3) }
      .map { case (appProduct, sdk_version, oid, country, ttp, user_see_not) =>
          var res = Array(0, 0, 0, 0, 0, 0, 0, 0, 0)

          if (ttp == "show_image") {
            res(5) = user_see_not._1._1
            res(6) = user_see_not._1._2
            res(7) = user_see_not._2._1
            res(8) = user_see_not._2._2
          }
          else {
            res(effective_ttp.indexOf(ttp)) = 1
          }

          (appProduct + "\t" + sdk_version + "\t" + oid + "\t" + country, res)

      }
      .reduceByKey {
        (a: Array[Int], b: Array[Int]) =>
          var res = Array(0, 0, 0, 0, 0, 0, 0, 0, 0)
          for (i <- 0 until 9)
            res(i) = a(i) + b(i)
          res
      }
      .map { x => (x._1, (x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8))) }

    log

  }
}
