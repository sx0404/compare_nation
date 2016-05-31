package  dailyLog

package  dailyLog
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author xm002
 *         这个object是用来更新表格 koala/ad_log_daily的数据的。
 *         但这张表格现在还有两个字段没有更新， 是server_click, 和 server_show. 这两个字段是要累计impression_id和click_id的数据－》这部分数据暂时存在show-server上。
 */
object adLogParse {

  //using jdbcRDD instead

  def main(args: Array[String]) {

    val conf = new SparkConf()

    val speculationEnabled = conf.getBoolean("spark.speculation", defaultValue = false)
    conf.getBoolean("spark.hadoop.outputCommitCoordination.enabled", speculationEnabled)

    val sc = new SparkContext(conf)

    val hadoopConf = sc.hadoopConfiguration

    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    hadoopConf.set("fs.s3n.awsAccessKeyId", "AKIAJIVCMRHJ56IM7IVA")

    hadoopConf.set("fs.s3n.awsSecretAccessKey", "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")

    val hc = new HiveContext(sc)

    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -3)
    val time0 = new SimpleDateFormat("yyyyMMddHH").format(cal.getTime)
    val time1 = new SimpleDateFormat("yyyy-MM-dd HH").format(cal.getTime)
    val time2 = new SimpleDateFormat("yyyyMMdd HH").format(cal.getTime)

    // ip映射到country
    val ip2nation = IpUtils.load_ip_file(sc)

    println("gyy-log: " + time0)

    //nginx get request log

    val nginxlog = NginxLogUtils.filter_nginx_log(sc, time0, ip2nation)

    val oid_list = nginxlog.map { x =>
      val item = x._1.split("\t")
      item(2)
    }
      .distinct()
      .collect()

    val adlog = AdLogUtils.filter_ad_log(sc, time0, ip2nation, oid_list)

    //data from yanyu
    val goLog = RequestTimeUtils.go_request_time(time0, oid_list, sc)
    //data from callback
    val callbackLog = AdCallbackUtils.getRdd(time1, oid_list, sc)

    val serverLog = ServerLogUtils.getServerLog(time0, sc)

    val totalLog = adlog.fullOuterJoin(nginxlog).fullOuterJoin(goLog).fullOuterJoin(serverLog).fullOuterJoin(callbackLog)

      .map { case (x, (y, z)) => (x, y, z) }
      .map { case (x, y, z) =>

      var sdk_success_count = 0
      var sdk_fail_count = 0
      var show_count = 0
      var click_count = 0
      var install_count = 0

      var show_success_count = 0
      var show_success_avgtime = 0.0
      var show_fail_count = 0
      var show_fail_avgtime = 0.0


      var nginx_success_count = 0
      var nginx_fail_count = 0

      var go_max_time = 0.0
      var go_min_time = 0.0
      var go_avg_time = 0.0

      var callback_count = 0
      var callback_revunue = 0.0

      var server_show_count =0
      var server_show_user_count = 0
      var server_click_count = 0
      var server_click_user_count = 0

      y match {
        case Some(a) =>
          a._1 match {
            case Some(clientLog) =>
              clientLog._1 match {
                case Some(sdk) =>
                  sdk._1 match {
                    case Some(sdk_log) =>

                      sdk_success_count = sdk_log._1
                      sdk_fail_count = sdk_log._2
                      show_count = sdk_log._3
                      click_count = sdk_log._4
                      install_count = sdk_log._5
                      show_success_count = sdk_log._6

                      if (show_success_count > 0)
                        show_success_avgtime = sdk_log._7.toFloat / show_success_count

                      show_fail_count = sdk_log._8

                      if (show_fail_count > 0)
                        show_fail_avgtime = sdk_log._9.toFloat / show_fail_count


                    case None =>
                  }


                  sdk._2 match {
                    case Some(nginx_log) =>
                      nginx_success_count = nginx_log._1
                      nginx_fail_count = nginx_log._2
                    case None =>
                  }
                case None =>
              }
              clientLog._2 match {
                case Some(go_request) =>
                  go_max_time = go_request._1
                  go_min_time = go_request._2
                  go_avg_time = go_request._3
                case None =>
              }

            case None =>
          }

          a._2 match {
            case Some(serverLog) =>
              server_show_count = serverLog._1
              server_show_user_count = serverLog._2
              server_click_count = serverLog._3
              server_click_user_count = serverLog._4
            case None =>
          }
        case None =>
      }

      z match {
        case Some(callback) =>
          callback_count = callback._1
          callback_revunue = callback._2
        case None =>
      }

      (x, sdk_success_count, sdk_fail_count, show_count, click_count, install_count, show_success_count, show_success_avgtime, show_fail_count, show_fail_avgtime, nginx_success_count, nginx_fail_count, go_max_time, go_min_time, go_avg_time, callback_count, callback_revunue, server_show_count, server_show_user_count, server_click_count, server_click_user_count)

    }
      .collect()

    HiveUtils.insertToHive(hc, sc, time2, totalLog)

    sc.stop()

  }

}
