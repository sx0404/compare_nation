package  dailyLog
import java.sql.DriverManager

/**
 * Created by gqlxj1987 on 3/25/16.
 */
object DbUtils {

  def update_mysql(time1: String, totalLog: Array[(String, Int, Int, Int, Int, Int, Int, Double, Int, Double, Int, Int, Double, Double, Double, Int, Double)]) = {

    val conn = DriverManager.getConnection("jdbc:mysql://172.31.12.234/koala",
      "mosh", "123456")

    if (!conn.isClosed())
      println("\tSucceeded connecting to the Database!\n")

    val stmt = conn.createStatement()



    for (log0 <- totalLog) {
      val item = log0._1.split("\t")
      val appproduct = item(0)
      val sdk_version = item(1)
      val oid = item(2)
      val country = item(3)

      val sdk_success_count = log0._2
      val sdk_fail_count = log0._3
      val show_count = log0._4
      val click_count = log0._5
      val install_count = log0._6

      val show_success_count = log0._7
      val show_success_avgtime = log0._8
      val show_fail_count = log0._9
      val show_fail_avgtime = log0._10

      val nginx_success_count = log0._11
      val nginx_fail_count = log0._12
      val go_max_time = log0._13
      val go_min_time = log0._14
      val go_avg_time = log0._15
      val callback_count = log0._16
      val callback_revunue = log0._17

      val prep = conn.prepareStatement("INSERT INTO new_ad_log_daily (date," +
        "hour," +
        "app_product," +
        "sdk_version," +
        "oid," +
        "country," +
        "sdk_request_success," +
        "sdk_request_fail," +
        "sdk_show," +
        "sdk_click," +
        "sdk_install," +
        "sdk_show_count_success," +
        "sdk_show_time_success_avg," +
        "sdk_show_count_fail," +
        "sdk_show_time_fail_avg," +
        "nginx_request_success," +
        "nginx_request_fail," +
        "go_request_avgtime," +
        "go_request_maxtime," +
        "go_request_mintime," +
        "callback_install," +
        "callback_revenue) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update sdk_request_success = ?,sdk_request_fail = ?,sdk_show = ?,sdk_click = ?,sdk_install = ?,nginx_request_success=?,nginx_request_fail = ?,go_request_avgtime = ?,go_request_maxtime = ?,go_request_mintime = ?,callback_install = ?,callback_revenue = ?, sdk_show_count_success = ?,sdk_show_time_success_avg = ?,sdk_show_count_fail = ?,sdk_show_time_fail_avg = ?")

      val time_item = time1.split(" ")

      val date1 = time_item(0)
      val hour1 = time_item(1)


      prep.setString(1, date1)
      prep.setString(2, hour1)

      prep.setString(3, appproduct)
      prep.setString(4, sdk_version)

      prep.setString(5, oid)
      prep.setString(6, country)

      prep.setInt(7, sdk_success_count)
      prep.setInt(8, sdk_fail_count)
      prep.setInt(9, show_count)
      prep.setInt(10, click_count)
      prep.setInt(11, install_count)

      prep.setInt(12, show_success_count)
      prep.setDouble(13, show_success_avgtime)
      prep.setInt(14, show_fail_count)
      prep.setDouble(15, show_fail_avgtime)


      prep.setInt(16, nginx_success_count)
      prep.setInt(17, nginx_fail_count)
      prep.setDouble(18, go_avg_time)
      prep.setDouble(19, go_max_time)
      prep.setDouble(20, go_min_time)

      prep.setInt(21, callback_count)
      prep.setDouble(22, callback_revunue)


      prep.setInt(23, sdk_success_count)
      prep.setInt(24, sdk_fail_count)
      prep.setInt(25, show_count)
      prep.setInt(26, click_count)
      prep.setInt(27, install_count)
      prep.setInt(28, nginx_success_count)
      prep.setInt(29, nginx_fail_count)
      prep.setDouble(30, go_avg_time)
      prep.setDouble(31, go_max_time)
      prep.setDouble(32, go_min_time)

      prep.setInt(33, callback_count)
      prep.setDouble(34, callback_revunue)

      prep.setInt(35, show_success_count)
      prep.setDouble(36, show_success_avgtime)
      prep.setInt(37, show_fail_count)
      prep.setDouble(38, show_fail_avgtime)



      try {
        prep.executeUpdate

      }
      catch {
        case e: Throwable => println("gyy-error info " + e + "\n" + log0)
      }

    }

    conn.close()

    println("gyy")

  }

}
