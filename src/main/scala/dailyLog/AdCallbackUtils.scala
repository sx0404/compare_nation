package  dailyLog

import java.sql.DriverManager

import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer


object AdCallbackUtils {

  def getRdd(timeInterval: String, oid_list: Array[String], sc: SparkContext) = {

    val conn = DriverManager.getConnection("jdbc:mysql://172.31.12.234/koala",
      "mosh", "123456")

    if (!conn.isClosed)
      println("\tSucceeded connecting to the Database!\n")

    val stmt = conn.createStatement()

    val sql = "select app_key, sdk_ver,oid,country, payout from new_ad_callback where create_date like '" + timeInterval + "%'"

    val rs = stmt.executeQuery(sql)

    val callback_count = new ArrayBuffer[(String, String, String, String, Float)]

    while (rs.next()) {
      var app_key = rs.getString("app_key")
      var sdk_ver = rs.getString("sdk_ver")
      var oid = rs.getString("oid")
      var country = rs.getString("country")
      val payout = rs.getString("payout").toFloat

      if (app_key == "")
        app_key = "unknown"

      if (sdk_ver == "")
        sdk_ver = "unknown"

      if ((oid == "") || (!oid_list.contains(oid)))
        oid = "unknown"

      if (country == "")
        country = "unknown"

      val app_name = AppUtils.getProductByAppKey(app_key)

      val item = (app_name, sdk_ver, oid, country, payout)
      callback_count += item
    }

    val callback_count_rdd = sc.parallelize(callback_count)
      .map { case (app_key, sdk_ver, oid, country, payout) => (app_key + "\t" + sdk_ver + "\t" + oid + "\t" + country, 1) }
      .reduceByKey(_ + _)

    val callback_revenue_rdd = sc.parallelize(callback_count)
      .map { case (app_key, sdk_ver, oid, country, payout) => (app_key + "\t" + sdk_ver + "\t" + oid + "\t" + country, payout) }
      .reduceByKey(_ + _)


    val callback_rdd = callback_count_rdd.join(callback_revenue_rdd)

    callback_rdd

  }
}
