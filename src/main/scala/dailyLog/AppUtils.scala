package  dailyLog
/**
 * Created by gqlxj1987 on 3/25/16.
 */
object AppUtils {

  val appkey2Product = Map("e2934742f9d3b8ef2b59806a041ab389" -> "ikeyboard",
    "34c0ab0089e7a42c8b5882e1af3d71f9" -> "lite",
    "df31bd097babc7cdc13625e8fbc20a1a" -> "hifont",
    "78472ddd7528bcacc15725a16aeec190" -> "kika",
    "4e5ab3a6d2140457e0423a28a094b1fd" -> "pro")

  def getProductByAppKey(app_key:String):String = {
    var appProduct = "unknown"
    if (appkey2Product.contains(app_key)) {
      appProduct = appkey2Product.get(app_key).get
    }
    appProduct
  }

}
