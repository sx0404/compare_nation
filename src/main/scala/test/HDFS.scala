package test
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

/**
 * @author wkd
 */
object HDFS {
  private val conf = new Configuration()

  // conf.addResource(new Path("/usr/hadoop/hadoop-2.2.0/etc/hadoop/core-site.xml"));
  // conf.addResource(new Path("/usr/hadoop/hadoop-2.2.0/etc/hadoop/hdfs-site.xml"));

  conf.addResource(new Path("/home/pubsrv/hadoop-2.7.1/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/pubsrv/hadoop-2.7.1/etc/hadoop/hdfs-site.xml"))

  private val HDFS = FileSystem.get(conf);

  def removeFile(filename: String): Boolean = {
    HDFS.delete(new Path(filename), true)
  }
}