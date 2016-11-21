
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration

class HBaseConfig(defaults: Configuration) extends Serializable {
  def get = HBaseConfiguration.create(defaults)
}

object HBaseConfig {
  def apply(conf: Configuration): HBaseConfig = new HBaseConfig(conf)

  def apply(options: (String, String)*): HBaseConfig = {
    val conf = HBaseConfiguration.create
    for ((key, value) <- options) { conf.set(key, value) }
    apply(conf)
  }

/*  def apply(conf: { def quorum: String ;def port: String; def znode: String }):
  HBaseConfig = apply(
     "hbase.zookeeper.quorum" -> conf.quorum
    ,"hbase.zookeeper.property.clientPort" -> conf.port
    ,"hzookeeper.znode.parent" -> conf.znode)*/
}
