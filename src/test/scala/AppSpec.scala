import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by markmo on 10/01/2016.
  */
class AppSpec extends UnitSpec {

  // Setup
  val conf = ConfigFactory.load()
  val sparkConf = new SparkConf().setAppName(conf.getString("app.name")).setMaster(conf.getString("spark.master"))
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

}
