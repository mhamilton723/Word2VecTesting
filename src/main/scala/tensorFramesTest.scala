/*
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.tensorframes.{dsl => tf}
import org.tensorframes.dsl.Implicits._
/**
  * Created by hdiuser on 9/7/16.
  */
object tensorFramesTest extends App{


  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("My App")
    .config("driver-memory", "10g")
    .config("spark.yarn.executor.memoryOverhead", "10g")
    .config("spark.executor.memory", "10g")
    .config("spark.driver.memory", "10g")
    .config("spark.driver.maxResultSize", "10g")
    .config("spark.sql.warehouse.dir", "/home/hdiuser/IdeaProjects/SparkWord2Vec/my/")
    .config("spark.rpc.message.maxSize", "246")
    .getOrCreate()
  LogManager.getRootLogger.setLevel(Level.WARN)
  import spark.implicits._

  spark.sparkContext.addJar("/home/hdiuser/IdeaProjects/SparkWord2Vec/target/scala-2.11/tensorframes-0.2.3-s_2.10.jar")
  val df = spark.createDataFrame(Seq(1.0->1.1, 2.0->2.2)).toDF("a", "b")

  // As in Python, scoping is recommended to prevent name collisions.
  val df2 = tf.withGraph {
    val a = df.block("a")
    // Unlike python, the scala syntax is more flexible:
    val out = a + 3.0 named "out"
    // The 'mapBlocks' method is added using implicits to dataframes.
    df.mapBlocks(out).select("a", "out")
  }

  // The transform is all lazy at this point, let's execute it with collect:
  df2.show()

}
*/