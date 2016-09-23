import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkException
import org.apache.spark.ml.{Model, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._

/**
  * Created by marhamil on 8/17/2016.
  */
class SparkModuleSpec extends FlatSpec with Matchers with BeforeAndAfterAll{
  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("My App")
    .config("spark.sql.warehouse.dir", "file:///C:/users/marhamil/IdeaProjects/Spark2Word2Vec/my/")
    .getOrCreate()
  val sc = spark.sparkContext
  // For implicit conversions like converting RDDs to DataFrames

  LogManager.getRootLogger.setLevel(Level.WARN)
  // this is used to implicitly convert an RDD to a DataFrame.
  def extractDFCol[T](dataFrame: DataFrame,outputCol:String)={
    dataFrame.select(outputCol).collect().map(_.getAs[T](0))
  }

  def testParameterExceptions[T](parameters: List[T],
                                 fitFunction: T => Unit, inputCol:String="words"): Unit ={
    parameters.foreach{ p=>
      an [IllegalArgumentException] should be thrownBy {
        fitFunction(p)
      }
    }
  }

  def testNullException[T <: Transformer](data: DataFrame,transformer:T): Unit ={
    a [NullPointerException] should be thrownBy {
      transformer.transform(data).show()
    }
    ()
  }

  def testSparkException[T <: Transformer](data: DataFrame,transformer:T): Unit ={
    a [org.apache.spark.SparkException] should be thrownBy {
      transformer.transform(data).show()
    }
    ()
  }

}
