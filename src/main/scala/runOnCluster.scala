import java.io.{File, FileInputStream}
import java.util.Calendar

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob._
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.{PostMethod, StringRequestEntity}
import org.apache.commons.io.FileUtils
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.io.Source
import scalaj.http.{Http, HttpOptions}


/**
  * Created by marhamil on 9/8/2016.
  */
object ClusterUtils {

  private def processArgs(argList: List[(String, Any)]) = {
    argList.map {
      case (key: String, value: Boolean) if value => " \"--" + key + "\""
      case (key: String, value: Boolean) if value => ""
      case (key: String, value: Any) => " \"--" + key + "\", \"" + value.toString + "\""
    }.foldLeft("")((left, add) => left + " " + add + ",").dropRight(1)
  }

  private def combine[A](xs: Traversable[Traversable[A]]): Seq[Seq[A]] = {
    xs.foldLeft(Seq(Seq.empty[A])) { (x, y) =>
      for (a <- x.view; b <- y) yield a :+ b
    }
  }

  def submitJobsToCluster(argList: List[(String, List[Any])],
                          authorization: String,
                          jarPath: String =
                          "wasb:///azureml/drivers/Mark/Spark2Word2Vec-assembly-1.0.jar",
                          className: String = "Word2VecTest",
                          clusterName: String = "moprescuspark"): Unit = {

    val keys = argList.map(_._1)
    val argStrings =
      combine[Any](argList.map(_._2)).toList.map(keys.zip(_)).map(processArgs)

    argStrings.foreach({ argString =>
      val data =
        s"""
           |{
           |  "file":"$jarPath",
           |  "className":"$className",
           |  "proxyUser":null,
           |  "args":[  $argString
           |  ],
           |"conf":{
           |    "spark.driver.extraClassPath":"${jarPath.split("/").last}",
           |    "spark.executor.extraClassPath":"${jarPath.split("/").last}",
           |    "spark.yarn.max.executor.failures":"3",
           |    "spark.dynamicAllocation.enabled":"true",
           |    "spark.shuffle.service.enabled":"true",
           |    "spark.dynamicAllocation.initialExecutors":"2",
           |    "spark.dynamicAllocation.minExecutors":"0",
           |    "spark.dynamicAllocation.maxExecutors":"1024",
           |    "spark.executor.instances":"0",
           |    "spark.executor.cores":"4",
           |    "spark.executor.memory":"21g",
           |    "spark.driver.memory":"10g",
           |    "spark.driver.memory":"10g",
           |    "spark.driver.maxResultSize":"40g",
           |    "spark.yarn.executor.memoryOverhead":"4096"
           |  }
           |}
           |
      """.stripMargin

      Http(s"https://$clusterName.azurehdinsight.net/livy/batches")
        .postData(data)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .header("Authorization", s"Basic $authorization")
        .header("Content-Type", "application/json; charset=utf-8")
        .header("Host", s"$clusterName.azurehdinsight.net")
        .header("Content-Length", "767")
        .header("Expect", "100-continue")
        .header("Connection", "Keep-Alive")
        .option(HttpOptions.readTimeout(10000))
        .asString
    })
  }
}


  /**
  * Created by marhamil on 8/25/2016.
  */
object runOnCluster extends App {

  val argListTest = List(
    ("onCluster", List(true)),
    ("fileName", List("small_data_100000")),
    ("nIter", List(1)),
    ("nPart", List(2))
  )

  val argListSmall = List(
    ("onCluster", List(true)),
    ("fileName", List("training-monolingual-europarl")), //List("small_data_1000")),
    ("nIter", List(1, 2, 5, 10)),
    ("nPart", List(1, 2, 5, 10))
  )

  val argListLarge = List(
    ("onCluster", List(true)),
    ("fileName", List("cleaned_pre.pos.corpus", "large_word2vec_data.txt")),
    ("nIter", List(5, 10)),
    ("nPart", List(5, 10))
  )
  ClusterUtils.submitJobsToCluster(argListTest, authorization = ???)

}
