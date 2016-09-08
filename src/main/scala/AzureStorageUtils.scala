import java.io.{File, FileInputStream, FileNotFoundException}

import com.microsoft.azure.storage.{CloudStorageAccount, StorageException}
import com.microsoft.azure.storage.blob.{CloudBlobClient, CloudBlobContainer, CloudBlockBlob}

import scala.io.Source
import scalaj.http.{Http, HttpOptions}


/**
  * Created by marhamil on 9/8/2016.
  */
object AzureStorageUtils {

  private def processArgs(argList: List[(String, Any)]) = {
    argList.map {
      case (key: String, value: Boolean) if value =>
        " \"--" + key + "\""
      case (key: String, value: Boolean) if value =>
        ""
      case (key: String, value: Any) =>
        " \"--" + key + "\", \"" + value.toString + "\""
    }.foldLeft("")((left, add) => left + " " + add + ",").dropRight(1)
  }

  private def combine[A](xs: Traversable[Traversable[A]]): Seq[Seq[A]] =
    xs.foldLeft(Seq(Seq.empty[A])) {
      (x, y) => for (a <- x.view; b <- y) yield a :+ b
    }

  def submitJobsToCluster(argList: List[(String, List[Any])],
                          authorization:String,
                          jarPath:String = "wasb:///azureml/drivers/Mark/Spark2Word2Vec-assembly-1.0.jar",
                          className:String = "Word2VecTest",
                          clusterName:String = "moprescuspark"
                         ): Unit = {


    val keys = argList.map(_._1)
    val argStrings = combine[Any](argList.map(_._2)).toList.map(keys.zip(_)).map(processArgs)

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


      val result = Http(s"https://$clusterName.azurehdinsight.net/livy/batches").postData(data)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .header("Authorization", s"Basic $authorization")
        .header("Content-Type", "application/json; charset=utf-8")
        .header("Host", s"$clusterName.azurehdinsight.net")
        .header("Content-Length", "767")
        .header("Expect", "100-continue")
        .header("Connection", "Keep-Alive")
        .option(HttpOptions.readTimeout(10000)).asString
    })
  }

  def postJSON(jsonString:String,url:String, host:String,auth:String): Unit = {
      val result = Http(url).postData(jsonString)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .header("Authorization", s"Basic $auth")
        .header("Content-Type", "application/json; charset=utf-8")
        .header("Host", host)
        //.header("Content-Length", "767")
        .header("Expect", "100-continue")
        .header("Connection", "Keep-Alive")
        .option(HttpOptions.readTimeout(10000)).asString
  }

  def uploadFileToStorage(pathToFile:String,nameOfUpload:String,accountName:String,accountKey:String,containerName:String) = {

    val storageConnectionString: String = "DefaultEndpointsProtocol=http;" +
      s"AccountName=$accountName;" +
      s"AccountKey=$accountKey"

    val account: CloudStorageAccount = CloudStorageAccount.parse(storageConnectionString)
    val serviceClient: CloudBlobClient = account.createCloudBlobClient

    // Container name must be lower case.
    val container: CloudBlobContainer = serviceClient.getContainerReference(containerName)
    container.createIfNotExists

    // Upload an file.
    val blob: CloudBlockBlob = container.getBlockBlobReference(nameOfUpload)
    val sourceFile: File = new File(pathToFile)
    blob.upload(new FileInputStream(sourceFile), sourceFile.length)

  }

  def zip(zipFilepath: String, filePaths: List[String]) {
    import java.io.{BufferedReader, FileOutputStream, File}
    import java.util.zip.{ZipEntry, ZipOutputStream}
    val files = filePaths.map(new File(_))
    def readByte(bufferedReader: BufferedReader): Stream[Int] = {
      bufferedReader.read() #:: readByte(bufferedReader)
    }
    val zip = new ZipOutputStream(new FileOutputStream(zipFilepath))
    try {
      for (file <- files) {
        //add zip entry to output stream
        zip.putNextEntry(new ZipEntry(file.getName))

        val in = Source.fromFile(file.getCanonicalPath).bufferedReader()
        try {
          readByte(in).takeWhile(_ > -1).toList.foreach(zip.write)
        }
        finally {
          in.close()
        }

        zip.closeEntry()
      }
    }
    finally {
      zip.close()
    }
  }

  def main(args:Array[String]): Unit ={
    val dataRoot= "C:\\Users\\marhamil\\Documents\\Data\\training-monolingual-europarl\\"
    val files = List("100","1000","10000").map(dataRoot+"small_data_"+_)
    zip(dataRoot+"zipped_data.zip",files)

    uploadFileToStorage(
      dataRoot+"small_data_1000",
      "azureml/drivers/Mark/small_data_1000",
      "moprescustorage",
      "dHFkMMb/y4iKj0p9QMeoUcFonE7ObA1fkWroADzlvqREk9XljmSM+LuKiz4nXMIUQykCn1NgWBjvYJSaw57IDA==",
      "moprescuspark"
    )
  }

}

