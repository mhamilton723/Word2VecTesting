import java.io.{File, FileInputStream, FileNotFoundException}
import java.lang.Enum
import java.net.{InetAddress, Socket}
import java.util
import javax.net.ssl.SSLSocketFactory

import com.microsoft.azure.storage.{CloudStorageAccount, StorageException}
import com.microsoft.azure.storage.blob._
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.{GetMethod, PostMethod}

import scala.io.Source
import scalaj.http.{Http, HttpOptions}

/**
  * Created by marhamil on 9/8/2016.
  */
object AzureStorageUtils {

  private def processArgs(argList: List[(String, Any)]) = {
    argList.map {
      case (key: String, value: Boolean) if value => " \"--" + key + "\""
      case (key: String, value: Boolean) if value => ""
      case (key: String, value: Any)              => " \"--" + key + "\", \"" + value.toString + "\""
    }.foldLeft("")((left, add) => left + " " + add + ",").dropRight(1)
  }

  private def combine[A](xs: Traversable[Traversable[A]]): Seq[Seq[A]] =
    xs.foldLeft(Seq(Seq.empty[A])) { (x, y) =>
      for (a <- x.view; b <- y) yield a :+ b
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

      val result =
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

  def javaGet(body: String, url: String, host: String): Unit = {
    System.setProperty("javax.net.ssl.trustStore",
                       "C:\\Program Files\\Java\\jre1.8.0_101\\lib\\security\\cacerts")
    System.setProperty("javax.net.ssl.trustStorePassword", "changeit")
    System.setProperty("javax.net.ssl.trustStoreType", "JKS")
    System.setProperty("javax.net.debug", "ssl")
    //my certificate and password
    System.setProperty(
      "javax.net.ssl.keyStore",
      "C:\\Users\\marhamil\\Documents\\Spark\\SparkTesting\\test000.azuremlidentity.cloudapp.net.pfx")
    System.setProperty("javax.net.ssl.keyStorePassword", "AzureMLCertific8")
    System.setProperty("javax.net.ssl.keyStoreType", "PKCS12")

    val httpclient = new HttpClient()

    val method = new PostMethod()
    method.setPath(url)
    method.setRequestHeader("Content-Type", "application/json")
    method.setRequestHeader("Charset", "UTF-8")
    method.setRequestHeader("Host", host)
    method.setRequestHeader("Expect", "100-continue")
    method.setRequestBody(body)

    val statusCode = httpclient.executeMethod(method)
    System.out.println("Status: " + statusCode)

    method.releaseConnection()

    method.getResponseBodyAsString()
  } //default truststore parameters

  def postJSON(jsonString: String, url: String, host: String): Unit = {
    System.setProperty("javax.net.ssl.trustStore",
                       "C:\\Program Files\\Java\\jre1.8.0_101\\lib\\security\\cacerts")
    System.setProperty("javax.net.ssl.trustStorePassword", "changeit")
    System.setProperty("javax.net.ssl.trustStoreType", "JKS")

    //my certificate and password
    System.setProperty(
      "javax.net.ssl.keyStore",
      "C:\\Users\\marhamil\\Documents\\Spark\\SparkTesting\\test000.azuremlidentity.cloudapp.net.pfx")
    System.setProperty("javax.net.ssl.keyStorePassword", "AzureMLCertific8")
    System.setProperty("javax.net.ssl.keyStoreType", "PKCS12")

    val sslsocketfactory =
      SSLSocketFactory.getDefault.asInstanceOf[SSLSocketFactory]

    val result = Http(url)
      .postData(jsonString)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .header("Host", host)
      .header("Expect", "100-continue")
      .header("Connection", "Keep-Alive")
      .option(HttpOptions.readTimeout(10000))
      .option(HttpOptions.sslSocketFactory(sslsocketfactory))
      .asString

    print("here")
  }

  def uploadFileToStorage(pathToFile: String,
                          nameOfUpload: String,
                          accountName: String,
                          accountKey: String,
                          containerName: String) = {

    val storageConnectionString: String = "DefaultEndpointsProtocol=http;" +
        s"AccountName=$accountName;" +
        s"AccountKey=$accountKey"

    val account: CloudStorageAccount =
      CloudStorageAccount.parse(storageConnectionString)
    val serviceClient: CloudBlobClient = account.createCloudBlobClient

    // Container name must be lower case.
    val container: CloudBlobContainer =
      serviceClient.getContainerReference(containerName)
    container.createIfNotExists


    // Upload an file.
    val blob: CloudBlockBlob = container.getBlockBlobReference(nameOfUpload)
    val sourceFile: File     = new File(pathToFile)
    blob.upload(new FileInputStream(sourceFile), sourceFile.length)


    println(getContainerSasUri(blob))
    //"https://wbaumannstorage.blob.core.windows.net/sparkdependencies/vanillavectorassembler_2.11-1.0.0.jar?st=2016-08-29T22%3A40%3A00Z&se=2030-08-30T22%3A40%3A00Z&sp=rl&sv=2015-04-05&sr=b&sig=0gAFos%2Fdh79nZoRObWqH0FMPJBVnPz2lzIa2YhbT%2Blc%3D"

  }

  def getContainerSasUri(container:CloudBlockBlob)={
    //Set the expiry time and permissions for the container.
    //In this case no start time is specified, so the shared access signature becomes valid immediately.
    val sasConstraints = new SharedAccessBlobPolicy()
    sasConstraints.setSharedAccessExpiryTime(new util.Date(2060))
    sasConstraints.setPermissions(util.EnumSet.of(SharedAccessBlobPermissions.LIST,SharedAccessBlobPermissions.WRITE))

    //Generate the shared access signature on the container, setting the constraints directly on the signature.
    val sasContainerToken = container.generateSharedAccessSignature(sasConstraints,"DownloadToolPolicy")


    //Return the URI string for the container, including the SAS token.
    container.getUri.toString +'/'+ sasContainerToken
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
        } finally {
          in.close()
        }

        zip.closeEntry()
      }
    } finally {
      zip.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val dataRoot = "D:\\Data\\Text\\"
    val files    = List("100", "1000", "10000").map(dataRoot + "small_data_" + _)
    //zip(dataRoot + "zipped_data.zip", files)

    uploadFileToStorage(
      dataRoot + "small_data_1000",
      "azureml/drivers/Mark/small_data_1000",
      "moprescustorage",
      "dHFkMMb/y4iKj0p9QMeoUcFonE7ObA1fkWroADzlvqREk9XljmSM+LuKiz4nXMIUQykCn1NgWBjvYJSaw57IDA==",
      "moprescuspark"
    )

    val url        = "https://rdscurrent.azureml-test.net/palettes/definitions"
    val host       = "rdscurrent.azureml-test.net"
    val jsonString = scala.io.Source.fromFile("tempPalette.json").mkString
    //javaGet(jsonString, url, host)
    //postJSON(jsonString,url,host)
  }
}
