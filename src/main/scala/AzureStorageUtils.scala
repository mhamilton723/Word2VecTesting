
import java.io.{File, FileInputStream}
import java.util.Calendar

import FolderZiper.zipFolder
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
object AzureStorageUtils {

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

  def postJSON(body: String, url: String, host: String, keyFile: String): Unit = {
    System.setProperty("javax.net.ssl.trustStore",
      "C:\\Program Files\\Java\\jre1.8.0_101\\lib\\security\\cacerts")
    System.setProperty("javax.net.ssl.trustStorePassword", "changeit")
    System.setProperty("javax.net.ssl.trustStoreType", "JKS")
    System.setProperty("javax.net.debug", "ssl")
    //my certificate and password
    System.setProperty("javax.net.ssl.keyStore", keyFile)
    System.setProperty("javax.net.ssl.keyStorePassword", "AzureMLCertific8")
    System.setProperty("javax.net.ssl.keyStoreType", "PKCS12")

    val httpclient = new HttpClient()

    val method = new PostMethod()
    method.setPath(url)
    method.setRequestHeader("Content-Type", "application/json")
    method.setRequestHeader("Charset", "UTF-8")
    method.setRequestHeader("Host", host)
    method.setRequestHeader("Expect", "100-continue")
    method.setRequestEntity(new StringRequestEntity(body, "application/json", "UTF-8"))

    val statusCode = httpclient.executeMethod(method)
    System.out.println("Status: " + statusCode)

    method.releaseConnection()

    method.getResponseBodyAsString()
  } //default trust store parameters


  def uploadFileToStorage(pathToFile: String,
                          nameOfUpload: String,
                          accountName: String,
                          accountKey: String,
                          containerName: String): String = {

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
    val sourceFile: File = new File(pathToFile)
    blob.upload(new FileInputStream(sourceFile), sourceFile.length)

    getContainerSasUri(blob)
  }

  def getContainerSasUri(container: CloudBlockBlob) = {

    val sasPolicy = new SharedAccessBlobPolicy
    val cal = Calendar.getInstance()
    cal.set(2030, 11, 31)
    sasPolicy.setSharedAccessExpiryTime {
      cal.getTime
    }
    sasPolicy.setSharedAccessStartTime(Calendar.getInstance.getTime)
    sasPolicy.setPermissionsFromString("rwl")
    //println(sasPolicy.getPermissions)
    val url = container.generateSharedAccessSignature(sasPolicy, null)
    //println(container.getUri)
    //println("Generated SAS URL", container.getUri + "?" + url, " Expiry time = ", sasPolicy.getSharedAccessExpiryTime, "StartTime = ", sasPolicy.getSharedAccessStartTime)
    println(container.getUri + "?" + url)
    container.getUri + "?" + url
  }

  def zip(zipFilepath: String, filePaths: List[String]) {
    import java.io.{BufferedReader, File, FileOutputStream}
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

  def addPlatformAssets(jsonString: String, sasUrls: List[String]) = {
    val jsonAst = jsonString.parseJson

    val amendedJson = jsonAst.asJsObject.fields + (
      "Platforms" -> JsArray(JsObject(
        "Name" -> JsString("Python"),
        "PlatformVersion" -> JsNull,
        "PlatformAssets" -> JsArray(sasUrls.map(JsString(_)).toVector),
        "ProvisionableAssets" -> JsNull
      )))

    amendedJson.toJson.toString()
  }

  def addPlatformAssetsToFile(jsonFile: String, sasUrls: List[String]) = {
    val jsonString = scala.io.Source.fromFile(jsonFile).mkString
    addPlatformAssets(jsonString, sasUrls)
    //scala.tools.nsc.io.File(jsonFile).writeAll(newJson)
  }

  def getVersions(jsonFile: String): (String, String) = {
    val jsonString = scala.io.Source.fromFile(jsonFile).mkString
    val jsonAst = jsonString.parseJson.asJsObject.fields
    (jsonAst("MajorVersion").toString().replace("\"",""), jsonAst("MinorVersion").toString().replace("\"",""))
  }

  def createFile(filename:String,body:String="")=scala.tools.nsc.io.File(filename).writeAll(body)

  def findFiles(directory:String,regex:String):Array[File]={
    new File(directory).listFiles()
      .filter(!_.isDirectory)
      .filter(f =>regex.r.findFirstIn(f.getName).isDefined)
  }

  def processPaletteFolder(dirName:String, keyFile:String)={

    createFile(dirName +"__init__.py")
    val pyFiles = findFiles(dirName,""".*\.py$""")
    val jarFiles = findFiles(dirName,""".*\.jar$""")
    val azureDir = new File(dirName+"\\azureml\\")
    azureDir.mkdir()
    azureDir.canRead
    pyFiles.foreach(f=>{
      println(f.getName)
      FileUtils.copyFile(f,new File(azureDir.toString+"\\"+f.getName))
    })
    zipFolder(dirName+"azureml",dirName+"azureml.zip")

    val filesToUpload= List(new File(dirName+"azureml.zip"))++jarFiles.toList

    //val moprescuKey ="dHFkMMb/y4iKj0p9QMeoUcFonE7ObA1fkWroADzlvqREk9XljmSM+LuKiz4nXMIUQykCn1NgWBjvYJSaw57IDA=="
    val wbaumannKey = "hx/zpWLu6RI/zbrD14vB5ITiFb95jfW6ZGxDdfwf3O/AqcvTjQQln270KOSPKDKHUW4VW2XRFYuZZ+WmeHt6sw=="

    val paletteFile = dirName+"tempPalette.json"
    val versionFolders = getVersions(paletteFile)._1+"/"+getVersions(paletteFile)._2+"/"

    val sasUrls= filesToUpload.map(f=>{
      println(f.toString)
      println("moduledependencies/"+versionFolders+f.getName)

      uploadFileToStorage(
        f.toString,
        versionFolders+f.getName,
        "wbaumannstorage",
        wbaumannKey,
        "moduledependencies"
      )
    })

    val jsonString = addPlatformAssetsToFile(paletteFile,sasUrls)
    val url = "https://rdscurrent.azureml-test.net/palettes/definitions"
    val host = "rdscurrent.azureml-test.net"
    postJSON(jsonString,url,host,keyFile)

  }

  def main(args: Array[String]): Unit = {
    //val dataRoot = "D:\\Data\\Text\\"
    //val files = List("100", "1000", "10000").map(dataRoot + "small_data_" + _)
    //zip(dataRoot + "zipped_data.zip", files)

    //sasTest()

    val exampleRoot = "C:\\Users\\marhamil\\Documents\\Spark\\PaletteUploadExample\\"
    val keyFile = exampleRoot + "test000.azuremlidentity.cloudapp.net.pfx"
    processPaletteFolder(exampleRoot,keyFile)


  }
}
