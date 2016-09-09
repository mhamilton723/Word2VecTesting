import java.io.FileWriter

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import scopt.OptionParser

//import org.apache.spark.implicits._

/**
  * Created by marhamil on 8/9/2016.
  */
object Word2VecTest {

  case class Config(fileName: String = "training-monolingual-europarl",
                    nDim: Int = 300,
                    minCount: Int = 10,
                    nPart: Int = 10,
                    nIter: Int = 10,
                    environment: String = "moprescuspark")

  val parser = new OptionParser[Config]("scopt") {
    head("Word2VecTest", "0.1")

    opt[String]("fileName").action((x, c) => c.copy(fileName = x)).text("Embedding Filename")
    opt[Int]("nDim").action((x, c) => c.copy(nDim = x)).text("Number of embedding dimension")
    opt[Int]("minCount")
      .action((x, c) => c.copy(minCount = x))
      .text("Min number of words to be included in embedding")
    opt[Int]("nPart").action((x, c) => c.copy(nPart = x)).text("Number of parallel jobs")
    opt[Int]("nIter").action((x, c) => c.copy(nIter = x)).text("Number of iterations")
    opt[String]("environment")
      .action((x, c) => c.copy(environment = x))
      .text("sets the app to run in a specific environment options local,moprescuspark,cluster")

  }

  def main(args: Array[String]) {
    println("Args start")
    args.foreach(println)
    println("Args end")

    parser.parse(args, Config()) match {
      case Some(config) =>
        var master: String    = null
        var dataRoot: String  = null
        var saveRoot: String  = null
        var warehouse: String = null

        if (config.environment == "local") {
          master = "local[*]"
          warehouse = "file:///C:/users/marhamil/IdeaProjects/Spark2Word2Vec/my/"
          dataRoot = "D:/Data/Text/"
          saveRoot = "D:/Data/fit_embeddings/"
        } else if (config.environment == "cluster") {
          master = "yarn-cluster"
          warehouse = "wasb:///azureml/drivers/Mark"
          dataRoot = "wasb:///azureml/drivers/Mark/text_data/"
          saveRoot = "wasb:///azureml/drivers/Mark/fit_embeddings/"
        } else if (config.environment == "moprescuspark") {
          master = "local[*]"
          warehouse = "/home/hdiuser/IdeaProjects/SparkWord2Vec/my/"
          dataRoot = "/home/hdiuser/Data/Text/"
          saveRoot = "/home/hdiuser/Data/fit_embeddings/"
        } else {
          throw new IllegalArgumentException(" not a proper environment: " + config.environment)
        }

        println((master, warehouse))
        val spark = SparkSession.builder
          .master(master)
          .appName("My App")
          .config("driver-memory", "10g")
          .config("spark.yarn.executor.memoryOverhead", "10g")
          .config("spark.executor.memory", "10g")
          .config("spark.driver.memory", "10g")
          .config("spark.driver.maxResultSize", "10g")
          .config("spark.sql.warehouse.dir", warehouse)
          .config("spark.rpc.message.maxSize", "246")
          .getOrCreate()
        LogManager.getRootLogger.setLevel(Level.WARN)
        import spark.implicits._

        //FOOFS
        val loadModel   = false
        val runWord2Vec = true
        val saveModel   = true

        def genFileName(root: String,
                        prefix: String = "embeddings",
                        extension: String = ".json"): String = {
          val middle = "_fn_" + config.fileName +
              "_minCount_" + config.minCount +
              "_nPart_" + config.nPart + "_nIter_" + config.nIter
          root + prefix + middle + extension
        }

        //MAIN CODE
        val logFile = dataRoot + config.fileName
        val sentenceDF = spark.read
          .textFile(logFile)
          .map(s => "http:\\/\\/.*".r.replaceAllIn(s, " url "))
          .map(s => "(\\d{5,})".r.replaceAllIn(s, " num "))
          .filter(s => !s.startsWith("CURRENT URL"))
          .flatMap(s => s.split("\\.|\\!|\\?"))
          .filter(s => !s.contains("| Next chapter | | Previous chapter |"))
          .toDF("text")
        //sentenceDF.show(10,truncate = false)

        val tokenizer =
          new RegexTokenizer().setInputCol("text").setOutputCol("tokens").setPattern("\\W")
        val tokenized = tokenizer.transform(sentenceDF) //.select('tokens).where('tokens)
        tokenized.show(5)
        //tokenized.printSchema()

        //val phraseFinder = new PhraseFinder().setIterations(2).setInputCol("tokens").setOutputCol("phrases").fit(tokenized)
        //println(phraseFinder.phrases)
        //val phrases = phraseFinder.transform(tokenized).show(truncate=false)

        val remover = new StopWordsRemover().setInputCol("tokens").setOutputCol("filtered_tokens")
        val removed = remover.transform(tokenized) //.distinct() this causes memory leaks for some reason
        //removed.show(5,truncate = false)

        if (runWord2Vec) {
          println("Starting word2vec")
          val word2Vec = new Word2Vec()
            .setInputCol("filtered_tokens")
            .setOutputCol("embeddings")
            .setVectorSize(config.nDim)
            .setMinCount(config.minCount)
            .setNumPartitions(config.nPart)
            .setMaxIter(config.nIter)
          val model = if (loadModel) {
            Word2VecModel.load(genFileName(saveRoot))
          } else {
            word2Vec.fit(removed)
          }

          if (saveModel) {
            println("saving model")
            try {
              val vects = model.getVectors
              vects.write.json(genFileName(saveRoot))
              println("Written")
            } catch {
              case e: org.apache.spark.sql.AnalysisException =>
                println("Overwriting")
                model.getVectors.write.mode("overwrite").json(genFileName(saveRoot))
            }
          }

          //PRINT LIST OF SYNONYMS
          val target_words =
            List("cat", "blue", "how", "can", "africa", "try", "ten", "two")
          for (word <- target_words) {
            try {
              val synonymns = model.findSynonyms(word, 5)
              println(word + " in vocab:")
              synonymns.show(5)
            } catch {
              case e: java.lang.IllegalStateException =>
                println(word + " not in vocab")
            }
          }
        }

      case None =>
      // arguments are bad, error message will have been displayed
    }
  }
}
