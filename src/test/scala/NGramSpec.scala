import org.apache.spark.ml.feature.{NGram, Tokenizer}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * Created by marhamil on 8/11/2016.
  */
class NGramSpec extends SparkModuleSpec{

  def ngramDFToScalaList(dataFrame: DataFrame,outputCol:String = "ngrams")={
    dataFrame.select(dataFrame(outputCol)).collect()
      .map(row=>row.getAs[mutable.WrappedArray[Any]](0))
      .map(_.toList)
  }


  "An n-gram featurizer" should "be able to operate on tokenized strings" in {
    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "can", "not", "foo")),
      (1, Array("I")),
      (2, Array("Logistic", "regression")),
      (3, Array("Log","f", "reg"))
    )).toDF("label", "words")

    val ngramDF = new NGram().setN(3).setInputCol("words").setOutputCol("ngrams").transform(wordDataFrame)
    val ngrams = ngramDFToScalaList(ngramDF)
    ngrams(0) should be (Array("Hi I can", "I can not", "can not foo"))
    ngrams(1) should be (Array())
    ngrams(2) should be (Array())
    ngrams(3) should be (Array("Log f reg"))
  }

  it should "support several values for n" in {
    val ns = 1 to 6
    val words = Array("Hi", "I", "can", "not", "foo","bar","foo","afk")
    val wordDataFrame = spark.createDataFrame(Seq((0, words))).toDF("label", "words")
    val nGramResults = ns.map(n=>
      ngramDFToScalaList(
        new NGram().setN(n).setInputCol("words").setOutputCol("ngrams").transform(wordDataFrame))
    )
    ns.foreach( n =>
      nGramResults(n-1)(0).head should be (words.take(n).reduce(_+" "+_))
    )
  }

  it should "handle empty strings gracefully" in {
    val wordDataFrame = spark.createDataFrame(Seq(
      (0, "hey you no way"),
      (1, ""))
    ).toDF("label", "sentence")

    val tokenized = new Tokenizer().setInputCol("sentence").setOutputCol("tokens").transform(wordDataFrame)
    val ngrams = new NGram().setInputCol("tokens").setOutputCol("ngrams").transform(tokenized)
    ngramDFToScalaList(ngrams)(1) should be(Nil)
  }

  it should "yield an error when applied to a null array" in {
    val tokenDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "can", "not", "foo")),
      (1, null))
    ).toDF("label", "tokens")
    tokenDataFrame.show()
    tokenDataFrame.printSchema()
    testNullException(tokenDataFrame,new NGram().setInputCol("tokens"))
  }

  it should "raise error when given strange values of n" in {
    val words = Array("Hi", "I", "can", "not", "foo","bar","foo", "afk")
    val wordDataFrame = spark.createDataFrame(Seq((0, words))).toDF("label", "words")

    testParameterExceptions(wordDataFrame,List(0,-1,-10),{n:Int=>
        var result = new NGram().setN(n).setInputCol("words").transform(wordDataFrame)
      })
  }




}
