import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector

/**
  * Created by marhamil on 8/11/2016.
  */
class HashingTFSpec extends SparkModuleSpec{

  "A HashingTF" should "be able to operate on tokenized strings" in {
    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "can", "not", "foo","foo")),
      (1, Array("I")),
      (2, Array("Logistic", "regression")),
      (3, Array("Log","f", "reg"))
    )).toDF("label", "words")

    val hashDF = new HashingTF().setInputCol("words").setOutputCol("hashedTF").transform(wordDataFrame)
    val lines = extractDFCol[SparseVector](hashDF,"hashedTF")

    val trueLines = List(
      new SparseVector(262144,Array(36073,51654,113890,139098,242088),Array(1.0,2.0,1.0,1.0,1.0)),
      new SparseVector(262144,Array(113890),Array(1.0)),
      new SparseVector(262144,Array(13671,142455),Array(1.0,1.0)),
      new SparseVector(262144,Array(24152,74466,122984),Array(1.0,1.0,1.0))
    )
    lines should be (trueLines)
  }

  it should "support several values for number of features" in {
    val featureSizes = List(1,5,100,100000)
    val words = Array("Hi", "I", "can", "not", "foo","bar","foo","afk")
    val wordDataFrame = spark.createDataFrame(Seq((0, words))).toDF("label", "words")

    val fsResults = featureSizes.map(n=>
      extractDFCol[SparseVector](
        new HashingTF().setNumFeatures(n).setInputCol("words").setOutputCol("hashedTF").transform(wordDataFrame), "hashedTF")(0)
    )
    val trueResults = List(
      new SparseVector(1,Array(0),Array(8.0)),
      new SparseVector(5,Array(0,2,3),Array(4.0,2.0,2.0)),
      new SparseVector(100,Array(0,10,18,33,62,67,80),Array(1.0,2.0,1.0,1.0,1.0,1.0,1.0)),
      new SparseVector(100000,Array(5833,9467,16680,29018,68900,85762,97510),Array(1.0,1.0,1.0,1.0,1.0,1.0,2.0))
    )
    fsResults should be (trueResults)
  }

  it should "treat empty strings as another word" in {
    val wordDataFrame = spark.createDataFrame(Seq(
      (0, "hey you no way"),
      (1, "")//,
    )
    ).toDF("label", "sentence")

    val tokenized = new Tokenizer().setInputCol("sentence").setOutputCol("tokens").transform(wordDataFrame)
    val hashDF = new HashingTF().setInputCol("tokens").setOutputCol("HashedTF").transform(tokenized)

    val lines = extractDFCol[SparseVector](hashDF,"hashedTF")
    lines(1) should be (new SparseVector(262144,Array(249180),Array(1.0)))
  }

  it should "yield an error when applied to a null array" in {
    val tokenDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "can", "not", "foo")),
      (1, null))
    ).toDF("label", "tokens")
    testNullException(tokenDataFrame,new HashingTF().setInputCol("tokens"))
  }

  it should "raise error when given strange values of n" in {
    val words = Array("Hi", "I", "can", "not", "foo","bar","foo", "afk")
    val wordDataFrame = spark.createDataFrame(Seq((0, words))).toDF("label", "words")

    testParameterExceptions(wordDataFrame,List(0,-1,-10),{n:Int=>
      var result = new HashingTF().setNumFeatures(n).setInputCol("words").transform(wordDataFrame)
    })
  }




}
