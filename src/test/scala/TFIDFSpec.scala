import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.functions._

/**
  * Created by marhamil on 9/20/2016.
  */
class TFIDFSpec extends SparkModuleSpec {

  import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

  "A IDF" should "be able to operate on hashingTF output" in {
    val sentenceData = spark.createDataFrame(Seq(
      (0, "Hi I"),
      (1, "I wish"),
      (2, "we Cant")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)

    //featurizedData.show(false)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    //rescaledData.select("features", "label").show(false)

    val lines = extractDFCol[SparseVector](rescaledData, "features")
    val trueLines = List(
      new SparseVector(20, Array(0, 9), Array(0.6931471805599453, 0.28768207245178085)),
      new SparseVector(20, Array(9, 15), Array(0.28768207245178085, 0.6931471805599453)),
      new SparseVector(20, Array(6, 13), Array(0.6931471805599453, 0.6931471805599453))
    )
    lines should be(trueLines)
  }

  it should "be able to operate on dense or sparse vectors" in {
    val denseVects = Seq(
      (0, new DenseVector(Array(1, 1, 0, 0, 0))),
      (1, new DenseVector(Array(0, 1, 1, 0, 0))),
      (2, new DenseVector(Array(0, 0, 0, 1, 1))))

    val denseVectDF = spark.createDataFrame(denseVects).toDF("label", "features")
    val sparseVectDF = spark.createDataFrame(denseVects.map(p => (p._1, p._2.toSparse))).toDF("label", "features")

    val rescaledDD = new IDF().setInputCol("features").setOutputCol("scaledFeatures").fit(denseVectDF).transform(denseVectDF)
    val rescaledDS = new IDF().setInputCol("features").setOutputCol("scaledFeatures").fit(denseVectDF).transform(sparseVectDF)
    val rescaledSD = new IDF().setInputCol("features").setOutputCol("scaledFeatures").fit(sparseVectDF).transform(denseVectDF)
    val rescaledSS = new IDF().setInputCol("features").setOutputCol("scaledFeatures").fit(sparseVectDF).transform(sparseVectDF)

    val resultsD = List(rescaledDD, rescaledSD).map(extractDFCol[DenseVector](_, "scaledFeatures"))
    val resultsS = List(rescaledDS, rescaledSS).map(extractDFCol[SparseVector](_, "scaledFeatures"))

    resultsD.head should be(resultsD(1))
    resultsS.head should be(resultsS(1))
    resultsD.head.map(_.toSparse) should be(resultsS.head)
  }

  it should "yield an error when applied to a null array" in {
    val df = spark.createDataFrame(Seq(
      (0, Some(new DenseVector(Array(1, 1, 0, 0, 0)))),
      (1, Some(new DenseVector(Array(0, 1, 1, 0, 0)))),
      (2, None))).toDF("id", "features")
    //df.show()
    val df2 = new IDF().setInputCol("features")
    a [org.apache.spark.SparkException] should be thrownBy {
      new IDF().setInputCol("features").fit(df)
    }
  }

  it should "support setting minDocFrequency" in {
    val df = spark.createDataFrame(Seq(
      (0, new DenseVector(Array(1, 1, 0, 0, 0))),
      (1, new DenseVector(Array(0, 1, 1, 0, 0))),
      (2, new DenseVector(Array(0, 0, 0, 1, 1))))).toDF("id", "features")

    val df2 = new IDF().setMinDocFreq(2).setInputCol("features").setOutputCol("rescaledFeatures").fit(df).transform(df)
    val lines = extractDFCol[DenseVector](df2, "rescaledFeatures")
    val trueLines = List(
      new DenseVector(Array(0.0,0.28768207245178085,0.0,0.0,0.0)),
      new DenseVector(Array(0.0,0.28768207245178085,0.0,0.0,0.0)),
      new DenseVector(Array(0.0,0.0,0.0,0.0,0.0))
    )
    lines should be(trueLines)
  }

  ignore should "raise an error when given strange values of minDocumentFrequency" in {
    val df = spark.createDataFrame(Seq(
      (0, new DenseVector(Array(1, 1, 0, 0, 0))),
      (1, new DenseVector(Array(0, 1, 1, 0, 0))),
      (2, new DenseVector(Array(0, 0, 0, 1, 1))))).toDF("id", "features")

    new IDF().setMinDocFreq(-1).setInputCol("features").fit(df).transform(df).show()
    testParameterExceptions(List(-1,-10),{n:Int=>
      var result = new IDF().setMinDocFreq(n).setInputCol("features").fit(df)
    })
  }

}