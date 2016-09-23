import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.DenseVector

/**
  * Created by marhamil on 9/20/2016.
  */
class Word2VecSpec extends SparkModuleSpec{
  def genTokenizedText()={
    spark.createDataFrame(Seq(
      (0, Array("I", "walked", "the", "dog","down","the","street")),
      (1, Array("I", "walked","with", "the", "dog")),
      (2, Array("I", "walked", "the", "pup"))
    )).toDF("label", "words")
  }

  "A Word2Vec featurizer" should "be able to operate on tokenized strings" in {
    val df = genTokenizedText()

    val df2 = new Word2Vec().setVectorSize(2).setMinCount(0)
      .setInputCol("words").setOutputCol("features").fit(df).transform(df)

    val lines = extractDFCol[DenseVector](df2, "features")
    val trueLines = List(
      new DenseVector(Array(-0.03570237755775452,-0.03602222885404314)),
      new DenseVector(Array(-0.044875194085761905,-0.06987244784832002)),
      new DenseVector(Array(-0.027968792244791985,-0.012610748410224915))
    )
    lines should be(trueLines)
  }

  it should "be able to return its vectors" in {
    val df = genTokenizedText()
    val model = new Word2Vec().setVectorSize(2).setMinCount(0)
      .setInputCol("words").setOutputCol("features").fit(df)
    val vectors = extractDFCol[DenseVector](model.getVectors,"vector")
    vectors.map(p=>p).apply(0) should be (new DenseVector(Array(-0.1556646227836609,0.17370985448360443)))
  }

  it should "be able to return synonyms" in {
    val df = genTokenizedText()
    val model = new Word2Vec().setVectorSize(2).setMinCount(0)
      .setInputCol("words").setOutputCol("features").fit(df)
    val synonyms = extractDFCol[String](model.findSynonyms("dog",2),"word")
    synonyms should be (List("I","down"))
  }


  it should "yield an error when applied to a null array" in {
    val tokenDataFrame = spark.createDataFrame(Seq(
      (0, Some(Array("Hi", "I", "can", "not", "foo"))),
      (1, None))
    ).toDF("label", "tokens")

    a [org.apache.spark.SparkException] should be thrownBy {
      new Word2Vec().setInputCol("tokens").setMinCount(0).fit(tokenDataFrame)
    }
  }

  it should "raise an error when given strange values of parameters" in {
    val df = genTokenizedText()

    testParameterExceptions(List(-1,-10),{n:Int=>
      var result = new Word2Vec().setMinCount(n).setInputCol("words").fit(df)
    })

    testParameterExceptions(List(-1,-10),{n:Int=>
      var result = new Word2Vec().setMinCount(0).setMaxIter(n).setInputCol("words").fit(df)
    })

    testParameterExceptions(List(0,-1,-10),{n:Int=>
      var result = new Word2Vec().setMinCount(0).setVectorSize(n).setInputCol("words").fit(df)
    })

    testParameterExceptions(List(0,-1,-10),{n:Int=>
      var result = new Word2Vec().setMinCount(0).setWindowSize(n).setInputCol("words").fit(df)
    })

    testParameterExceptions(List(0,-1,-10),{n:Int=>
      var result = new Word2Vec().setMinCount(0).setMaxSentenceLength(n).setInputCol("words").fit(df)
    })

    testParameterExceptions(List(0,-1,-10),{n:Int=>
      var result = new Word2Vec().setMinCount(0).setNumPartitions(n).setInputCol("words").fit(df)
    })

    testParameterExceptions(List(0.0,-1.0,-10.0),{n:Double=>
      var result = new Word2Vec().setMinCount(0).setStepSize(n).setInputCol("words").fit(df)
    })
  }


  it should "return a vector of zeros when it encounters an OOV word" in {
    val df = genTokenizedText()
    val model = new Word2Vec().setVectorSize(2).setMinCount(1).setInputCol("words").setOutputCol("features").fit(df)
    val df2 = spark.createDataFrame(Seq(
      (0, Array("ketchup")))).toDF("label", "words")
    val results = model.transform(df2)

    val lines = extractDFCol[DenseVector](results, "features")
    val trueLines = List(
      new DenseVector(Array(0.0,0.0))
    )
    lines should be(trueLines)
  }

  it should "be able to set vector size" in {
    val df = genTokenizedText()
    val vectorSizes = List(1,10,100)

    vectorSizes.map(n=>{
                          (n,extractDFCol[DenseVector](new Word2Vec().setMinCount(0).setVectorSize(n)
                          .setInputCol("words").setOutputCol("features").fit(df).transform(df),"features"))
                        }
    ).foreach(pair=>
      pair._2.map(p=>p).apply(0).size should be (pair._1)
    )

  }

}
