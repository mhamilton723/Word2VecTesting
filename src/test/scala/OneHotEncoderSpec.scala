import org.apache.spark.ml.feature.{HashingTF, NGram, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.functions.{lit, udf}

/**
  * Created by marhamil on 8/17/2016.
  */
class OneHotEncoderSpec extends SparkModuleSpec{

  "A OneHotEncoder" should "expand category indicies" in {
    val df = spark.createDataFrame(Seq(
      (0, 0.0),
      (1, 1.0),
      (2, 0.0),
      (3, 2.0),
      (4, 1.0),
      (5, 0.0)
    )).toDF("id", "categoryIndex")

    val encoded= new OneHotEncoder().setInputCol("categoryIndex").setOutputCol("categoryVec").transform(df)
    val oneHotList = extractDFCol[SparseVector](encoded,"categoryVec")
    val trueList = List(
      new SparseVector(2,Array(0),Array(1.0)),
      new SparseVector(2,Array(1),Array(1.0)),
      new SparseVector(2,Array(0),Array(1.0)),
      new SparseVector(2,Array(),Array()),
      new SparseVector(2,Array(1),Array(1.0)),
      new SparseVector(2,Array(0),Array(1.0))
    )
    oneHotList should be (trueList)
  }

  it should "also support interger indicies" in {
    val df = spark.createDataFrame(Seq(
      (0, 0),
      (1, 1),
      (2, 0),
      (3, 2),
      (4, 1),
      (5, 0)
    )).toDF("id", "categoryIndex")

    val encoded= new OneHotEncoder().setInputCol("categoryIndex").setOutputCol("categoryVec").transform(df)
    val oneHotList = extractDFCol[SparseVector](encoded,"categoryVec")
    val trueList = List(
      new SparseVector(2,Array(0),Array(1.0)),
      new SparseVector(2,Array(1),Array(1.0)),
      new SparseVector(2,Array(0),Array(1.0)),
      new SparseVector(2,Array(),Array()),
      new SparseVector(2,Array(1),Array(1.0)),
      new SparseVector(2,Array(0),Array(1.0))
    )
    oneHotList should be (trueList)
  }

  it should "support not dropping the last feature" in {
    val df = spark.createDataFrame(Seq(
      (0, 0.0),
      (1, 1.0),
      (2, 0.0),
      (3, 2.0),
      (4, 1.0),
      (5, 0.0)
    )).toDF("id", "categoryIndex")

    val encoded= new OneHotEncoder().setDropLast(false).setInputCol("categoryIndex").setOutputCol("categoryVec").transform(df)
    val oneHotList = extractDFCol[SparseVector](encoded,"categoryVec")
    val trueList = List(
      new SparseVector(3,Array(0),Array(1.0)),
      new SparseVector(3,Array(1),Array(1.0)),
      new SparseVector(3,Array(0),Array(1.0)),
      new SparseVector(3,Array(2),Array(1.0)),
      new SparseVector(3,Array(1),Array(1.0)),
      new SparseVector(3,Array(0),Array(1.0))
    )
    oneHotList should be (trueList)
  }


  it should "yield an error when applied to a null array" in {
    val df = spark.createDataFrame(Seq(
      (0, 0.0),
      (1, 1.0),
      (2, 1.0)
    )).toDF("id", "categoryIndex")
    df.show()
    val f: Int=>Option[Float] = {int:Int => {if(int==2){None}else{Some(int.toFloat)}}}
    val getNull = udf(f)
    val input = df.select(df("id"),getNull(df("id")).alias("categoryIndex"))
    testSparkException(input,new OneHotEncoder().setInputCol("categoryIndex"))
  }

  it should "yield an error when it receives a strange float" in {
    val df = spark.createDataFrame(Seq(
      (0, 0.0),
      (1, 1.0),
      (2, 0.4)
    )).toDF("id", "categoryIndex")
    testSparkException(df,new OneHotEncoder().setInputCol("categoryIndex"))

    val df2 = spark.createDataFrame(Seq(
      (0, 0.0),
      (1, 1.0),
      (2, -1.0)
    )).toDF("id", "categoryIndex")
    testSparkException(df2,new OneHotEncoder().setInputCol("categoryIndex"))
  }


}
