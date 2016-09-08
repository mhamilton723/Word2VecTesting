import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.NGram
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable


trait PhraseFinderParams extends Params with HasInputCol with HasOutputCol{
  val delta: DoubleParam = new DoubleParam(this, "delta", "value to subtract from bigram counts" +
    " to prevent unfrequent bigrams from being chosen", ParamValidators.gtEq(0))
  def setDelta(value: Double): this.type = set(delta, value)
  def getDelta: Double = $(delta)
  setDefault(delta -> 1)

  val threshold: DoubleParam = new DoubleParam(this, "threshold", "threshold value on bigram score" +
    " to include it as a phrase")
  def setThreshold(value: Double): this.type = set(threshold, value)
  def getThreshold: Double = $(threshold)
  setDefault(threshold -> .25)

  val iterations: IntParam = new IntParam(this, "iterations", "number of passes through the data, " +
    " sets the max length of phrases to include it as a phrase", ParamValidators.gtEq(1))
  def setIterations(value: Int): this.type = set(iterations, value)
  def getIterations: Int = $(iterations)
  setDefault(iterations-> 1)
}

/**
  * Created by marhamil on 8/10/2016.
  */
class PhraseFinder(override val uid: String) extends Estimator[PhraseFinderModel]
  with PhraseFinderParams{

  def this() = this(Identifiable.randomUID("phrasefinder"))

  val spark = SparkSession.builder.getOrCreate()

  var scores:DataFrame = _
  var phrases: Set[String] = _
  protected var bigramCollections: List[Broadcast[Set[(String, String)]]] = Nil
  //var selectedBigrams: Broadcast[Set[(String, String)]] = _
  import spark.implicits._

  private def transformColumn(oneColDataset:DataFrame,
                              bigrams:Broadcast[Set[(String, String)]],
                              outputCol:String = "phrases"):DataFrame = {

    oneColDataset.rdd.map(row =>
      row(0).asInstanceOf[mutable.WrappedArray[String]].toArray
    ).map {
      case Array() => Array():Array[String]
      case Array(s) => Array(s)
      case arr@a =>
        var result: List[String] = Nil
        var i = 0
        while (i < arr.length - 1) {
          if (bigrams.value.contains((arr(i), a(i + 1)))) {
            result ::= (a(i) + " " + a(i + 1))
            i += 2
          }else if(i < arr.length - 2){
            result ::= arr(i)
            i += 1
          }else{
            result ::= arr(i)
            result ::= arr(i+1)
            i+=1
          }
        }
        result.reverse.toArray
    }.toDF(outputCol)
  }

  private def findBigrams(dataframe:DataFrame, inputCol:String) ={
    val wordCounts = dataframe.select(explode(dataframe(inputCol)).alias("word"))
      .groupBy("word").count().select('word,'count.alias("word_count"))

    val bigrams = new NGram().setInputCol(inputCol).setOutputCol("bigrams").transform(dataframe)
    val bigramCounts = bigrams
      .select(explode('bigrams).alias("bigram"))
      .groupBy("bigram")
      .count()


    //TODO left off here
    val sepBigrams = bigramCounts.map(row=> (row.get(0), row.getLong(1))).toDF().select('_1.alias("bigram"),'_2.alias("bigram_count"))


    sepBigrams.cache()

    val allCounts = sepBigrams
      .join(wordCounts,$"bigram._1"===wordCounts("word"))
      .select('bigram,'bigram_count,'word.alias("word1"),'word_count.alias("word_count1"))
      .join(wordCounts,$"bigram._2"===wordCounts("word"))
      .select('bigram,'bigram_count,'word1,'word_count1,'word.alias("word2"),'word_count.alias("word_count2"))

    allCounts.printSchema()
    val bigramScores = allCounts.select('bigram,(('bigram_count-getDelta)/('word_count1*'word_count1)).alias("bigram_score")).cache()

    bigramScores.sort(-'bigram_score).show()

    scores=bigramScores

    spark.sparkContext.broadcast(
      scores.select(scores("bigram"))
        .where(scores("bigram_score")>=getThreshold)
        .rdd.map(row=>
        (row(0).asInstanceOf[GenericRowWithSchema].get(0),
          row(0).asInstanceOf[GenericRowWithSchema].get(1)).asInstanceOf[(String,String)]
      )
        .collect().toSet)
  }

  def fit(dataset:Dataset[_]):PhraseFinderModel = {
    var internalDataset = dataset.select(dataset(getInputCol).alias("words"))
    (1 to getIterations).foreach(i =>{
      val bigrams = findBigrams(internalDataset,"words")
      internalDataset=transformColumn(internalDataset,bigrams,"words")
      bigramCollections ::= bigrams
    })

    phrases = bigramCollections.head.value.map(p=>p._1+" "+p._2)
    copyValues(new PhraseFinderModel(uid, bigramCollections)).setParent(this)

  }

  def getScores:DataFrame= scores

  protected def outputDataType: DataType = new ArrayType(StringType, false)

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    //validateInputType(inputType) //TODO get validate input type to work
    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), outputDataType, nullable = false)
    StructType(outputFields)
  }


  override def copy(extra: ParamMap) = defaultCopy(extra)

}


class PhraseFinderModel (override val uid: String, val bigramCollections: List[Broadcast[Set[(String,String)]]])
  extends Model[PhraseFinderModel] with PhraseFinderParams{

  override def copy(extra: ParamMap): PhraseFinderModel =  defaultCopy(extra)
  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._
  lazy val phrases = parent.asInstanceOf[PhraseFinder].phrases //TODO fix this hack

  override def transformSchema(schema: StructType): StructType = parent.transformSchema(schema)

  def zip(df1: Dataset[_],df2: DataFrame):DataFrame={ //TODO make this robust to naming collisions
    def addIndex(df: Dataset[_]) = spark.sqlContext.createDataFrame(
      // Add index
      df.rdd.zipWithIndex.map{
        case (r:Row, i) => Row.fromSeq(r.toSeq :+ i)
        case _ => throw new IllegalArgumentException("Need to operate on a dataframe")
      },
      // Create schema
      StructType(df.schema.fields :+ StructField("___index", LongType, nullable = false))
    )

    // Join and clean
    addIndex(df1).join(addIndex(df2), Seq("___index")).drop("___index")
  }

  override def transform(dataset:Dataset[_]):DataFrame={
    var internalDataset = dataset.select(dataset(getInputCol).alias("words"))
    (0 until getIterations).foreach(iter => {
      val out = internalDataset.rdd.map(row =>
        row(0).asInstanceOf[mutable.WrappedArray[String]].toArray
      ).map {
        case Array() => Array():Array[String]
        case Array(s) => Array(s)
        case arr@a =>
          var result: List[String] = Nil
          var i = 0
          while (i < arr.length - 1) {
            if (bigramCollections.reverse(iter).value.contains((arr(i), a(i + 1)))) {
              result ::= (a(i) + " " + a(i + 1))
              i += 2
            }else if(i < arr.length - 2){
              result ::= arr(i)
              i += 1
            }else{
              result ::= arr(i)
              result ::= arr(i+1)
              i+=1
            }
          }
          result.reverse.toArray
      }.toDF(getOutputCol)
      internalDataset=out
    })



    zip(dataset,internalDataset)
  }


}
