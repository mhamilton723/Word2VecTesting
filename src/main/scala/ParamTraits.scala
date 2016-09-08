import org.apache.spark.annotation.Since
import org.apache.spark.ml.param.{Param, Params, StringArrayParam}

/**
  * Created by marhamil on 9/1/2016.
  */
/**
  * Trait for shared param inputCol.
  */
trait HasInputCol extends Params {

  /**
    * Param for input column name.
    * @group param
    */
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  /** @group getParam */
  final def getInputCol: String = $(inputCol)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)
}


/**
  * Trait for shared param outputCol (default: uid + "__output").
  */
trait HasOutputCol extends Params {

  /**
    * Param for output column name.
    * @group param
    */
  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")

  setDefault(outputCol, uid + "__output")

  /** @group getParam */
  final def getOutputCol: String = $(outputCol)


  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

}
