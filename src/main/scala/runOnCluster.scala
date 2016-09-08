import AzureStorageUtils._


/**
  * Created by marhamil on 8/25/2016.
  */

object runOnCluster extends App {

  val argListTest = List(
    ("onCluster", List(true)),
    ("fileName", List("small_data_100000")),
    ("nIter", List(1)),
    ("nPart", List(2))
  )

  val argListSmall = List(
    ("onCluster", List(true)),
    ("fileName", List("training-monolingual-europarl")),//List("small_data_1000")),
    ("nIter", List(1, 2, 5, 10)),
    ("nPart", List(1, 2, 5, 10))
  )

  val argListLarge = List(
      ("onCluster", List(true)),
      ("fileName", List("cleaned_pre.pos.corpus","large_word2vec_data.txt")),
      ("nIter", List(5, 10)),
      ("nPart", List(5, 10))
    )
  submitJobsToCluster(argListTest,authorization = "YWRtaW46Tng1NXhlNm50M0dWc3B2RSE=")

}





