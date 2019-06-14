import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * created by LMR on 2019/6/13
  */
object word2vec {

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://192.168.177.13/novels.tokenizer")
      .getOrCreate()

    val data: DataFrame = MongoSpark.load(spark)
    val content: DataFrame = data.select("content", "category").limit(5)

    content.select("content").rdd.foreach{row =>

      println(row.get(0))
    }
    content.show(5)
    val word2Vec = new Word2Vec()
      .setInputCol("content")
      .setOutputCol("features")
      .setVectorSize(3)
      .setMinCount(0)


    val model: Word2VecModel = word2Vec.fit(content)

    model.save("E://word2VexModel")
    val result: DataFrame = model.transform(content)

    result.show(5)
   // val wordVec: DataFrame = result.select("result")

    val kmeans = new KMeans().setK(5).setSeed(1L)
    val kmeansModel = kmeans.fit(result)
    // val centers: Array[linalg.Vector] = model.clusterCenters聚类中心
    kmeansModel.save("E://kmensModel")
    val res: DataFrame = kmeansModel.transform(result)

    val rows: Array[Row] = res.select("category", "prediction", "content").collect()

    for (elem <- rows) {
      println(elem.toSeq)
    }

  }

}
