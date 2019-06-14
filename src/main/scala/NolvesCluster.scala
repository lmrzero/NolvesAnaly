import java.util

import com.huaban.analysis.jieba.JiebaSegmenter
import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, ml}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * created by LMR on 2019/6/13
  */

object NolvesCluster {

  Logger.getLogger("org").setLevel(Level.ERROR)
  case class Item(category: String, content: Array[String])
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://192.168.177.13/novels.novelContentAndCategory")
      .getOrCreate()


    val data: DataFrame = getDocumentsFromMongoDB(spark)
    //计算小说类型
   // data.createTempView("novels")
   // val numsCategoryFrame: DataFrame = spark.sql("SELECT DISTINCT(category) as disCategory FROM novels")
    //计算类别数量
   // numsCategoryFrame.createTempView("temp")
   // val numsCategory: Long = spark.sql("SELECT COUNT(*) FROM temp").first().getLong(0)
   // println(numsCategory)
    val wordsData: DataFrame = Utils.tokenizer(data, spark)
    //往数据库存储分词数据
    val conf: SparkConf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://192.168.177.13/novels.tokenizer")
   // MongoSpark.save(wordsData, WriteConfig(conf))
    val rescaleData: DataFrame = Utils.featurizes(wordsData)
 /*   val hashingTF = new HashingTF().setInputCol("content").setOutputCol("rawFeature").setNumFeatures(10)

    val featurizedData: DataFrame = hashingTF.transform(wordsData)

    featurizedData.show()
    val idf: IDF = new IDF().setInputCol("rawFeature").setOutputCol("features").setMinDocFreq(2)
    val idfModel: IDFModel = idf.fit(featurizedData)
    val rescaleData: DataFrame = idfModel.transform(featurizedData)*/

    rescaleData.show(5)
    // Trains a k-means model.
   /* val kmeans = new KMeans().setK(5).setSeed(1L)
    val model = kmeans.fit(rescaleData)
   // val centers: Array[linalg.Vector] = model.clusterCenters聚类中心
    model.save("E://kmensModel")*/
    val model: KMeansModel = KMeansModel.load("E://kmensModel")
    val res: DataFrame = model.transform(rescaleData)
    //预测结果和原始类标
    val labelAndPredict: DataFrame = res.select("category", "prediction")

    labelAndPredict.show()
    //往数据库存储预测结果
    /*val conf1: SparkConf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://192.168.177.13/novels.cluster")
    MongoSpark.save(labelAndPredict, WriteConfig(conf1))*/

  }

  def getDocumentsFromMongoDB(spark : SparkSession): DataFrame = {

    val mongoFrame: DataFrame = MongoSpark.load(spark)
    mongoFrame.createTempView("nolves")
    val res: DataFrame = spark.sql("SELECT category, content from nolves")
    res
  }

  def tokenizer(data: DataFrame, spark: SparkSession): DataFrame ={

    data.show(5)
    val itemRDD: RDD[Item] = data.rdd.map { row =>
      val category = row.getAs[String]("category")
      val content: String = row.getAs[String]("content")
      val str: String = new JiebaSegmenter().sentenceProcess(content).toString()
      val splits: Array[String] = str.split(" ").filter(!_.equals(""))
      Item(category, splits)
    }
    import spark.implicits._
    itemRDD.toDS.toDF()

  }



}
