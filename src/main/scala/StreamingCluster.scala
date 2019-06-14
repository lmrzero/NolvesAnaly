
import NolvesCluster.Item
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON

/**
  * created by LMR on 2019/6/14
  */
object StreamingCluster {

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MongoSparkConnectorIntro")
      .getOrCreate()

    //val conf: SparkConf = new SparkConf().setAppName("WindowOperaions").setMaster("local[*]")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val zkQuorum = "master:2181,slave1:2181,slave2:2181"
    val groupId = "g1"
    val topic = Map[String, Int]("test" -> 1)

    //创建DStream，需要KafkaDStream
    val streamData: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)
    //对数据进行处理
    //Kafak的ReceiverInputDStream[(String, String)]里面装的是一个元组（key是写入的key，value是实际写入的内容）
    val lines: DStream[(String, String)] = streamData.map { row =>
      val jsonStr: String = row._2
      val jsonToMap: Map[String, String] = JSON.parseFull(jsonStr).get.asInstanceOf[Map[String, String]]
      val content: String = jsonToMap.get("content").get
      val category: String = jsonToMap.get("category").get
      (Utils.replace(content), category)
    }


    val kmeansModel: KMeansModel = KMeansModel.load("E://kmensModel")
    val word2VexModel: Word2VecModel = Word2VecModel.load("E://word2VexModel")
    lines.foreachRDD{rdd =>

      val structType  = StructType(Array(StructField("content",StringType,true), StructField("category",StringType,true)))
      val rowRDD: RDD[Row] = rdd.map(x => Row(x._1, x._2))
      val dataFrame: DataFrame = spark.createDataFrame(rowRDD, structType)
      val tokenizer: DataFrame = Utils.tokenizer(dataFrame, spark)
      val features: DataFrame = word2VexModel.transform(tokenizer)
      val res: DataFrame = kmeansModel.transform(features)
      res.show()

    }
    ssc.start()
    ssc.awaitTermination()
  }

}
