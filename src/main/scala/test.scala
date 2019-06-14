
import java.util

import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.concurrent.JavaConversions
import com.mongodb.util.JSON

/**
  * created by LMR on 2019/6/13
  */
object test {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {

    val str = "千万年前，李七夜栽下一株翠竹。对滴【宿舍】调度到！说说说"
    val res: String = str.replaceAll("，|。|【|】|！", " ")

    //println(res)
    import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

    /*val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://192.168.177.13/novels.novel")
      .getOrCreate()

    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    wordsData.show()
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors
    featurizedData.show()
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    val rows: Array[Row] = rescaledData.select("features").collect()
    for (elem <- rows) {
      println(elem.toSeq)
    }*/
   // jiebaTest()
    jsonTest()
  }

  def jiebaTest(): Unit ={

    val str = "这是一个就业路上屡被蹂躏的古汉语专业研究生,回到了明朝中叶,进入了山村一家幼童身体后的故事,木讷父亲泼辣娘,一水的极品亲戚 农家小院是非不少 好在 咱有几千年的历史积淀 四书五经八股文 专业也对口 谁"
    val segmenter = new JiebaSegmenter()

    val list = segmenter.sentenceProcess(str)
    println(list.toString)
   /* import scala.collection.JavaConverters
    val buffer: mutable.Buffer[String] = JavaConverters.asScalaIteratorConverter(list.iterator).asScala.toBuffer.filter(!_.equals(" "))

    println(buffer)

    val terms: util.List[Term] = StandardTokenizer.segment(str)
    val buffer1: mutable.Buffer[Term] = JavaConverters.asScalaIteratorConverter(terms.iterator()).asScala.toBuffer


    println(buffer1)*/
    /* for (elem <- array) {
      println(elem)
    }*/

  }

  def jsonTest(): Unit ={
    val str = "{\"author\": \"\\u54c8\\u54c8\", \"category\": \"\\u6b66\\u4fa0\", \"content\": \"\\u554a\\u554a\\u554a\\u554a\\u554a\\u554a\\u554a\\u554a\\u554a\\u554a\", \"bookname\": \"\\u65e0\\u654c\", \"spider\": \"sss\"}"
   /* val jsonS = JSON.parseFull(str)
    val buffer: mutable.Buffer[Any] = jsonS.toBuffer
    println(buffer)*/




  }
  def regJson(json:AnyRef) = json match {
    case Some(map: Map[String, Any]) => map
    //      case None => "erro"
    //      case other => "Unknow data structure : " + other
  }

}
