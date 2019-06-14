import NolvesCluster.Item
import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * created by LMR on 2019/6/14
  */
object Utils {

  def replace(str : String) : String = {//去除小说简介中的标点符号

    str.trim.replaceAll("，|。|【|】|！|：|、|\\s|\\.", "")
  }

  def tokenizer(data: DataFrame, spark: SparkSession): DataFrame ={

    data.show(5)
    val itemRDD: RDD[Item] = data.rdd.map { row =>
      val category = row.getAs[String]("category")
      val content: String = row.getAs[String]("content")
      /*val str: String = new JiebaSegmenter().sentenceProcess(content).toString()
      val splits: Array[String] = str.split(" ").filter(!_.equals(""))*/
      val splits: Array[String] = strTokenizer(content)
      Item(category, splits)
    }
    import spark.implicits._
    itemRDD.toDS.toDF()

  }

  def strTokenizer(str : String): Array[String] ={
    val res: String = new JiebaSegmenter().sentenceProcess(str).toString()
    res.split(" ").filter(!_.equals(""))
  }

  def featurizes(data : DataFrame): DataFrame ={
    val hashingTF = new HashingTF().setInputCol("content").setOutputCol("rawFeature").setNumFeatures(10)
    val featurizedData: DataFrame = hashingTF.transform(data)
    featurizedData.show()
    val idf: IDF = new IDF().setInputCol("rawFeature").setOutputCol("features").setMinDocFreq(2)
    val idfModel: IDFModel = idf.fit(featurizedData)
    idfModel.transform(featurizedData)
  }



}
