import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.mongodb.util.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}


/**
  * created by LMR on 2019/6/12
  */
object DataPreProcess {

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://192.168.177.13/novels.aliwenxue")
      .getOrCreate()

    val frame: DataFrame = MongoSpark.load(spark)
    frame.createTempView("nolves")
    val res: DataFrame = spark.sql("SELECT category, content from nolves")

    val parseRDD: RDD[Row] = res.rdd.map { row =>
      val category: String = row.getAs[String]("category")
      val con: String = row.getAs[String]("content")
      val parse: String = Utils.replace(con)
      Row(category, parse)
    }

    //结果类型，其实就是表头，用于描述DataFrame
    val sch: StructType = StructType(List(
      StructField("category", StringType, true),
      StructField("content", StringType, true)
    ))

    //将RowRDD关联schema
    import spark.sqlContext
    val bdf: DataFrame = sqlContext.createDataFrame(parseRDD, sch)





    val conf: SparkConf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://192.168.177.13/novels.novelContentAndCategory")
    MongoSpark.save(bdf, WriteConfig(conf))

  }


}
