package Generator

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.log

case class DOC(id: String, tf: Int, tfidf: Double)
case class Data(word: String, df: Int, doc: Array[DOC])

object TfIdf {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TfIdf")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    import sql.implicits._

    val inputFile = "hdfs://10.141.200.205:9000/home/16307130023/cleanLowerIndex"
    val outputFile = "hdfs://10.141.200.205:9000/home/16307130023/resIndex"

//    val inputFile = "hdfs://localhost:9000/cleanindex"
//    val outputFile = "hdfs://localhost:9000/resIndex"

    val wikiWithIndex = sc.objectFile[(String, (Int, Array[(String, Int, Array[Int])]))](inputFile)
    val totalDoc = wikiWithIndex.count()
    val df = wikiWithIndex.map {
      case (word, (df, l)) => {
        val withoutPos = l.map {
          case ((id, tf, pos)) => {
            (id, tf, tf * log(totalDoc / df))
          }
        }.sortBy(x => -x._3)
        (word, (df, withoutPos.take(3)))
      }
    }.map {
      case (word, (df, docs)) => {
        Data(word, df, docs.map {
          case (id, tf, tfidf) => {
            DOC(id, tf, tfidf)
          }
        })
      }
    }.toDF

    df.write.json(outputFile)
  }
}
