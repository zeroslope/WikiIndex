package WikiIndex

import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level,Logger}
import scala.Array.concat
import scala.xml.XML
import scala.collection.mutable.{Map, ArrayBuffer}

object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("WikiIndex2")
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<page>")
    sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</page>")
    sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "utf-8")

    val inputFile = "hdfs://10.141.200.205:9000/home/jc/enwikisource-20171020-pages-articles-multistream.xml"
//    val inputFile = "hdfs://localhost:9000/wiki.xml"

    val records = sc.newAPIHadoopFile(
      inputFile,
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text]
    ).flatMap {
      case doc => {
        val docXml = XML.loadString(doc._2.toString())
        val id = (docXml \ "id").text
        val text = (docXml \ "revision" \ "text").text
        val m:Map[String, (Int, ArrayBuffer[Int])] = Map()

        text.replaceAll("(&ensp;|&emsp;|&nbsp;)", " ")
          .replaceAll("&lt;", "<")
          .replaceAll("&gt;", ">")
          .replace("&amp;", "&")
          .replaceAll("&quot;", "\"")
          .replaceAll("[^\\w-]", " ")
          .split("\\s")
          .zipWithIndex
          .foreach {
            case (word, index) => {
              if(m.contains(word)) {
                val t = m(word)
                m(word) = (t._1+1, (t._2) += index)
              } else {
                m(word) = (1, ArrayBuffer(index))
              }
            }
          }
        m.map {
          case (a, b) => {
            (a, Array( (id, b._1, b._2.toArray )))
          }
        }
      }
    }.reduceByKey {
      case (a, b) => concat(a, b)
    }.map {
      case (a, b) => (a, (b.length, b))
    }.saveAsObjectFile("hdfs://10.141.200.205:9000/home/16307130023/cleanindex")

//      .foreach {
//        case (w, (df, b)) => {
//          print(s"word:\t${w}\tdf:\t${df}\n")
//          for (i <- b) {
//            print(s"(${i._1} ${i._2}) ")
//          }
//          println()
//        }}

  }
}