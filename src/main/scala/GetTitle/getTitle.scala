package GetTitle

import com.databricks.spark.xml.XmlInputFormat

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.XML

object getTitle {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("GetTitle")
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
    ).map {
      case doc => {
        val docXml = XML.loadString(doc._2.toString())
        val id = (docXml \ "id").text
        val title = (docXml \ "title").text
        (id, title)
      }
    }.saveAsTextFile("hdfs://10.141.200.205:9000/home/16307130023/title")
  }
}
