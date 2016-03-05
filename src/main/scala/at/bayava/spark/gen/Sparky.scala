package at.bayava.spark.gen

import at.bayava.spark.io.MyInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by pbayer.
 */
object Sparky {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:/dev/winutils")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[8]")
      .set("spark.executor.memory", "4g").set("spark.storage.memoryFraction", "0.1")
    val sc = new SparkContext(conf)

    //    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "},{")
    val logData = sc.newAPIHadoopFile("/temp/test2.txt", classOf[MyInputFormat], classOf[LongWritable], classOf[Text])
    //    val logData = sc.textFile("/temp/test.txt",16)
    //    val logData = sc.newAPIHadoopFile("/temp/test.txt",FileInputFormat,String,String)
    //    println(logData.count())
    //    println(logData.filter( kv => !kv._2.toString.contains("\"id\":") ).collect().foreach(x => println(x)))

    println(logData.count())
    //    val result: Array[(String, Long)] = logData.map(x => {
    //      ("\"id\":(\\d+)".r.findFirstMatchIn(x._2.toString).get.group(1), 1L)
    //    }).reduceByKey(_ + _).filter(in => in._2 > 1).collect()
    val result = logData.map(x => {
      try {
        BigDecimal("\"reading\":([0-9\\.]+)".r.findFirstMatchIn(x._2.toString).get.group(1).toDouble)
      }
      catch {
        case e: Exception => println(x + "\n" + x._2); throw e
      }
    }).reduce(_ + _)
    println(result.toString())
    //    println(result.length)
    //    for (x <- result) {
    //      println("id: " + x._1)
    //      println("count: " + x._2)
    //    }
    //    logData.foreach( x=> println(x._2))
    //    val numAs = logData.filter(line => line.contains("a")).count()
    //    val numBs = logData.filter(line => line.contains("b")).count()
    //    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }


}
