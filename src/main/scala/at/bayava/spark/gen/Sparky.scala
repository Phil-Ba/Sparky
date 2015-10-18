package at.bayava.spark.gen

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by pbayer.
 */
object Sparky {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:/dev/winutils")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[16]")
      .set("spark.executor.memory", "4g").set("spark.storage.memoryFraction", "0.1")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "},{")
    val logData = sc.textFile("/temp/test.txt",16)
//    val logData = sc.newAPIHadoopFile("/temp/test.txt",FileInputFormat,String,String)
//    println(logData.count())
    println(logData.filter(line => line.contains("\"id\":4668,")).collect().foreach(x=>println(x)))
    //    val numAs = logData.filter(line => line.contains("a")).count()
    //    val numBs = logData.filter(line => line.contains("b")).count()
    //    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }


}
