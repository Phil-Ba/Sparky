package at.bayava.spark.io

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

/**
 * Created by pbayer.
 */
class MyInputFormat extends FileInputFormat[LongWritable, Text] {


  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    new MyRecordReader()
  }

}
