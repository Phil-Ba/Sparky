package at.bayava.spark.io

import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

/**
 * Created by pbayer.
 */
class MyRecordReader extends RecordReader[LongWritable, Text] {
  val readString = new StringBuilder()
  var value: Text = _
  var key: LongWritable = new LongWritable()
  var maxLineLength: Int = 2048
  var start: Long = _
  var end: Long = _
  var pos: Long = _
  var fileIn: FSDataInputStream = _

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val inputSplit: FileSplit = split.asInstanceOf[FileSplit]
    val config: Configuration = context.getConfiguration
    //    maxLineLength = config.getInt("mapred.linerecordreader.maxlength", Int.MaxValue)
    start = inputSplit.getStart
    end = start + inputSplit.getLength
    //    maxLineLength = (inputSplit.getLength / 3).toInt
    val path: Path = inputSplit.getPath
    val fs: FileSystem = path.getFileSystem(config)
    fileIn = fs.open(path)
        pos = start
    //        pos = if (start == 0) start else start -1
    // If Split "S" starts at byte 0, first line will be processed
    // If Split "S" does not start at byte 0, first line has been already
    // processed by "S-1" and therefore needs to be silently ignored
    //    val skipFirstLine = start != 0
    //    if (!skipFirstLine) {
    // Set the file pointer at "start - 1" position.
    // This is to make sure we won't miss any line
    // It could happen if "start" is located on a EOL
    //      start -= 1
    //      fileIn.seek(start)
    //    }


    // If first line needs to be skipped, read first line
    // and stores its content to a dummy Text
    //    if (skipFirstLine) {
    //      val dummy = new Text()
    // Reset "start" to "start + line offset"
    //      start += in.readLine(dummy, 0,
    //        Math.min(Int.MaxValue,
    //          end - start).toInt)
    //    }

    // Position is the actual start

  }

  override def getProgress: Float = if (start == end) 0.0f else Math.min(1.0f, (pos - start) / (end - start))

  override def nextKeyValue(): Boolean = {
    //    println(s"start:$start end:$end pos:$pos")
    if (pos > end) {
      return false
    }
    var startIndex: Int = readString.indexOf("{")
    if (startIndex == -1) {
      startIndex = readIntoBufferWhileNotFound("{")
    }
    if (startIndex == -1 || pos >= end) {
      return false
    }
    readString.delete(0, startIndex + 1)
    startIndex = 0
    var endIndex: Int = readString.indexOf("}")
    if (endIndex == -1) {
      endIndex = readIntoBufferWhileNotFound("}")
    }
    if (endIndex == -1) {
      return false
    }

    key.set(pos + startIndex)
    //    println("Key: " + key.get())
    value = new Text(readString.substring(startIndex, endIndex))
    readString.delete(0, endIndex + 1)
    //        println("value found: " + value)
    //        println("value found => trimming buffer: " + readString.mkString)
    true
  }

  def readIntoBufferWhileNotFound(stringToFind: String): Int = {
    val x = new Array[Byte](maxLineLength)
    var bytesRead = fileIn.read(pos, x, 0, maxLineLength)

    pos += bytesRead

    readString.append(new String(x, StandardCharsets.UTF_8))
    val index = readString.indexOf(stringToFind)
    if (index != -1) {
      index
    } else if (bytesRead == -1) {
      -1
    }
    else {
      readIntoBufferWhileNotFound(stringToFind)
    }
  }

  override def getCurrentValue: Text = value

  override def getCurrentKey: LongWritable = key

  override def close(): Unit = fileIn.close()

}
