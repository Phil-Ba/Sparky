package at.bayava.spark.gen

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.util.LineReader

/**
 * Created by pbayer.
 */
class JsonObjectRecordReader extends RecordReader[LongWritable, Text] {
  var value: Text = value

  var key: LongWritable = key

  var maxLineLength: Int = _

  var start: Long = _

  var end: Long = _

  var in: LineReader = _

  var pos: Long = pos

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val inputSplit: FileSplit = split.asInstanceOf[FileSplit]
    val config: Configuration = context.getConfiguration
    maxLineLength = config.getInt("mapred.linerecordreader.maxlength", Int.MaxValue)
    start = inputSplit.getStart
    end = start + inputSplit.getLength
    val path: Path = inputSplit.getPath
    val fs: FileSystem = path.getFileSystem(config)
    val fileIn: FSDataInputStream = fs.open(path)
    // If Split "S" starts at byte 0, first line will be processed
    // If Split "S" does not start at byte 0, first line has been already
    // processed by "S-1" and therefore needs to be silently ignored
    val skipFirstLine = start != 0
    if (!skipFirstLine) {
      // Set the file pointer at "start - 1" position.
      // This is to make sure we won't miss any line
      // It could happen if "start" is located on a EOL
      start -= 1
      fileIn.seek(start)
    }

    in = new LineReader(fileIn, config)

    // If first line needs to be skipped, read first line
    // and stores its content to a dummy Text
    if (skipFirstLine) {
      val dummy = new Text()
      // Reset "start" to "start + line offset"
      start += in.readLine(dummy, 0,
        Math.min(Int.MaxValue,
          end - start).toInt)
    }

    // Position is the actual start
    pos = start

  }

  override def getProgress: Float = if (start == end) 0.0f else Math.min(1.0f, (pos - start) / (end - start))


  override def nextKeyValue(): Boolean = {
    // Current offset is the key
    key.set(pos)

    var bytesRead = 0


    def readWhileEndNotReached() {
      bytesRead = in.readLine(value, maxLineLength,
        Math.max(Math.min(
          Int.MaxValue, end - pos),
          maxLineLength).toInt)
      // No byte read, seems that we reached end of Split
      // Break and return false (no key / value)
      if (bytesRead == 0) {
        return
      }
      // Line is read, new position is set
      pos += bytesRead

      // Line is lower than Maximum record line size
      // break and return true (found key / value)
      if (bytesRead < maxLineLength) {
        return
      }
      readWhileEndNotReached()
    }
    readWhileEndNotReached()

    if (bytesRead == 0) {
      // We've reached end of Split
      key = null
      value = null
      false
    } else {
      // Tell Hadoop a new line has been found
      // key / value will be retrieved by
      // getCurrentKey getCurrentValue methods
      true
    }
  }

  override def getCurrentValue: Text = value

  override def getCurrentKey: LongWritable = key

  override def close(): Unit = in.close()
}
