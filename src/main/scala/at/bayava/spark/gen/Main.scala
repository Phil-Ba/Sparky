package at.bayava.spark.gen

import java.io.{File, FileOutputStream}

import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator}
import com.fasterxml.jackson.databind.ObjectMapper
import org.fluttercode.datafactory.impl.DataFactory
import org.joda.time.{LocalDate, LocalDateTime}

import scala.util.Random

/**
	* Created by pbayer.
	*/
object Main {
	val df: DataFactory = new DataFactory
	val start = new LocalDate(2013, 9, 9).toDate

	def main(args: Array[String]) {
		val file: File = new File("/temp/test2.txt")
		val mapper: ObjectMapper = new ObjectMapper()
		val factory: JsonFactory = new JsonFactory()
		val fos: FileOutputStream = new FileOutputStream(file)
		val gen: JsonGenerator = factory.createGenerator(fos)
		//    gen.useDefaultPrettyPrinter()
		var sum: BigDecimal = 0
		gen.writeStartArray()
		for (i <- 1 to 10000000) {
			//      for (i <- 1 to 14){
			gen.writeStartObject()
			gen.writeNumberField("id", df.getNumberBetween(1, 100000))
			gen.writeStringField("tmstmp", modifyTime)
			val temp: Double = Random.nextDouble()
			sum += temp.toDouble
			//      gen.writeNumberField("reading", temp)
			gen.writeNumberField("reading", 1)
			gen.writeEndObject()
		}
		gen.writeEndArray()
		gen.writeRaw(sum.toString())
		gen.close()
	}

	private def modifyTime: String = {
		val originalDate = LocalDateTime.fromDateFields(df.getDate(start, -356, 356))
		originalDate.plusHours(df.getNumberBetween(-23, 23)).plusMinutes(df.getNumberBetween(-60, 60))
			.plusSeconds(df.getNumberBetween(-60, 60)).toString("yyyy-MM-dd HH:mm:ss")
	}

}
