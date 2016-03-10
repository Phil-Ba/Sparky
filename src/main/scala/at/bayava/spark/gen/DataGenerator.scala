package at.bayava.spark.gen

import java.io.{File, FileOutputStream}
import java.math
import java.math.MathContext
import java.util.Date

import com.fasterxml.jackson.core.JsonGenerator.Feature
import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator}
import org.fluttercode.datafactory.impl.DataFactory
import org.joda.time.{LocalDate, LocalDateTime}

import scala.util.Random

trait DataGenerator[T <: math.BigDecimal] {

	val df: DataFactory
	val start: Date

	def generateReadingValue: T

	val mc: MathContext = MathContext.UNLIMITED

	def generateData(dataFileLocation: String, count: Int = 10000000): math.BigDecimal = {
		val file: File = new File(dataFileLocation)
		val factory: JsonFactory = new JsonFactory()
		val fos: FileOutputStream = new FileOutputStream(file)
		factory.enable(Feature.WRITE_BIGDECIMAL_AS_PLAIN)
		val gen: JsonGenerator = factory.createGenerator(fos)
		var sum: math.BigDecimal = math.BigDecimal.ZERO
		gen.writeStartArray()
		for (i <- 1 to count) {
			gen.writeStartObject()
			gen.writeNumberField("id", df.getNumberBetween(0, count))
			gen.writeStringField("tmstmp", modifyTime)
			val temp = generateReadingValue
			sum = sum.add(temp, mc)
			gen.writeNumberField("reading", temp)
			gen.writeEndObject()
		}
		gen.writeEndArray()
		gen.flush()
		gen.close()
		fos.flush()
		fos.close()
		println("sum: " + sum)
		sum
	}

	private def modifyTime: String = {
		val originalDate = LocalDateTime.fromDateFields(df.getDate(start, -356, 356))
		originalDate.plusHours(df.getNumberBetween(-23, 23)).plusMinutes(df.getNumberBetween(-60, 60))
			.plusSeconds(df.getNumberBetween(-60, 60)).toString("yyyy-MM-dd HH:mm:ss")
	}

}

/**
	* Created by pbayer.
	*/
object StaticDataGenerator extends DataGenerator[math.BigDecimal] {
	val df: DataFactory = new DataFactory
	val start = new LocalDate(2013, 9, 9).toDate

	override def generateReadingValue: math.BigDecimal = math.BigDecimal.ONE
}

object RandomDoubleDataGenerator extends DataGenerator[math.BigDecimal] {
	val df: DataFactory = new DataFactory
	val start = new LocalDate(2013, 9, 9).toDate

	override def generateReadingValue: math.BigDecimal = math.BigDecimal.valueOf(Random.nextDouble())
}

object RandomFloatDataGenerator extends DataGenerator[math.BigDecimal] {
	val df: DataFactory = new DataFactory
	val start = new LocalDate(2013, 9, 9).toDate

	override def generateReadingValue: math.BigDecimal = math.BigDecimal.valueOf(Random.nextFloat())
}

object RandomIntDataGenerator extends DataGenerator[math.BigDecimal] {
	val df: DataFactory = new DataFactory
	val start = new LocalDate(2013, 9, 9).toDate

	override def generateReadingValue: math.BigDecimal = math.BigDecimal.valueOf(Random.nextInt(10000))
}
