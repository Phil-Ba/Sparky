package at.bayava.spark.io

import java.math
import java.math.MathContext

import at.bayava.spark.gen._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSpec

/**
	* Created by philba on 3/5/16.
	*/
class MyInputFormatTest extends FunSpec {

	val sc = Sparky.setupContext
	val datasize: Int = 1500000
	val testDataLocation: String = "temp/testdata.txt"

	describe("") {
		it("randomDouble") {
			val total: math.BigDecimal = RandomDoubleDataGenerator.generateData(testDataLocation, datasize)
			val rdd: RDD[(LongWritable, Text)] = Sparky.createRdd(testDataLocation, sc)

			val result = rdd.map(value => {
				try {
					new math.BigDecimal("\"reading\":(\\d+\\.?\\d*)".r.findFirstMatchIn(value._2.toString).get.group(1))
				}
				catch {
					case e: Exception => println(value + "\n" + value._2)
						throw e
				}
			}).fold(math.BigDecimal.ZERO)(_.add(_, MathContext.UNLIMITED))
			assert(total.compareTo(result)==0, s"Expected $total got $result")
		}
		it("randomInt") {
			val total: math.BigDecimal = RandomIntDataGenerator.generateData(testDataLocation, datasize)
			val rdd: RDD[(LongWritable, Text)] = Sparky.createRdd(testDataLocation, sc)

			val result = rdd.map(value => {
				try {
					new math.BigDecimal("\"reading\":(\\d+\\.?\\d*)".r.findFirstMatchIn(value._2.toString).get.group(1))
				}
				catch {
					case e: Exception => println(value + "\n" + value._2)
						throw e
				}
			}).fold(math.BigDecimal.ZERO)(_.add(_))
			assert(total.compareTo(result)==0, s"Expected $total got $result")
		}
		it("randomFloat") {
			val total: math.BigDecimal = RandomFloatDataGenerator.generateData(testDataLocation, datasize)
			val rdd: RDD[(LongWritable, Text)] = Sparky.createRdd(testDataLocation, sc)

			val result = rdd.map(value => {
				try {
					new math.BigDecimal("\"reading\":(\\d+\\.?\\d*)".r.findFirstMatchIn(value._2.toString).get.group(1))
				}
				catch {
					case e: Exception => println(value + "\n" + value._2)
						throw e
				}
			}).fold(math.BigDecimal.ZERO)(_.add(_,MathContext.UNLIMITED))
			assert(total.compareTo(result)==0, s"Expected $total got $result")
		}

		it("static") {
			val total: math.BigDecimal = StaticDataGenerator.generateData(testDataLocation, datasize)
			val rdd: RDD[(LongWritable, Text)] = Sparky.createRdd(testDataLocation, sc)

			val result = rdd.map(value => {
				try {
					new math.BigDecimal("\"reading\":(\\d+\\.?\\d*)".r.findFirstMatchIn(value._2.toString).get.group(1))
				}
				catch {
					case e: Exception => println(value + "\n" + value._2)
						throw e
				}
			}).fold(math.BigDecimal.ZERO)(_.add(_))
				assert(total.compareTo(result)==0, s"Expected $total got $result")
		}

		ignore("counti") {
			var increment = 23
			for (i <- Iterator.iterate(1)((prev) => {
				val next = prev + increment
				increment += 23
				next
			}) takeWhile (_ < 100000)) {
				val total: BigDecimal = StaticDataGenerator.generateData(testDataLocation, i)
				val rdd: RDD[(LongWritable, Text)] = Sparky.createRdd(testDataLocation, sc)

				val result = rdd.count()
				assert(i == result)
			}
		}

		it("count") {
			val total: BigDecimal = StaticDataGenerator.generateData(testDataLocation, datasize)
			val rdd: RDD[(LongWritable, Text)] = Sparky.createRdd(testDataLocation, sc)
			val result = rdd.map(_ => math.BigDecimal.ONE).fold(math.BigDecimal.ZERO)(_.add(_))
			assert(datasize == result.longValue())
		}
	}

}
