/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.utah.cs.simba.examples

import edu.utah.cs.simba.SimbaContext
import edu.utah.cs.simba.index.RTreeType
import edu.utah.cs.simba.util.DateTime
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by dongx on 11/14/2016.
  */
object TestMain {
  case class PointData(id: String, x: Double, y: Double, t: Double)

  val eachQueryLoopTimes = 5

  val id = "538004086"
  val startTime = "2019-02-01T00:00:00"
  val tqueries = List("2019-02-01T00:59:59", "2019-02-01T05:59:59", "2019-02-01T23:59:59", "2019-02-07T23:59:59", "2019-02-28T23:59:59")
  val queries = List(
    (-118.28, -118.23, 33.68, 33.73),
    (-118.30, -118.20, 33.66, 33.76),
    (-118.35, -118.15, 33.61, 33.81),
    (-118.45, -118.05, 33.51, 33.91),
    (-118.50, -118.00, 33.46, 33.96)
  )

  def main(args: Array[String]): Unit = {
    var PointRDDNumPartitions = 5
    var PointRDDInputLocation = "file:///Users/tianwei/Projects/data/ais_small.csv"

    if (args.length > 0) {
      println(args.mkString(", "))
      if (args.length == 1) {
        PointRDDInputLocation = args(0)
      } else if (args.length == 2) {
        PointRDDInputLocation = args(0)
        PointRDDNumPartitions = args(1).toInt
      } else {

      }
    }

    val sparkConf = new SparkConf().setAppName("SimbaAISExperiment")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)
    val simbaContext = new SimbaContext(sc)

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    var rightData = ListBuffer[PointData]()

    import simbaContext.implicits._
    import simbaContext.SimbaImplicits._

    println(s"Input Data File: $PointRDDInputLocation, $PointRDDNumPartitions")

    var start = System.currentTimeMillis()
    val pointRDD = sc.textFile(PointRDDInputLocation, PointRDDNumPartitions)
    val leftDF = pointRDD.map(line => {
      val items = line.split(",")
      PointData(items(0), items(3).toDouble, items(2).toDouble, items(1).toLong * 1000)
    }).toDF()
    leftDF.persist(StorageLevel.MEMORY_ONLY)
    println(s"<Simba> Generate DataFrame, use time ${System.currentTimeMillis() - start}ms")


    val rightDF = sc.parallelize(rightData).toDF

    start = System.currentTimeMillis()
    leftDF.registerTempTable("point1")
    println(s"<Simba> Table Register, use time ${System.currentTimeMillis() - start}ms")

    testIDTQuery(simbaContext)
    testSRQuery(simbaContext)
    testSTQuery(simbaContext)

//    simbaContext.indexTable("point1", RTreeType, "rt", Array("x", "y"))
//
//    val df = simbaContext.sql("SELECT * FROM point1 WHERE x <= -118.23 and x >= -118.28 and y >= 33.68 and y <= 33.73 and id == '538004086'")//.collect().foreach(println)
//    df.explain(true)
//    df.show()

    /*leftDF.index(RTreeType, "rt", Array("x", "y"))

    val df = leftDF.knn(Array("x", "y"), Array(10.0, 10), 3)
    println(df.queryExecution)
    df.show()

    leftDF.range(Array("x", "y"), Array(-118.28, -118.23), Array(33.68, 33.73)).show()

    leftDF.knnJoin(rightDF, Array("x", "y"), Array("x", "y"), 3).show(100)*/

    sc.stop()
    println("All Simba finished!")
  }

  def testIDTQuery(simbaContext: SimbaContext): Unit = {
    println("=========Test ID-Temporal Query=========")
    var start = System.currentTimeMillis()
    simbaContext.indexTable("point1", RTreeType, "idt", Array("t"))
    println(s"<Simba> Build IDT Index, use time ${System.currentTimeMillis() - start}ms")
    println(simbaContext.indexManager.getIndexInfo.mkString(","))

    tqueries.foreach(query => {
      println(query)
      val sql = "SELECT * FROM point1 WHERE t>=" + DateTime.format2Stamp(startTime) + " and t<=" + DateTime.format2Stamp(query) + " and id='" + id + "'"
      println(s"ID-Temporal Range: $sql")
      for (i <- 1 to eachQueryLoopTimes) {
        start = System.currentTimeMillis()
        val result = simbaContext.sql(sql).collect()
        println(s"<Simba> ID-Temporal Query finished, use time ${System.currentTimeMillis() - start}ms, ${result.length}")
      }
    })
    simbaContext.clearIndex()
  }

  def testSRQuery(simbaContext: SimbaContext): Unit = {
    println("=========Test Spatial Range Query=========")
    var start = System.currentTimeMillis()
    simbaContext.indexTable("point1", RTreeType, "rt", Array("x", "y"))
    println(s"<Simba> Build RT Index, use time ${System.currentTimeMillis() - start}ms")
    println(simbaContext.indexManager.getIndexInfo.mkString(","))

    queries.foreach(query => {
      println(query)
      val sql = "SELECT * FROM point1 WHERE x>=" + query._1 + " and x<=" + query._2 + " and y>=" + query._3 + " and y<=" + query._4
      println(s"Spatial Range: $sql")
      for (i <- 1 to eachQueryLoopTimes) {
        start = System.currentTimeMillis()
        val result = simbaContext.sql(sql).collect()
        println(s"<Simba> Spatial Range Query finished, use time ${System.currentTimeMillis() - start}ms, ${result.length}")
      }
    })
    simbaContext.clearIndex()
  }

  def testSTQuery(simbaContext: SimbaContext): Unit = {
    println("=========Test Spatial-Temporal Range Query=========")
    var start = System.currentTimeMillis()
    simbaContext.indexTable("point1", RTreeType, "st", Array("x", "y", "t"))
    println(s"<Simba> Build ST Index, use time ${System.currentTimeMillis() - start}ms")
    println(simbaContext.indexManager.getIndexInfo.mkString(","))

    tqueries.foreach(tquery => {
      queries.foreach(query => {
        println(tquery, query)
        val sql = "SELECT * FROM point1 WHERE x>=" + query._1 + " and x<=" + query._2 + " and y>=" + query._3 + " and y<=" + query._4 + " and t>=" + DateTime.format2Stamp(startTime) + " and t<=" + DateTime.format2Stamp(tquery)
        println(s"Spatial Range: $sql")
        for (i <- 1 to eachQueryLoopTimes) {
          start = System.currentTimeMillis()
          val result = simbaContext.sql(sql).collect()
          println(s"<Simba> Spatial-Temporal Range Query finished, use time ${System.currentTimeMillis() - start}ms, ${result.length}")
        }
      })
    })

    simbaContext.clearIndex()
  }
}
