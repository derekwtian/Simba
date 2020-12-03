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
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by dongx on 11/14/2016.
  */
object TestMain {
  case class PointData(id: String, x: Double, y: Double, t: Double)

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

    val sparkConf = new SparkConf().setAppName("SpatialOperationExample")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)
    val simbaContext = new SimbaContext(sc)

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //var leftData = ListBuffer[PointData]()
    var rightData = ListBuffer[PointData]()

    import simbaContext.implicits._
    import simbaContext.SimbaImplicits._

    println(s"Input Data File: $PointRDDInputLocation, $PointRDDNumPartitions")
    val pointRDD = sc.textFile(PointRDDInputLocation, PointRDDNumPartitions)
    println(pointRDD.count())
    val leftData = pointRDD.map(line => {
      val items = line.split(",")
      PointData(items(0), items(3).toDouble, items(2).toDouble, items(1).toDouble)
    })
    println(leftData.count())


    val leftDF = leftData.toDF
    val rightDF = sc.parallelize(rightData).toDF

    leftDF.registerTempTable("point1")
    //simbaContext.indexTable("point1", RTreeType, "rt1", Array("x","y"))
    simbaContext.indexTable("point1", RTreeType, "rt2", Array("t"))
    //val indexInfo = simbaContext.indexManager.getIndexInfo(0).


    val df = simbaContext.sql("SELECT * FROM point1 WHERE x <= -118.23 and x >= -118.28 and y >= 33.68 and y <= 33.73 and t  <=1549033000 and t>=1549022168")//.collect().foreach(println)
    df.explain(true)
    df.show()

//    simbaContext.indexTable("point1", RTreeType, "rt", List("x", "y"))
    //leftDF.index(RTreeType, "rt", Array("x", "y"))

//    val df = leftDF.knn(Array("x", "y"), Array(10.0, 10), 3)
//    println(df.queryExecution)
//    df.show()

    //leftDF.range(Array("x", "y"), Array(-118.28, -118.23), Array(33.68, 33.73)).show()
//
//    leftDF.knnJoin(rightDF, Array("x", "y"), Array("x", "y"), 3).show(100)

    sc.stop()
  }
}
