/*
 * Copyright 2015 Simin You
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package spatialspark.main

import spatialspark.index.STIndex
import spatialspark.util.MBR
import com.vividsolutions.jts.geom.{Envelope, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import spatialspark.operator.SpatialOperator
import spatialspark.query.RangeQuery
import org.apache.spark.{SparkContext, SparkConf}

/*
 * Created by Simin You on 11/4/14.
 */
object Query {
  val usage = """
    Standalone Implementation of Spatial Range Query on Spark
    Usage: rangequery  --input input
                       --geom geometry index for input data (default 0)
                       --query query window in format of xmin,ymin,xmax,ymax
                       --separator field separator (default TAB)
                       --predicate supported predicated include (within, withind, contains)
                       --distance distance used in the predicate (e.g., withind)
                       --output output location
                       --help
              """

  def main(args: Array[String]) {
    if (args.length == 0) println(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "--help" :: tail =>
          println(usage)
          sys.exit(0)
        case "--input" :: value :: tail =>
          nextOption(map ++ Map('input -> value), tail)
        case "--geom" :: value :: tail =>
          nextOption(map ++ Map('geom -> value.toInt), tail)
        case "--query" :: value :: tail =>
          nextOption(map ++ Map('query -> value), tail)
        case "--distance" :: value :: tail =>
          nextOption(map ++ Map('distance -> value.toDouble), tail)
        case "--separator" :: value :: tail =>
          nextOption(map ++ Map('separator -> value), tail)
        case "--output" :: value :: tail =>
          nextOption(map = map ++ Map('output -> value), list = tail)
        case "--predicate" :: value :: tail =>
          nextOption(map = map ++ Map('predicate -> value), list = tail)
        case "--use_index" :: value :: tail =>
          nextOption(map = map ++ Map('index -> value.toBoolean), list = tail)
        case option :: tail => println("Unknown option "+option)
          sys.exit(1)
      }
    }
    val options = nextOption(Map(),arglist)
    val conf = new SparkConf().setAppName("Spatial Query")
    //.setMaster("local[4]")
    //.setSparkHome("/Users/you/spark-1.1.0")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "spatialspark.util.KyroRegistrator")
    val inputFile = options.getOrElse('input, "").asInstanceOf[String]
    val outputFile = options.getOrElse('output, "").asInstanceOf[String]
    val query = options.getOrElse('query, "").asInstanceOf[String]
    val q = query.split(",").map(_.toDouble)
    //val queryGeometry = (new WKTReader).read(query)
    val queryGeometry = (new GeometryFactory()).toGeometry(new Envelope(q(0), q(2), q(1), q(3)))
    val geomIdx = options.getOrElse('geom, 0).asInstanceOf[Int]
    val radius = options.getOrElse('distance, 0).asInstanceOf[Double]
    val separator = options.getOrElse('separator, "\t").asInstanceOf[String]
    val predicate = options.getOrElse('predicate, "").asInstanceOf[String]
    val useIndex = options.getOrElse('index, false).asInstanceOf[Boolean]
    val joinPredicate = predicate.toLowerCase() match{
      case "within" => SpatialOperator.Within
      case "withind" => SpatialOperator.WithinD
      case "contains" => SpatialOperator.Contains
      case "nearestd" => SpatialOperator.NearestD
      case "intersects" => SpatialOperator.Intersects
      case _ => SpatialOperator.NA
    }
    if (joinPredicate == SpatialOperator.NA || joinPredicate == SpatialOperator.NearestD) {
      println("unsupported predicate: " + predicate)
      sys.exit(0)
    }

    val timerBegin = System.currentTimeMillis()
    val sc = new SparkContext(conf)
    if (useIndex == false) {
      val inputData = sc.textFile(inputFile).map(x => (new WKTReader).read(x.split(separator).apply(geomIdx)))
      val inputDataWithId = inputData.zipWithIndex().map(_.swap)
      val results = RangeQuery(sc, inputDataWithId, queryGeometry, joinPredicate, radius)
      results.saveAsTextFile(outputFile)
    }
    else {
      val queryEnv = queryGeometry.getEnvelopeInternal
      val q = new MBR(queryEnv.getMinX, queryEnv.getMinY, queryEnv.getMaxX, queryEnv.getMaxY)
      val results = STIndex.query(sc, inputFile, geomIdx, q)
      results.saveAsTextFile(outputFile)
      //println("results: " + results.count)
    }
    val timerEnd = System.currentTimeMillis()
    println("query time: " + (timerEnd - timerBegin) + " ms")

  }
}
