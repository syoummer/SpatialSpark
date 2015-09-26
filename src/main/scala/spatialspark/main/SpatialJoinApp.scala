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

import com.vividsolutions.jts.io.{WKBReader, WKTReader}
import com.vividsolutions.jts.geom.Geometry
import spatialspark.operator.SpatialOperator
import spatialspark.partition.bsp.BinarySplitPartitionConf
import spatialspark.partition.fgp.FixedGridPartitionConf
import spatialspark.partition.stp.SortTilePartitionConf
import spatialspark.join.{BroadcastSpatialJoin, PartitionedSpatialJoin}
import spatialspark.util.MBR
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
 * Spatial Join Application
 * Perform spatial join on two datasets, generate a list of matched pairs
 */
object SpatialJoinApp {
  // TODO:
  //  1. do parameter checking
  //  2. use custom partition file
  //  3. validate WKT string
  //  4. optimize self join (esp. for point dataset)

  val COMMA = ","
  val TAB = "\t"
  val SPACE = " "
  var SEPARATOR = "\t"

  val usage = """
    Standalone Implementation of Spatial Join on Spark
    Usage: spatialjoin --left left_data
                       --geom_left geometry index (default 0)
                       --right right_data
                       --geom_right geometry index (default 0)
                       --broadcast broadcast join or partitioned join (default:false)
                       --predicate join predicate (within, withind, contains, intersects, overlaps or nearestd)
                       --distance withind/nearestd must provide a distance (default 0)
                       --output output file location
                       --separator field separator (default TAB)
                       --partition number of partitions for input data(default 512)
                       --method partition method for distributed spatial join (fgp(default), bsp, stp)
                       --conf partitioned join configuration, separated by ':'.
                                    dimX:dimY (fgp), level:ratio (bsp), dimX:dimY:ratio (stp)
                       --extent extent of two datasets (if not specified, it will be calculated
                                on the fly) format: minX:minY:maxX:maxY
                       --num_output number of output file partitions
                       --parallel_part use parallel partition implementation (default: false)
                       --help
              """

  def main(args: Array[String]) {
    if (args.length == 0) println(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--help" :: tail =>
          println(usage)
          sys.exit(0)
        case "--left" :: value :: tail =>
          nextOption(map ++ Map('left -> value), tail)
        case "--geom_left" :: value :: tail =>
          nextOption(map ++ Map('geom_left -> value.toInt), tail)
        case "--geom_right" :: value :: tail =>
          nextOption(map ++ Map('geom_right -> value.toInt), tail)
        case "--right" :: value :: tail =>
          nextOption(map ++ Map('right -> value), tail)
        case "--broadcast" :: value :: tail =>
          nextOption(map ++ Map('broadcast -> value.toBoolean), tail)
        case "--predicate" :: value :: tail =>
          nextOption(map ++ Map('predicate -> value), tail)
        case "--distance" :: value :: tail =>
          nextOption(map ++ Map('distance -> value.toDouble), tail)
        case "--output" :: value :: tail =>
          nextOption(map ++ Map('output -> value), tail)
        case "--separator" :: value :: tail =>
          nextOption(map = map ++ Map('separator -> value), list = tail)
        case "--partition" :: value :: tail =>
          nextOption(map = map ++ Map('partition -> value.toInt), list = tail)
        case "--method" :: value :: tail =>
          nextOption(map = map ++ Map('method -> value), list = tail)
        case "--conf" :: value :: tail =>
          nextOption(map = map ++ Map('conf -> value), list = tail)
        case "--extent" :: value :: tail =>
          nextOption(map = map ++ Map('extent -> value), list = tail)
        case "--parallel_part" :: value :: tail =>
          nextOption(map = map ++ Map('parallel_part -> value.toBoolean), list = tail)
        case "--num_output" :: value :: tail =>
          nextOption(map = map ++ Map('num_output -> value.toInt), list = tail)
        case "--wkt" :: value :: tail =>
          nextOption(map = map ++ Map('wkt -> value.toBoolean), list = tail)
        case option :: tail => println("Unknown option " + option)
          sys.exit(1)
      }
    }
    val options = nextOption(Map(), arglist)
    val conf = new SparkConf().setAppName("Spatial Join App")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "spatialspark.util.KyroRegistrator")
    val sc = new SparkContext(conf)
    val outFile = options.getOrElse('output, Nil).asInstanceOf[String]
    val rightFile = options.getOrElse('right, Nil).asInstanceOf[String]
    val rightGeometryIndex = options.getOrElse('geom_right, 0).asInstanceOf[Int]
    val leftFile = options.getOrElse('left, Nil).asInstanceOf[String]
    val leftGeometryIndex = options.getOrElse('geom_left, 0).asInstanceOf[Int]
    val predicate = options.getOrElse('predicate, Nil).asInstanceOf[String]
    val joinPredicate = predicate.toLowerCase match {
      case "within" => SpatialOperator.Within
      case "withind" => SpatialOperator.WithinD
      case "contains" => SpatialOperator.Contains
      case "intersects" => SpatialOperator.Intersects
      case "overlaps" => SpatialOperator.Overlaps
      case "nearestd" => SpatialOperator.NearestD
      case _ => SpatialOperator.NA
    }
    if (joinPredicate == SpatialOperator.NA) {
      println("unsupported predicate: " + predicate)
      sys.exit(0)
    }
    val inputSeparator = options.getOrElse('separator, "tab").asInstanceOf[String].toUpperCase
    SEPARATOR = inputSeparator match {
      case "TAB" => TAB
      case "COMMA" => COMMA
      case "SPACE" => SPACE
      case _ => TAB //tab by default
    }
    val numPartitions = options.getOrElse('partition, 512).asInstanceOf[Int]
    val radius = options.getOrElse('distance, 0.0).asInstanceOf[Double]
    val broadcastJoin = options.getOrElse('broadcast, false).asInstanceOf[Boolean]
    val method = options.getOrElse('method, "fgp")
    val methodConf = options.getOrElse('conf, "").asInstanceOf[String]
    val extentString = options.getOrElse('extent, "").asInstanceOf[String]
    val numOutputPart = options.getOrElse('num_output, 0).asInstanceOf[Int]
    val paralllelPartition = options.getOrElse('parallel_part, false).asInstanceOf[Boolean]

    val beginTime = System.currentTimeMillis()

    //load left dataset
    val leftData = sc.textFile(leftFile, numPartitions).map(x => x.split(SEPARATOR)).zipWithIndex()

    val leftGeometryById = leftData.map(x => (x._2, Try(new WKTReader().read(x._1.apply(leftGeometryIndex)))))
      .filter(_._2.isSuccess).map(x => (x._1, x._2.get))

    //load right dataset
    val rightData = sc.textFile(rightFile, numPartitions).map(x => x.split(SEPARATOR)).zipWithIndex()

    val rightGeometryById = rightData.map(x => (x._2, Try(new WKTReader().read(x._1.apply(rightGeometryIndex)))))
      .filter(_._2.isSuccess).map(x => (x._1, x._2.get))

    //join processing
    var matchedPairs: RDD[(Long, Long)] = sc.emptyRDD
    if (broadcastJoin)
      matchedPairs = BroadcastSpatialJoin(sc, leftGeometryById, rightGeometryById, joinPredicate, radius)
    else {
      //get extent that covers both datasets
      val extent = extentString match {
        case "" =>
          val temp = leftGeometryById.map(x => x._2.getEnvelopeInternal)
            .map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
            .reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))
          val temp2 = rightGeometryById.map(x => x._2.getEnvelopeInternal)
            .map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
            .reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))
          (temp._1 min temp2._1, temp._2 min temp2._2, temp._3 max temp2._3, temp._4 max temp2._4)
        case _ => (extentString.split(":").apply(0).toDouble, extentString.split(":").apply(1).toDouble,
          extentString.split(":").apply(2).toDouble, extentString.split(":").apply(3).toDouble)
      }

      val partConf = method match {
        case "stp" =>
          val dimX = methodConf.split(":").apply(0).toInt
          val dimY = methodConf.split(":").apply(1).toInt
          val ratio = methodConf.split(":").apply(2).toDouble
          new SortTilePartitionConf(dimX, dimY, new MBR(extent._1, extent._2, extent._3, extent._4), ratio, paralllelPartition)
        case "bsp" =>
          val level = methodConf.split(":").apply(0).toLong
          val ratio = methodConf.split(":").apply(1).toDouble
          new BinarySplitPartitionConf(ratio, new MBR(extent._1, extent._2, extent._3, extent._4), level, paralllelPartition)
        case _ =>
          val dimX = methodConf.split(":").apply(0).toInt
          val dimY = methodConf.split(":").apply(1).toInt
          new FixedGridPartitionConf(dimX, dimY, new MBR(extent._1, extent._2, extent._3, extent._4))
      }
      matchedPairs = PartitionedSpatialJoin(sc, leftGeometryById, rightGeometryById, joinPredicate, radius, partConf)
    }

    println(matchedPairs.count())
    val runtime = System.currentTimeMillis() - beginTime
    println("join time: " + runtime)

    //write back results
    if (numOutputPart == 0)
      matchedPairs.map(x => x._1 + SEPARATOR + x._2).saveAsTextFile(outFile)
    else
      matchedPairs.repartition(numOutputPart).map(x => x._1 + SEPARATOR + x._2).saveAsTextFile(outFile)

    //post-processing for gathering data using hashing join
  }
}
