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

import com.vividsolutions.jts.io.WKTReader
import spatialspark.partition.bsp.BinarySplitPartition
import spatialspark.partition.fgp.FixedGridPartition
import spatialspark.partition.PartitionMethod
import spatialspark.partition.stp.SortTilePartition
import spatialspark.util.MBR
import org.apache.spark.{SparkContext, SparkConf}

object Partitioner {
  val usage = """
    Standalone Implementation of Spatial Partition on Spark
    Usage: partitioner --input input
                       --geom geometry index for input data (default 0)
                       --output output
                       --separator field separator (default TAB)
                       --wkt  output as wkt (default true)
                       --method partition method for distributed spatial join (fgp(default), bsp, stp)
                       --conf partitioned join configuration, separated by ':'.
                                    dimX:dimY (fgp), level:ratio (bsp), dimX:dimY:ratio (stp)
                       --extent the extent of space (if not specified, the extent will be generated via full scan)
                                    minX:minY:maxX:maxY
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
        case "--input" :: value :: tail =>
          nextOption(map ++ Map('input -> value), tail)
        case "--geom" :: value :: tail =>
          nextOption(map ++ Map('geom -> value.toInt), tail)
        case "--output" :: value :: tail =>
          nextOption(map ++ Map('output -> value), tail)
        case "--wkt" :: value :: tail =>
          nextOption(map ++ Map('wkt -> value.toBoolean), tail)
        case "--method" :: value :: tail =>
          nextOption(map ++ Map('method -> value), tail)
        case "--separator" :: value :: tail =>
          nextOption(map ++ Map('separator -> value), tail)
        case "--conf" :: value :: tail =>
          nextOption(map ++ Map('conf -> value), tail)
        case "--extent" :: value :: tail =>
          nextOption(map = map ++ Map('extent -> value), list = tail)
        case "--parallel" :: value :: tail =>
          nextOption(map = map ++ Map('parallel -> value.toBoolean), list = tail)
        case option :: tail => println("Unknown option " + option)
          sys.exit(1)
      }
    }
    val options = nextOption(Map(), arglist)
    val conf = new SparkConf().setAppName("Spatial Partition")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "spatialspark.util.KyroRegistrator")
    val inputFile = options.getOrElse('input, "").asInstanceOf[String]
    val outputFile = options.getOrElse('output, "").asInstanceOf[String]
    val isWkt = options.getOrElse('wkt, true).asInstanceOf[Boolean]
    val separator = options.getOrElse('separator, "\t").asInstanceOf[String]
    val partitionConf = options.getOrElse('conf, "").asInstanceOf[String]
    var partitions: Array[MBR] = Array.empty
    val partitionMethodOpt = options.getOrElse('method, "fgp").asInstanceOf[String]
    val partitionMethod = partitionMethodOpt match {
      case "fgp" => PartitionMethod.FGP
      case "bsp" => PartitionMethod.BSP
      case "stp" => PartitionMethod.STP
      case _ => PartitionMethod.FGP //default
    }
    val geomIdx = options.getOrElse('geom, 0).asInstanceOf[Int]
    val extentString = options.getOrElse('extent, "").asInstanceOf[String]
    val parallel = options.getOrElse('parallel, true).asInstanceOf[Boolean]

    val sc = new SparkContext(conf)
    val inputMBRs = sc.textFile(inputFile).map(x => (new WKTReader).read(x.split(separator).apply(geomIdx)))
      .map(x => x.getEnvelopeInternal).map(x => new MBR(x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
    val extent: MBR = if (extentString == "")
      inputMBRs.reduce(_ union _)
    else new MBR(extentString.split(":").apply(0).toDouble, extentString.split(":").apply(1).toDouble,
      extentString.split(":").apply(2).toDouble, extentString.split(":").apply(3).toDouble)

    if (partitionMethod == PartitionMethod.FGP) {
      // grid partition
      val dimX = partitionConf.split(":").apply(0).toInt
      val dimY = partitionConf.split(":").apply(1).toInt
      val FGPartition = FixedGridPartition(sc, extent, dimX, dimY)
      partitions = FGPartition
    } else if (partitionMethod == PartitionMethod.STP) {
      // ST partition
      val dimX = partitionConf.split(":").apply(0).toInt
      val dimY = partitionConf.split(":").apply(1).toInt
      val ratio = partitionConf.split(":").apply(2).toDouble
      val STPartition = SortTilePartition(sc, inputMBRs.sample(withReplacement = false, fraction = ratio), extent, dimX, dimY, parallel)
      partitions = STPartition
    } else if (partitionMethod == PartitionMethod.BSP) {
      val level = partitionConf.split(":").apply(0).toInt
      val ratio = partitionConf.split(":").apply(1).toDouble
      val BSPartition = BinarySplitPartition(sc, inputMBRs.sample(withReplacement = false, fraction = ratio), extent, level, parallel)
      partitions = BSPartition
    } else {
      //Not support..
    }

    val output = sc.parallelize(partitions, 1).zipWithIndex()
      .map(x => x._2 + "\t" + x._1.toText).saveAsTextFile(outputFile)
  }

}
