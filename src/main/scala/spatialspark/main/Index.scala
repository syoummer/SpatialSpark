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

import spatialspark.index.IndexConf
import spatialspark.index.STIndex
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Simin You on 3/19/15.
 */
object Index {


  val usage = """
    create spatial index on dataset
    Usage: index --input input path
                 --geom geometry index (default 0)
                 --output output path
                 --conf configuration (dim
                 --help
              """
  def main (args: Array[String]) {
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
        case "--output" :: value :: tail =>
          nextOption(map ++ Map('output -> value), tail)
        case "--conf" :: value :: tail =>
          nextOption(map = map ++ Map('conf -> value), list = tail)
        case option :: tail => println("Unknown option "+option)
          sys.exit(1)
      }
    }
    val options = nextOption(Map(),arglist)

    val conf = new SparkConf().setAppName("Build Index")
    //.setMaster("local[4]")
    //.setSparkHome("/Users/you/spark-1.2.0")
    val sc = new SparkContext(conf)

    val inputFile = options.getOrElse('input, "").asInstanceOf[String]
    val outputFile = options.getOrElse('output, "").asInstanceOf[String]
    val methodConf = options.getOrElse('conf, "").asInstanceOf[String]

    val SEPARATOR = "\t"
    val geometryIndex = options.getOrElse('geom, 0).asInstanceOf[Int]
    val dimX = methodConf.split(":").apply(0).toInt
    val dimY = methodConf.split(":").apply(1).toInt
    val ratio = methodConf.split(":").apply(2).toDouble

    val indexConf = new IndexConf(inputFile, outputFile, SEPARATOR, geometryIndex, dimX, dimY, ratio)
    val timerBegin = System.currentTimeMillis()
    STIndex.build(sc, indexConf)
    val timerEnd = System.currentTimeMillis()
    println("index time: " + (timerEnd - timerBegin) + " ms")
  }
}
