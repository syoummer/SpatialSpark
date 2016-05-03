/*
 *  Copyright $year Simin You
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 *
 */

package spatialspark.exp

import com.vividsolutions.jts.io.{WKBReader, WKTReader}
import org.apache.spark.sql.Row
import spatialspark.index.serial.RTree
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Simin You on 7/27/15.
 */
object Join {

  //TODO: refactor this...
  type MBR = (Double, Double, Double, Double)

  def intersect(a:MBR, b:MBR):Boolean = {
    !(a._1 > b._3 || a._3 < b._1 || a._2 > b._4 || a._4 < b._2)
  }
  val usage = """
    Standalone Implementation of Spatial Join on Spark
    Usage: rangequery  --left left input
                       --geom_left geometry index for input data (default 0)
                       --right right input
                       --geom_right geometry index for input data (default 0)
                       --separator field separator (default TAB)
                       --output output location
                       --use_index
                       --help
              """


  case class Pair2(lid:Long, rid:Long, pid:Long)
  case class Pair(lid:Long, rid:Long)
  case class Geom(rid:Long, geom:Array[Byte])
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
        case "--left" :: value :: tail =>
          nextOption(map ++ Map('left -> value), tail)
        case "--geom_left" :: value :: tail =>
          nextOption(map ++ Map('geom_left -> value.toInt), tail)
        case "--right" :: value :: tail =>
          nextOption(map ++ Map('right -> value), tail)
        case "--geom_right" :: value :: tail =>
          nextOption(map ++ Map('geom_right -> value.toInt), tail)
        case "--separator" :: value :: tail =>
          nextOption(map ++ Map('separator -> value), tail)
        case "--output" :: value :: tail =>
          nextOption(map = map ++ Map('output -> value), list = tail)
        case "--use_index" :: value :: tail =>
          nextOption(map = map ++ Map('index -> value.toBoolean), list = tail)
        case option :: tail => println("Unknown option "+option)
          sys.exit(1)
      }
    }
    val options = nextOption(Map(),arglist)
    val conf = new SparkConf().setAppName("Spatial Join")
    //.setMaster("local[4]")
    //.setSparkHome("/Users/you/spark-1.4.1")
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrator", "me.simin.spark.spatial.util.KyroRegistrator")
    val leftFile = options.getOrElse('left, "").asInstanceOf[String]
    val rightFile = options.getOrElse('right, "").asInstanceOf[String]
    val outFile = options.getOrElse('output, "").asInstanceOf[String]

    val leftGeomIdx = options.getOrElse('geom_left, 0).asInstanceOf[Int]
    val rightGeomIdx = options.getOrElse('geom_right, 0).asInstanceOf[Int]

    val separator = options.getOrElse('separator, "\t").asInstanceOf[String]
    val useIndex = options.getOrElse('index, false).asInstanceOf[Boolean]

    val timerBegin = System.currentTimeMillis()
    val sc = new SparkContext(conf)

    if (useIndex == false) {
      val leftPath = leftFile
      val leftIndexPath = leftPath + ".index"
      val rightPath = rightFile
      val rightIndexPath = rightPath + ".index"

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      sqlContext.setConf("spark.sql.parquet.filterPushdown", "true")
      import sqlContext.implicits._
      def getParquetDF(path:String) = {
        try {
          val parquetFile = sqlContext.read.parquet(path)
          parquetFile
        } catch {
          case e: Exception => throw new Exception("failed to load index from " + path + "\n because of " + e.getMessage)
        }
      }
      val leftIndex = getParquetDF(leftIndexPath)
      leftIndex.registerTempTable("leftIndex")
      //val rightIndex = getParquetDF(rightIndexPath)
      //rightIndex.registerTempTable("rightIndex")

      val leftPartition = leftIndex.map(x => ((x.getAs[Double]("xmin"), x.getAs[Double]("ymin"),
        x.getAs[Double]("xmax"), x.getAs[Double]("ymax")), x.getAs[Long]("id"))).collect
      //val rightPartition = rightIndex.map(x => ((x.getAs[Double]("xmin"), x.getAs[Double]("ymin"),
      //  x.getAs[Double]("xmax"), x.getAs[Double]("ymax")), x.getAs[Long]("id"))).collect

      val leftData = getParquetDF(leftFile)
      val rightData = getParquetDF(rightFile)

      leftData.cache()
      rightData.cache()

      val rightGeom = rightData.rdd.map(x => (x.getAs[Long]("rid"), new WKTReader().read(x.getAs[String]("geom"))))
      //val rightGeom = rightData.rdd.map(x => (x.getAs[Long]("rid"), new WKBReader().read(WKBReader.hexToBytes(x.getAs[String]("geom")))))
      //val rightGeom = rightData.rdd.map(x => (x.getAs[Long]("rid"), new WKBReader().read(WKBReader.hexToBytes(x.getAs[String]("geom")))))
      //                         .map(x => Geom(x._1, new WKBWriter().write(x._2))).toDF
      //rightGeom.cache()

      //val leftGeom = leftData.rdd.map(x => (x.getAs[Long]("rid"), new WKTReader().read(x.getAs[String]("geom"))))
      //val leftGeom = leftData.rdd.map(x => (x.getAs[Long]("rid"), new WKBReader()read(WKBReader.hexToBytes(x.getAs[String]("geom")))))
      //val leftGeom = leftData.rdd.map(x => (x.getAs[Long]("rid"), new WKBReader()read(WKBReader.hexToBytes(x.getAs[String]("geom")))))
      //                       .map(x => Geom(x._1, new WKBWriter().write(x._2))).toDF
      //leftGeom.cache()

      //val leftLocal = leftGeom.select("geom").map(x => new WKBReader().read(x.getAs[Array[Byte]]("geom"))).collect
      //val rightLocal = rightGeom.select("geom").map(x => new WKBReader().read(x.getAs[Array[Byte]]("geom"))).collect

      //var rtree = new STRtree()
      //for (geom <- rightLocal)
      //  rtree.insert(geom.getEnvelopeInternal, geom)
      //val localJoin = leftLocal.flatMap(x => rtree.query(x.getEnvelopeInternal).toArray())
      //println("local join count:" + localJoin.size)

      val rightMBR = rightGeom.map(x => (x._1, (x._2.getEnvelopeInternal.getMinX, x._2.getEnvelopeInternal.getMinY,
                                                x._2.getEnvelopeInternal.getMaxX, x._2.getEnvelopeInternal.getMaxY)))

      def findPartIds(partitions:Array[(MBR, Long)], mbr: MBR):Array[Long] = {
        val matchedIds = partitions.filter(x => intersect(x._1, mbr)).map(x => x._2)
        matchedIds
      }
      val rightMBRWithPartitionId = rightMBR.flatMap {
        x => val ids = findPartIds(leftPartition, x._2); ids.map(y => (x._1, x._2, y))}
      val leftIndexRDD = leftIndex.map(x => (x.getAs[Long]("id"), x.getAs[Seq[Row]]("tree").map(r =>
        (r.getDouble(0), r.getDouble(1), r.getDouble(2),
          r.getDouble(3), r.getLong(4), r.getLong(5)))))
      val candidatePairs = rightMBRWithPartitionId.map(x => (x._3, (x._1, x._2))).join(leftIndexRDD).flatMap {
        x => val queryResults = RTree.queryRtree(x._2._2, x._2._1._2, 0); queryResults.map(y => (x._2._1._1, y))
      } .map(x => Pair(x._2, x._1)).toDF


      val rightJoinPairs = candidatePairs.join(rightData, candidatePairs("rid") === rightData("rid"))
        .select(rightData("rid").as("rid"), rightData("geom").as("rgeom"), candidatePairs("lid"))

      val finalJoinPairs = rightJoinPairs.join(leftData, rightJoinPairs("lid") === leftData("rid"))
        .select(leftData("geom").as("lgeom"), rightJoinPairs("rgeom"), leftData("rid").as("lid"), rightJoinPairs("rid"))

      //def intersect(a:String, b:String):Boolean = {
      //  val lgeom = new WKTReader().read(a)
      //  val rgeom = new WKTReader().read(b)
      //  lgeom.intersects(rgeom)
      //}
      //println("candidate count: " + candidatePairs.count)
      val refinedPairs = finalJoinPairs.rdd.filter(x => new WKTReader().read(x.getAs[String]("lgeom")).intersects(
          new WKTReader().read(x.getAs[String]("rgeom"))))
        .map(x => (x.getAs[Long]("lid"), x.getAs[Long]("rid")))
      //candidatePairs.cache
      //val rightJoinPairs = rightGeom.join(candidatePairs).map(x => (x._2._2, (x._2._1, x._1)))
      //val refinedPairs = leftGeom.join(rightJoinPairs).filter(x => x._2._1.intersects(x._2._2._1))
      //                           .map(x => (x._1, x._2._2._2))
      //println("final count:" + refinedPairs.count())
      refinedPairs.saveAsTextFile(outFile)
    }
    else {
      /*
      val leftPath = leftFile + ".index"
      val leftIndex = new DistIndex(sc, leftPath)
      leftIndex.cache
      val rightPath = rightFile + ".index"
      val rightIndex = new DistIndex(sc, rightPath)
      rightIndex.cache
      val timerBegin = System.currentTimeMillis()
      val results = leftIndex.join(rightIndex)
      val timerEnd = System.currentTimeMillis()
      println("results: " + results.count)
      println("real query time: " + (timerEnd - timerBegin) + " ms")
      leftIndex.unpersist
      rightIndex.unpersist
      */
      val leftPath = leftFile
      val leftIndexPath = leftPath + ".index"
      val rightPath = rightFile
      val rightIndexPath = rightPath + ".index"

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      sqlContext.setConf("spark.sql.parquet.filterPushdown", "true")
      import sqlContext.implicits._
      def getParquetDF(path: String) = {
        try {
          val parquetFile = sqlContext.read.parquet(path)
          parquetFile
        } catch {
          case e: Exception => throw new Exception("failed to load index from " + path + "\n because of " + e.getMessage)
        }
      }
      val leftIndex = getParquetDF(leftIndexPath)
      leftIndex.registerTempTable("leftIndex")
      val rightIndex = getParquetDF(rightIndexPath)
      rightIndex.registerTempTable("rightIndex")

      val leftData = getParquetDF(leftFile)
      val rightData = getParquetDF(rightFile)

      leftData.cache()
      rightData.cache()

      val rightGeom = rightData.rdd.map(x => (x.getAs[Long]("rid"), new WKTReader().read(x.getAs[String]("geom"))))

      val leftGeom = leftData.rdd.map(x => (x.getAs[Long]("rid"), new WKTReader().read(x.getAs[String]("geom"))))

      val leftPartition = leftIndex.map(x => ((x.getAs[Double]("xmin"), x.getAs[Double]("ymin"),
        x.getAs[Double]("xmax"), x.getAs[Double]("ymax")), x.getAs[Long]("id"))).collect
      val rightPartition = rightIndex.map(x => ((x.getAs[Double]("xmin"), x.getAs[Double]("ymin"),
        x.getAs[Double]("xmax"), x.getAs[Double]("ymax")), x.getAs[Long]("id"))).collect

      def partitionLocalJoin(a: Seq[((Double, Double, Double, Double), Long)],
                             b: Seq[((Double, Double, Double, Double), Long)]) = {

        val rtreeA = RTree.createRtreeFromBulkLoading(a)
        val rtreeB = RTree.createRtreeFromBulkLoading(b)
        RTree.join(rtreeA, rtreeB)
      }

      val matchedPartitions = partitionLocalJoin(leftPartition, rightPartition).toDF("lpid", "rpid").distinct
      //println("matchedPartitions:" + matchedPartitions.count)

      def treeJoin(a: Seq[Row], b: Seq[Row]) = {
        RTree.join(a.map(x => (x.getDouble(0), x.getDouble(1), x.getDouble(2), x.getDouble(3), x.getLong(4), x.getLong(5))),
          b.map(x => (x.getDouble(0), x.getDouble(1), x.getDouble(2), x.getDouble(3), x.getLong(4), x.getLong(5))))
      }

      def intersect(a: Array[Byte], b: Array[Byte]): Boolean = {
        val lgeom = new WKBReader().read(a)
        val rgeom = new WKBReader().read(b)
        lgeom.intersects(rgeom)
      }

      //register UDF to Spark SQL
      sqlContext.udf.register("treeJoin", treeJoin _)
      sqlContext.udf.register("ST_Intersect", intersect _)
      matchedPartitions.registerTempTable("matchedPartitions")

      val joinPartition = leftIndex.join(matchedPartitions, leftIndex("id") === matchedPartitions("lpid"))
        .join(rightIndex, matchedPartitions("rpid") === rightIndex("id"))
        .select(leftIndex("tree").as("lt"), rightIndex("tree").as("rt"))
        .flatMap(x => treeJoin(x.getAs[Seq[Row]]("lt"), x.getAs[Seq[Row]]("rt"))).distinct() //.sortBy(_._2)
        .map(x => Pair(x._1, x._2)).toDF //.sort("rid")


      println("candidate pairs:" + joinPartition.count)
      //use embeded SQL statement
      //val joinPartition = sqlContext.sql(
      //"""
      //  |select treeJoin(leftIndex.tree, rightIndex.tree) as ps
      //  |from leftIndex join matchedPartitions on leftIndex.id = matchedPartitions.lpid
      //  |join rightIndex on matchedPartitions.rpid = rightIndex.id
      //""".stripMargin).explode('ps) {ps => ps.getAs[Seq[Row]](0).map(x => Pair(x.getLong(0), x.getLong(1)))}
      //  .select('lid, 'rid).distinct

      //below is generating cross product, which is very slow
      //val joinPartition = leftIndex.join(rightIndex,
      //  leftIndex("xmin") <= rightIndex("xmax") && leftIndex("ymin") <= rightIndex("ymax") &&
      //    leftIndex("xmax") >= rightIndex("xmin") && leftIndex("ymax") >= rightIndex("ymin"))
      //  .select(leftIndex("tree").as("lt"), rightIndex("tree").as("rt"))

      joinPartition.registerTempTable("joinPartition")

      val joinPairs = joinPartition
      joinPairs.registerTempTable("joinPairs")
      joinPairs.cache()


      /* SQL
      val refinedPairs = sqlContext.sql(
        """
          |select leftGeom.rid as lid, rightGeom.rid as rid
          |from leftGeom join joinPairs on leftGeom.rid = joinPairs.lid join rightGeom on rightGeom.rid = joinPairs.rid
          |where ST_Intersect(leftGeom.geom, rightGeom.geom) = true
        """.stripMargin)
      */
      val rightJoinPairs = joinPairs.join(rightData, joinPairs("rid") === rightData("rid"))
        .select(rightData("rid").as("rid"), rightData("geom").as("rgeom"), joinPairs("lid"))//.sort("lid")

      val finalJoinPairs = rightJoinPairs.join(leftData, rightJoinPairs("lid") === leftData("rid"))
        .select(leftData("geom").as("lgeom"), rightJoinPairs("rgeom"), leftData("rid").as("lid"), rightJoinPairs("rid"))

      //val refinedPairs = finalJoinPairs.rdd.filter(x => intersect(x.getAs[String]("lgeom"), x.getAs[String]("rgeom")))
      //                                 .map(x => (x.getAs[Long]("lid"), x.getAs[Long]("rid")))

      val refinedPairs = finalJoinPairs.rdd.filter(x => new WKTReader().read(x.getAs[String]("lgeom")).intersects(
          new WKTReader().read(x.getAs[String]("rgeom"))))
                                       .map(x => (x.getAs[Long]("lid"), x.getAs[Long]("rid")))
      //refinedPairs.rdd.map(x => (x.getAs[Long]("lid"), x.getAs[Long]("rid"))).saveAsTextFile(outFile)
      refinedPairs.saveAsTextFile(outFile)
      //println("final count:" + refinedPairs.count()):w

    }

    val timerEnd = System.currentTimeMillis()
    println("total time: " + (timerEnd - timerBegin) + " ms")

  }
}
