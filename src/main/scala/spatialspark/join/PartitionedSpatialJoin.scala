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

package spatialspark.join

import com.vividsolutions.jts.geom.{Envelope, Geometry}
import com.vividsolutions.jts.index.strtree.STRtree
import spatialspark.operator.SpatialOperator
import spatialspark.partition._
import SpatialOperator._
import spatialspark.partition.bsp.{BinarySplitPartitionConf, BinarySplitPartition}
import spatialspark.partition.fgp.FixedGridPartitionConf
import spatialspark.partition.stp.{SortTilePartitionConf, SortTilePartition}
import spatialspark.util.MBR
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer


object PartitionedSpatialJoin {
  def mbr2pair(x: (Long, Geometry), radius: Double, fgConf: FixedGridPartitionConf): Array[(Long, (Long, Geometry))] = {
    val results: ArrayBuffer[(Long, (Long, Geometry))] = new ArrayBuffer[(Long, (Long, Geometry))]()
    val mbr = (x._2.getEnvelopeInternal.getMinX, x._2.getEnvelopeInternal.getMinY,
      x._2.getEnvelopeInternal.getMaxX, x._2.getEnvelopeInternal.getMaxY)
    val gridDimX = fgConf.gridDimX
    val gridDimY = fgConf.gridDimY
    val extent = fgConf.extent
    val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble
    val _xmin: Long = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toLong
    val _ymin: Long = (math.floor((mbr._2 - extent.ymin) / gridSizeY) max 0).toLong
    val _xmax: Long = (math.ceil((mbr._3 - extent.xmin) / gridSizeX) min gridDimX).toLong
    val _ymax: Long = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toLong

    for (i <- _xmin to _xmax; j <- _ymin to _ymax) {
      val id = j * gridDimX.toLong + i
      val pair = (id, x)
      results.append(pair)
    }

    results.toArray
  }

  def localJoinWithinRtreeReturnDist(x: Iterable[(Long, Geometry)], y: Iterable[(Long, Geometry)],
                                     predicate: SpatialOperator, r: Double = 0.0): Array[(Long, (Long, Double))] = {
    val results: ArrayBuffer[(Long, (Long, Double))] = new ArrayBuffer[(Long, (Long, Double))]()
    val rtree = new STRtree()
    for (i <- y) {
      val mbr = i._2.getEnvelopeInternal
      rtree.insert(mbr, i)
    }
    for (i <- x) {
      val mbr = i._2.getEnvelopeInternal
      mbr.expandBy(r)
      val queryResults = rtree.query(mbr).toArray
      for (j <- queryResults) {
        val obj = j.asInstanceOf[(Long, Geometry)]
        val dist = i._2.distance(obj._2)
        if (dist <= r) results.append((i._1, (obj._1, dist)))
      }
    }
    results.toArray
  }


  def localJoinWithinRtree(x: Iterable[(Long, Geometry)], y: Iterable[(Long, Geometry)], predicate: SpatialOperator,
                           r: Double = 0.0): Array[(Long, Long)] = {
    val results: ArrayBuffer[(Long, Long)] = new ArrayBuffer[(Long, Long)]()
    val rtree = new STRtree()
    for (i <- y) {
      val mbr = i._2.getEnvelopeInternal
      rtree.insert(mbr, i)
    }
    for (i <- x) {
      val mbr = i._2.getEnvelopeInternal
      mbr.expandBy(r)
      val queryResults = rtree.query(mbr).toArray
      for (j <- queryResults) {
        val obj = j.asInstanceOf[(Long, Geometry)]
        if (predicate == SpatialOperator.Within) {
          if (obj._2.contains(i._2)) results.append((i._1, obj._1))
        }
        else if (predicate == SpatialOperator.Contains) {
          if (i._2.contains(obj._2)) results.append((i._1, obj._1))
        }
        else if (predicate == SpatialOperator.Intersects) {
          if (i._2.intersects(obj._2)) results.append((i._1, obj._1))
        }
        else if (predicate == SpatialOperator.Overlaps) {
          if (i._2.overlaps(obj._2)) results.append((i._1, obj._1))
        }
        else if (predicate == SpatialOperator.WithinD) {
          if (i._2.isWithinDistance(obj._2, r)) results.append((i._1, obj._1))
        }
      }
    }
    results.toArray
  }

  def apply(sc: SparkContext,
            leftGeometryWithId: RDD[(Long, Geometry)],
            rightGeometryWithId: RDD[(Long, Geometry)],
            joinPredicate: SpatialOperator,
            radius: Double,
            paritionConf: PartitionConf,
            nestedLoop: Boolean = false) = {

    def rtreeQuery(rtree: => Broadcast[STRtree], x: (Long, Geometry), r: Double): Array[(Long, (Long, Geometry))] = {
      val rtreeLocal = rtree.value
      val queryEnv = x._2.getEnvelopeInternal
      queryEnv.expandBy(r)
      val candidates = rtreeLocal.query(queryEnv).toArray
      val results = candidates.map { case (geom_, id_) => (id_.asInstanceOf[Int].toLong, x) }
      /*
      val results = (candidates.filter { case (geom_, id_) => new Envelope(geom_.asInstanceOf[MBR].xmin - r,
        geom_.asInstanceOf[MBR].xmax + r,
        geom_.asInstanceOf[MBR].ymin - r,
        geom_.asInstanceOf[MBR].ymax + r)
        .intersects(x._2.getEnvelopeInternal)
      }
        .map { case (geom_, id_) => (id_.asInstanceOf[Int].toLong, x)})
        */
      results
    }

    var leftPairs: RDD[(Long, Iterable[(Long, Geometry)])] = sc.emptyRDD
    var rightPairs: RDD[(Long, Iterable[(Long, Geometry)])] = sc.emptyRDD

    //TODO: maybe we can sampling from both datasets for generating the partition file
    if (paritionConf.paritionMethod == PartitionMethod.FGP) {
      //partitioned spatial join using fixed grid
      val fgConf = paritionConf.asInstanceOf[FixedGridPartitionConf]
      leftPairs = leftGeometryWithId.flatMap(x => mbr2pair(x, 0, fgConf)).groupByKey()
      rightPairs = rightGeometryWithId.flatMap(x => mbr2pair(x, 0, fgConf)).groupByKey()
    } else if (paritionConf.paritionMethod == PartitionMethod.STP) {
      //partitioned spatial join using Sort-Tile
      val STPConf = paritionConf.asInstanceOf[SortTilePartitionConf]
      val dimX = STPConf.gridDimX
      val dimY = STPConf.gridDimY
      val extent = STPConf.extent
      val ratio = STPConf.ratio
      val parallel = STPConf.parallel

      val sampleData = rightGeometryWithId.sample(withReplacement = false, fraction = ratio)
        .map(_._2.getEnvelopeInternal)
        .map(x => new MBR(x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
      val partitions = SortTilePartition(sc, sampleData, extent, dimX, dimY, parallel).zipWithIndex
      val rtree: STRtree = new STRtree()
      for (i <- partitions) {
        val mbr = new Envelope(i._1.xmin, i._1.xmax, i._1.ymin, i._1.ymax)
        rtree.insert(mbr, i)
      }

      val rtreeBroadcast = sc.broadcast(rtree)
      //assign partition id for each object
      leftPairs = leftGeometryWithId.flatMap(x => rtreeQuery(rtreeBroadcast, x, radius)).groupByKey()
      rightPairs = rightGeometryWithId.flatMap(x => rtreeQuery(rtreeBroadcast, x, radius)).groupByKey()
    } else if (paritionConf.paritionMethod == PartitionMethod.BSP) {
      //partitioned spatial join using Binary-Split
      val BSPConf = paritionConf.asInstanceOf[BinarySplitPartitionConf]
      val extent = BSPConf.extent
      val ratio = BSPConf.ratio
      val level = BSPConf.level
      val parallel = BSPConf.parallel

      val sampleData = rightGeometryWithId.sample(withReplacement = false, fraction = ratio)
        .map(_._2.getEnvelopeInternal)
        .map(x => new MBR(x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
      val partitions = BinarySplitPartition(sc, sampleData, extent, level, parallel).zipWithIndex
      val rtree: STRtree = new STRtree()
      for (i <- partitions) {
        val mbr = new Envelope(i._1.xmin, i._1.xmax, i._1.ymin, i._1.ymax)
        rtree.insert(mbr, i)
      }

      val rtreeBroadcast = sc.broadcast(rtree)
      //assign partition id for each object
      leftPairs = leftGeometryWithId.flatMap(x => rtreeQuery(rtreeBroadcast, x, radius)).groupByKey()
      rightPairs = rightGeometryWithId.flatMap(x => rtreeQuery(rtreeBroadcast, x, radius)).groupByKey()
    }

    //Local join processing
    var refinedPairs: RDD[(Long, Long)] = sc.emptyRDD[(Long, Long)]
    if (nestedLoop) {
      //nested-loop join
      val joinedPairs = leftPairs.leftOuterJoin(rightPairs)

      if (joinPredicate == SpatialOperator.Within) {
        refinedPairs = joinedPairs.flatMap(x =>
          for (i <- x._2._1; j <- x._2._2.getOrElse(Iterable.empty[(Long, Geometry)]); if i._2.within(j._2))
            yield (i._1, j._1)).distinct()
      }
      else if (joinPredicate == SpatialOperator.Contains) {
        refinedPairs = joinedPairs.flatMap(x =>
          for (i <- x._2._1; j <- x._2._2.getOrElse(Iterable.empty[(Long, Geometry)]); if i._2.contains(j._2))
            yield (i._1, j._1)).distinct()
      }
      else if (joinPredicate == SpatialOperator.WithinD) {
        refinedPairs = joinedPairs.flatMap(x =>
          for (i <- x._2._1; j <- x._2._2.getOrElse(Iterable.empty[(Long, Geometry)])
               if i._2.isWithinDistance(j._2, radius))
            yield (i._1, j._1)).distinct()
      }
      else if (joinPredicate == SpatialOperator.Intersects) {
        refinedPairs = joinedPairs.flatMap(x =>
          for (i <- x._2._1; j <- x._2._2.getOrElse(Iterable.empty[(Long, Geometry)]); if i._2.intersects(j._2))
            yield (i._1, j._1)).distinct()
      }
      else if (joinPredicate == SpatialOperator.Overlaps) {
        refinedPairs = joinedPairs.flatMap(x =>
          for (i <- x._2._1; j <- x._2._2.getOrElse(Iterable.empty[(Long, Geometry)]); if i._2.overlaps(j._2))
            yield (i._1, j._1)).distinct()
      }
      else if (joinPredicate == SpatialOperator.NearestD) {
        val joinedPairsWithDist = joinedPairs.flatMap(x =>
          for (i <- x._2._1; j <- x._2._2.getOrElse(Iterable.empty[(Long, Geometry)]))
            yield (i._1, (j._1, i._2.distance(j._2)))
        ).reduceByKey((a, b) => if (a._2 < b._2) a else b)
        refinedPairs = joinedPairsWithDist.reduceByKey((a, b) => if (a._2 < b._2) a else b).map(x => (x._1, x._2._1))
      }
      refinedPairs
    } else {
      //R-tree join on each partition
      if (joinPredicate == SpatialOperator.NearestD) {
        val joinedPairs = leftPairs.leftOuterJoin(rightPairs).flatMap(x =>
          localJoinWithinRtreeReturnDist(
            x._2._1, x._2._2.getOrElse(Iterable.empty[(Long, Geometry)]), joinPredicate, radius))
        val nearestSearch = joinedPairs.reduceByKey((a, b) => if (a._2 < b._2) a else b).map(x => (x._1, x._2._1))
        nearestSearch
      }
      else {
        val joinedPairs = leftPairs.leftOuterJoin(rightPairs).flatMap(x =>
          localJoinWithinRtree(x._2._1, x._2._2.getOrElse(Iterable.empty[(Long, Geometry)]), joinPredicate, radius))
          .distinct()
        joinedPairs
      }
    }

  }
}
