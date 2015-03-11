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

package spatialspark.partition.stp

import spatialspark.util.MBR
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Created by Simin You on 10/22/14.
 */
class SortTilePartition extends Serializable{

}

object SortTilePartition {
  case class Wrapped[A](elem: A)(implicit ordering: Ordering[A])
    extends Ordered[Wrapped[A]] {
    def compare(that: Wrapped[A]): Int = ordering.compare(this.elem, that.elem)
  }

  def sortTile(sampleData:RDD[MBR], extent:MBR, dimX:Int, dimY:Int): Array[MBR] = {
    val numObjects = sampleData.count
    val numObjectsPerTile = math.ceil(numObjects.toDouble / (dimX * dimY)).toLong
    val numObjectsPerSlice = numObjectsPerTile * dimY

    //sort by center_x, slice, center_y
    val centroids = sampleData.map(x => ((x.xmin + x.xmax) / 2.0, (x.ymin + x.ymax) / 2.0))
    val objs = centroids.sortByKey(true).zipWithIndex().map(x => (x._1._1, x._1._2, x._2))
    val objectsSorted = objs.map(x=>(Wrapped(x._3/numObjectsPerSlice, x._2), x))
      .sortByKey(true).values

    //pack
    val tiles = objectsSorted.zipWithIndex().map(x => (x._2/numObjectsPerTile,
      (x._1._1, x._1._2, x._1._1, x._1._2, x._2/numObjectsPerSlice, x._2/numObjectsPerTile)))
      .reduceByKey((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4, a._5, a._6))
      .values
    //reduce for slice boundaries
    val sliceMap = tiles.map(x=>(x._5, (x._1, x._3))).reduceByKey((a, b) => (a._1 min b._1, a._1 max b._1)).collectAsMap()
    //val sliceBoundsBroadcast = sc.broadcast(sliceBounds)
    //val tileMap = tiles.map()
    val tilesLocal = tiles.collect()
    val tileMap = tiles.keyBy(_._6).collectAsMap()
    //fill out tiles as continuous partitions
    val sliceBounds = sliceMap.map(x => (x._1 -> (if (x._1==0) extent.xmin else (x._2._1+sliceMap(x._1-1)._2)/2,
      if (x._1==sliceMap.size-1) extent.xmax else (x._2._2+sliceMap(x._1+1)._1)/2)))
    val tilesFinal = tilesLocal.map(x=>(sliceBounds(x._5)._1,
      if(x._6 == 0 || (x._6 != 0 && x._5 != tileMap(x._6-1)._5)) extent.ymin else (x._2+tileMap(x._6-1)._4)/2,
      sliceBounds(x._5)._2,
      if(x._6 == tileMap.size-1 || (x._6 != tileMap.size-1 && x._5 != tileMap(x._6+1)._5)) extent.ymax else (x._4+tileMap(x._6+1)._2)/2
      ))

    tilesFinal.map(x => new MBR(x._1, x._2, x._3, x._4))
  }

  def sortTileSeq(sampleData:Array[MBR], extent:MBR, dimX:Int, dimY:Int): Array[MBR] = {
    val numObjects = sampleData.length
    val numObjectsPerTile = math.ceil(numObjects.toDouble / (dimX * dimY)).toLong
    val numObjectsPerSlice = numObjectsPerTile * dimY

    //sort by center_x, slice, center_y
    val centroids = sampleData.map(x => ((x.xmin + x.xmax) / 2.0, (x.ymin + x.ymax) / 2.0))
    val objs = centroids.sortBy(_._1).zipWithIndex.map(x => (x._1._1, x._1._2, x._2))
    val objectsSorted = objs.map(x=>(Wrapped(x._3/numObjectsPerSlice, x._2), x)).sortBy(_._1).map(_._2)

    //pack
    val tiles = objectsSorted.zipWithIndex.map(x => (x._2/numObjectsPerTile,
      (x._1._1, x._1._2, x._1._1, x._1._2, x._2/numObjectsPerSlice, x._2/numObjectsPerTile)))
        .groupBy(_._1).map(_._2.map(y=>y._2).reduce((a, b) =>
                  (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4, a._5, a._6))).toArray

    //reduce for slice boundaries
    val sliceMap = tiles.map(x=>(x._5, (x._1, x._3))).groupBy(_._1)
      .map(x => (x._1, x._2.map(y=>y._2).reduce((a, b) => (a._1 min b._1, a._1 max b._1)))).toMap//.collectAsMap()
    //val sliceBoundsBroadcast = sc.broadcast(sliceBounds)
    //val tileMap = tiles.map()
    val tilesLocal = tiles
    val tileMap = tiles.map(x => (x._6, x)).toMap
    //fill out tiles as continuous partitions
    val sliceBounds = sliceMap.map(x => (x._1 -> (if (x._1==0) extent.xmin else (x._2._1+sliceMap(x._1-1)._2)/2,
      if (x._1==sliceMap.size-1) extent.xmax else (x._2._2+sliceMap(x._1+1)._1)/2)))
    val tilesFinal = tilesLocal.map(x=>(sliceBounds(x._5)._1,
      if(x._6 == 0 || (x._6 != 0 && x._5 != tileMap(x._6-1)._5)) extent.ymin else (x._2+tileMap(x._6-1)._4)/2,
      sliceBounds(x._5)._2,
      if(x._6 == tileMap.size-1 || (x._6 != tileMap.size-1 && x._5 != tileMap(x._6+1)._5)) extent.ymax else (x._4+tileMap(x._6+1)._2)/2
      ))

    tilesFinal.map(x => new MBR(x._1, x._2, x._3, x._4))
  }


  def apply(sc:SparkContext, sampleData:RDD[MBR], extent:MBR, dimX:Int, dimY:Int, parallel:Boolean = true): Array[MBR] = {

    if (parallel)
      sortTile(sampleData, extent, dimX, dimY)
    else
      sortTileSeq(sampleData.collect(), extent, dimX, dimY)
  }

}
