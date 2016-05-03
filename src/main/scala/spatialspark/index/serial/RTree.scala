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

package spatialspark.index.serial

import spatialspark.util.MBR

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by Simin You on 7/27/15.
 */


object RTree {

  type RTreeNode = (Double, Double, Double, Double, Long, Long)

  /**
   * Query R-tree using a window (MBR)
   * @todo make it tail recursion
   * @param objects
   * @param mbr
   * @param index
   * @return ids that intersect with query window
   */
  def queryRtree(objects:Seq[(Double, Double, Double, Double, Long, Long)],
                 mbr:(Double, Double, Double, Double), index:Int):Seq[Long] = {
    val hits = ArrayBuffer[Long]()
    val obj = objects.apply(index)
    val xmin = obj._1
    val ymin = obj._2
    val xmax = obj._3
    val ymax = obj._4
    if (!(xmin > mbr._3 || ymin > mbr._4 || xmax < mbr._1 || ymax < mbr._2)) {
      if (obj._6 == -1l) {
        //leaf
        hits += obj._5
      }
      else {
        for( i <- 0l until obj._6)
          hits ++= queryRtree(objects, mbr, obj._5.toInt + i.toInt)
      }
    }
    hits.toArray
  }

  def flatHelper[A,B,C,D,E](t: ((A,B,C,D),E)) = (t._1._1, t._1._2, t._1._3, t._1._4, t._2)

  /**
   * Create R-tree using STR bulk-loading
   * @param objects MBRs with id
   * @param nodeCapacity maximum capacity of each R-tree node
   * @return R-tree in array format. Each tuple represents (xmin, ymin, xmax, ymax, fc, nc), fc is index of
   *         first child, nc is number of children for this node. Tuples with nc == -1 are leaf nodes
   */
  def createRtreeFromBulkLoading(objects:Seq[((Double, Double, Double, Double), Long)], nodeCapacity:Int = 10)
  :Seq[(Double, Double, Double, Double, Long, Long)] = {
    val numObjects = objects.length
    val numTiles = math.ceil(numObjects.toDouble / nodeCapacity.toDouble)
    val xDim : Int = math.ceil(math.sqrt(numTiles.toDouble)).toInt
    val yDim : Int = math.ceil(math.sqrt(numTiles.toDouble)).toInt
    val numObjectsPerSlice = nodeCapacity * yDim

    //sort
    val objectsSorted = objects.map(x=>((x._1._1+x._1._3, x._2/numObjectsPerSlice, x._1._2+x._1._4), x))
      .sortBy(_._1).map(x=>x._2)

    //pack
    var rtree = objectsSorted.map(_._1).zipWithIndex.map(flatHelper)
      .map(x=>(x._1, x._2, x._3, x._4, x._5.toLong, 1l)).groupBy(_._5.toInt/nodeCapacity).map {
      case (group, traversable) =>
        (group, traversable.reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4,
          a._5 min b._5, a._6+b._6)))} .toArray.sortBy(_._1).map(_._2)

    val offset = rtree.length.toLong
    rtree = rtree.map(x=>(x._1,x._2,x._3,x._4,x._5+offset,x._6))

    var currentLevel = rtree

    while (currentLevel.length > nodeCapacity) {
      val numTiles = math.ceil(currentLevel.length.toDouble / nodeCapacity.toDouble)
      //val xDim : Int = math.ceil(math.sqrt(numTiles.toDouble)).toInt;
      val yDim : Int = math.ceil(math.sqrt(numTiles.toDouble)).toInt
      val numObjectsPerSlice = nodeCapacity * yDim
      //println("numTiles: " + numTiles + " " + currentLevel.length + " " + nodeCapacity)
      val objectsSorted = currentLevel.map(x=>(x._1, x._2, x._3, x._4)).zipWithIndex
        .map(x=>((x._1._1+x._1._3, x._2/numObjectsPerSlice, x._1._2+x._1._4), x))
        .sortBy(_._1).map(x=>x._2)

      //pack
      val newLevel = objectsSorted.map(_._1).zipWithIndex.map(flatHelper)
        .map(x=>(x._1, x._2, x._3, x._4, x._5.toLong, 1l)).groupBy(_._5/nodeCapacity).map {
        case (group, traversable) =>
          (group, traversable.reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4,
            a._5 min b._5, a._6+b._6)))} .toArray.sortBy(_._1).map(_._2)
      val newLevelSize = newLevel.length
      rtree = (newLevel ++ rtree).map(x=>(x._1,x._2,x._3,x._4,x._5+newLevelSize,x._6))
      currentLevel = newLevel.zipWithIndex.map(x=>(x._1._1, x._1._2, x._1._3, x._1._4, x._2.toLong, 1l))
    }

    if (currentLevel.length > 1 ) {
      val root = currentLevel.reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4,
        1l, a._6+b._6))
      rtree = root +: rtree.map(x=>(x._1,x._2,x._3,x._4,x._5+1,x._6))
    }
    rtree = rtree ++ objectsSorted.map(x=>(x._1._1, x._1._2, x._1._3, x._1._4, x._2, -1l))
    rtree
  }

  def join(a:Seq[RTreeNode], b:Seq[RTreeNode]): Seq[(Long, Long)] = {
    val results = ArrayBuffer[(Long, Long)]()

    val queue = new mutable.Queue[(Int, Int)]
    queue.enqueue((0, 0))
    def intersect(n1:RTreeNode, n2:RTreeNode):Boolean = {
      MBR.intersect((n1._1, n1._2, n1._3, n1._4), (n2._1, n2._2, n2._3, n2._4))
    }
    while (!queue.isEmpty) {
      val (aIndex, bIndex) = queue.dequeue()
      val aNode = a.apply(aIndex)
      val bNode = b.apply(bIndex)
      if (aNode._6 == -1 && bNode._6 == -1 && intersect(aNode, bNode)) {
        val p = (aNode._5, bNode._5)
        results += p
      }
      else {

        if (intersect(aNode, bNode)) {
          if (aNode._6 == -1l) {
            val candidates = for (j <- List.range(bNode._5.toInt, bNode._5.toInt + bNode._6.toInt))
              yield (aIndex, j)
            queue ++= candidates
          }
          else if (bNode._6 == -1l) {
            val candidates = for (i <- List.range(aNode._5.toInt, aNode._5.toInt + aNode._6.toInt))
              yield (i, bIndex)
            queue ++= candidates
          }
          else {
            val candidates = for (i <- List.range(aNode._5.toInt, aNode._5.toInt + aNode._6.toInt);
                                  j <- List.range(bNode._5.toInt, bNode._5.toInt + bNode._6.toInt))
               yield (i, j)
            queue ++= candidates
          }
        }
      }
    }
    results.toSeq
  }
}
