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

package spatialspark.partition.bsp

import spatialspark.util.MBR
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Created by Simin You on 10/22/14.
 */
class BinarySplitPartition extends Serializable{

}

object BinarySplitPartition {
  /**
   * Performs K-D tree-like split on __points__ with desired __extent__ until
   * number of points (__np__) or split depth (__level__) reached
   * @param points points in format of (x, y)
   * @param extent extent in format of (xmin, ymin, xmax, ymax)
   * @param level max split level
   * @return partitions represented in (xmin, ymin, xmax, ymax)
   */
  def binaryPartition(points:RDD[(Double, Double)], extent:(Double, Double, Double, Double), level:Long)
  : RDD[(Double, Double, Double, Double)] = {
    //TODO optimize this implementation. reduce number of sorts
    val numPoints = points.count()
    val halfNumPoints = numPoints/2
    if (numPoints < 3 || level == 0)
      return points.sparkContext.parallelize(List(extent))

    var result = points.sparkContext.parallelize(List.empty[(Double, Double, Double, Double)])

    if (extent._3 - extent._1 > extent._4 - extent._2) {
      val pointsSorted = points.sortByKey(true).cache()
      val first = pointsSorted.zipWithIndex().filter(_._2 / halfNumPoints == 0).map(_._1)
      val second = pointsSorted.subtract(first)
      val bound = first.map(_._1).max()
      val firstExtent = (extent._1, extent._2, bound, extent._4)
      val secondExtent = (bound, extent._2, extent._3, extent._4)
      result ++= binaryPartition(first, firstExtent, level-1)
      result ++= binaryPartition(second, secondExtent, level-1)
    }
    else {
      val pointsSorted = points.keyBy(_._2).sortByKey(true).values.cache()
      val first = pointsSorted.zipWithIndex().filter(_._2 / halfNumPoints == 0).map(_._1)
      val second = pointsSorted.subtract(first)
      val bound = first.map(_._2).max()
      val firstExtent = (extent._1, extent._2, extent._3, bound)
      val secondExtent = (extent._1, bound, extent._3, extent._4)
      result ++= binaryPartition(first, firstExtent, level-1)
      result ++= binaryPartition(second, secondExtent, level-1)
    }
    result.cache()
  }

  /**
   * Performs K-D tree-like split on __points__ with desired __extent__ until
   * number of points (__np__) or split depth (__level__) reached
   * @param points points in format of (x, y)
   * @param extent extent in format of (xmin, ymin, xmax, ymax)
   * @param level max split level
   * @return partitions represented in (xmin, ymin, xmax, ymax)
   */
  def binaryPartitionSeq(points:Seq[(Double, Double)], extent:(Double, Double, Double, Double), level:Long)
  : Seq[(Double, Double, Double, Double)] = {
    //TODO optimize this implementation. reduce number of sorts
    val numPoints = points.length
    val halfNumPoints = numPoints/2
    if (numPoints < 3 || level == 0)
      return Seq(extent)

    var result = Seq.empty[(Double, Double, Double, Double)]

    if (extent._3 - extent._1 > extent._4 - extent._2) {
      val pointsSorted = points.sortBy(_._1)
      val (first, second) = pointsSorted.splitAt(halfNumPoints)
      val bound = first.map(_._1).max
      val firstExtent = (extent._1, extent._2, bound, extent._4)
      val secondExtent = (bound, extent._2, extent._3, extent._4)
      result ++= binaryPartitionSeq(first, firstExtent, level-1)
      result ++= binaryPartitionSeq(second, secondExtent, level-1)
    }
    else {
      val pointsSorted = points.sortBy(_._2)
      val (first, second) = pointsSorted.splitAt(halfNumPoints)
      val bound = first.map(_._2).max
      val firstExtent = (extent._1, extent._2, extent._3, bound)
      val secondExtent = (extent._1, bound, extent._3, extent._4)
      result ++= binaryPartitionSeq(first, firstExtent, level-1)
      result ++= binaryPartitionSeq(second, secondExtent, level-1)
    }
    result
  }
  
  def apply(sc:SparkContext, sampleData:RDD[MBR], extent:MBR, level:Long, parallel:Boolean = true) : Array[MBR] = {
    val centroids = sampleData.map(x => ((x.xmin + x.xmax) / 2.0, (x.ymin + x.ymax) / 2.0))
    if (parallel) {
      val partitions = binaryPartition(centroids, (extent.xmin, extent.ymin, extent.xmax, extent.ymax), level)
      return partitions.map(x => new MBR(x._1, x._2, x._3, x._4)).collect()
    } else {
      val partitions = binaryPartitionSeq(centroids.collect().toSeq,
                                          (extent.xmin, extent.ymin, extent.xmax, extent.ymax), level)
      return partitions.map(x => new MBR(x._1, x._2, x._3, x._4)).toArray
      //val partitions = binaryPartition(centroids.repartition(1), (extent.xmin, extent.ymin, extent.xmax, extent.ymax), level)
      //return partitions.map(x => new MBR(x._1, x._2, x._3, x._4)).collect()
    }
  }


}
