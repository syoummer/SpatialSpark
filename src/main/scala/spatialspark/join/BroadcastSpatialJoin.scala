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

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.index.strtree.STRtree
import spatialspark.operator.SpatialOperator
import SpatialOperator.SpatialOperator
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * Created by Simin You on 10/22/14.
 */
object BroadcastSpatialJoin {


    def queryRtree(rtree: => Broadcast[STRtree], leftId:Long, geom:Geometry, predicate:SpatialOperator,
                   radius:Double): Array[(Long, Long)] = {
      var result = Array.empty[(Long, Long)]
      val queryEnv = geom.getEnvelopeInternal
      //queryEnv.expandBy(radius)
      val candidates = rtree.value.query(queryEnv).toArray//.asInstanceOf[Array[(Long, Geometry)]]
      if (predicate == SpatialOperator.Within) {
        result = candidates.filter{case (id_, geom_) => geom.within(geom_.asInstanceOf[Geometry])}
                           .map{case (id_, geom_) => (leftId, id_.asInstanceOf[Long])}
      } else if (predicate == SpatialOperator.Contains) {
        result = candidates.filter{case (id_, geom_) => geom.contains(geom_.asInstanceOf[Geometry])}
                           .map{case (id_, geom_) => (leftId, id_.asInstanceOf[Long])}
      } else if (predicate == SpatialOperator.WithinD) {
        result = candidates.filter{case (id_, geom_) => geom.isWithinDistance(geom_.asInstanceOf[Geometry], radius)}
                           .map{case (id_, geom_) => (leftId, id_.asInstanceOf[Long])}
      } else if (predicate == SpatialOperator.Intersects) {
        result = candidates.filter { case (id_, geom_) => geom.intersects(geom_.asInstanceOf[Geometry])}
          .map { case (id_, geom_) => (leftId, id_.asInstanceOf[Long])}
      } else if (predicate == SpatialOperator.Overlaps) {
        result = candidates.filter { case (id_, geom_) => geom.overlaps(geom_.asInstanceOf[Geometry])}
          .map { case (id_, geom_) => (leftId, id_.asInstanceOf[Long])}
      } else if (predicate == SpatialOperator.NearestD) {
        if (candidates.length == 0)
          return result
        val nearestItem = candidates.map{case (id_, geom_) => (id_.asInstanceOf[Long], geom_.asInstanceOf[Geometry].distance(geom))}
                           .reduce((a, b) => if (a._2 < b._2) a else b)
        result = Array((leftId, nearestItem._1))
      } else {
      }
      result
    }

  def apply(sc:SparkContext,
           leftGeometryWithId:RDD[(Long, Geometry)],
           rightGeometryWithId:RDD[(Long, Geometry)],
           joinPredicate:SpatialOperator,
           radius:Double = 0) : RDD[(Long, Long)] = {
    //create R-tree on right dataset
    val strtree = new STRtree()
    val rightGeometryWithIdLocal = rightGeometryWithId.collect()
    rightGeometryWithIdLocal.foreach(x => {val y = x._2.getEnvelopeInternal; y.expandBy(radius); strtree.insert(y, x)})
    val rtreeBroadcast = sc.broadcast(strtree)
    leftGeometryWithId.flatMap(x => queryRtree(rtreeBroadcast, x._1, x._2, joinPredicate, radius))
   }
}
