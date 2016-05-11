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

import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.geom.{Geometry, GeometryFactory}
import spatialspark.operator.SpatialOperator
import SpatialOperator.SpatialOperator
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD


object BroadcastSpatialJoin {

  def wider_search(rtree: => Broadcast[STRtree], leftId: Long, geom: Geometry, radius: Double): Array[AnyRef] = {
    val geometryFactory = new GeometryFactory()
    val geom_ex = geom.getEnvelopeInternal
    geom_ex.expandBy(radius)
    val new_envelope = geometryFactory.toGeometry(geom_ex).getEnvelopeInternal
    val candidates2 = rtree.value.query(new_envelope).toArray
    if (candidates2.size != 0 || radius>10){
      candidates2
    }else
      wider_search(rtree,leftId,geom,radius*20)
  }

  def queryRtree(rtree: => Broadcast[STRtree], leftId: Long, geom: Geometry, predicate: SpatialOperator,
                 radius: Double): Array[(Long, Long)] = {
    val queryEnv = geom.getEnvelopeInternal
    //queryEnv.expandBy(radius)
    val candidates = rtree.value.query(queryEnv).toArray //.asInstanceOf[Array[(Long, Geometry)]]
    if (predicate == SpatialOperator.Within) {
      candidates.filter { case (id_, geom_) => geom.within(geom_.asInstanceOf[Geometry]) }
        .map { case (id_, geom_) => (leftId, id_.asInstanceOf[Long]) }
    } else if (predicate == SpatialOperator.Contains) {
      candidates.filter { case (id_, geom_) => geom.contains(geom_.asInstanceOf[Geometry]) }
        .map { case (id_, geom_) => (leftId, id_.asInstanceOf[Long]) }
    } else if (predicate == SpatialOperator.WithinD) {
      candidates.filter { case (id_, geom_) => geom.isWithinDistance(geom_.asInstanceOf[Geometry], radius) }
        .map { case (id_, geom_) => (leftId, id_.asInstanceOf[Long]) }
    } else if (predicate == SpatialOperator.Intersects) {
      candidates.filter { case (id_, geom_) => geom.intersects(geom_.asInstanceOf[Geometry]) }
        .map { case (id_, geom_) => (leftId, id_.asInstanceOf[Long]) }
    } else if (predicate == SpatialOperator.Overlaps) {
      candidates.filter { case (id_, geom_) => geom.overlaps(geom_.asInstanceOf[Geometry]) }
        .map { case (id_, geom_) => (leftId, id_.asInstanceOf[Long]) }
    } else if (predicate == SpatialOperator.NearestD) {
      if (candidates.size ==0) {
        //making a bigger query
        val candidates2 = wider_search(rtree,leftId,geom,radius)
        if (candidates2.size==0){
          return Array.empty[(Long, Long)]
        }
        val nearestItem = candidates2.map {
          case (id_, geom_) => (id_.asInstanceOf[Long], geom_.asInstanceOf[Geometry].distance(geom))
        }.reduce((a, b) => if (a._2 < b._2) a else b)
        Array((leftId, nearestItem._1))

      }else {
        val nearestItem = candidates.map {
          case (id_, geom_) => (id_.asInstanceOf[Long], geom_.asInstanceOf[Geometry].distance(geom))
        }.reduce((a, b) => if (a._2 < b._2) a else b)
        Array((leftId, nearestItem._1))
      }
    } else {
      Array.empty[(Long, Long)]
    }
  }

  def apply(sc: SparkContext,
            leftGeometryWithId: RDD[(Long, Geometry)],
            rightGeometryWithId: RDD[(Long, Geometry)],
            joinPredicate: SpatialOperator,
            radius: Double = 0): RDD[(Long, Long)] = {
    // create R-tree on right dataset
    var radius_exp = radius
    if (joinPredicate ==SpatialOperator.NearestD && radius_exp ==0){
         radius_exp = 0.0002
       }

    val strtree = new STRtree()
    val rightGeometryWithIdLocal = rightGeometryWithId.collect()
    rightGeometryWithIdLocal.foreach(x => {
      val y = x._2.getEnvelopeInternal
      y.expandBy(radius_exp)
      strtree.insert(y, x)
    })
    val rtreeBroadcast = sc.broadcast(strtree)
    leftGeometryWithId.flatMap(x => queryRtree(rtreeBroadcast, x._1, x._2, joinPredicate, radius_exp))
  }
}
