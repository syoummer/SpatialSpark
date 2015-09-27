/*
 * Copyright 2015 Kamil Gorlo
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

import com.holdenkarau.spark.testing.SharedSparkContext
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.scalatest.{FlatSpec, Matchers}
import spatialspark.operator.SpatialOperator.Intersects

class BroadcastSpatialJoinSpec extends FlatSpec with Matchers with SharedSparkContext {

  // A intersects B and C, C is inside A, B and C does not intersect
  val rectangleA = List((0, 0), (0, 10), (10, 10), (10, 0))
  val rectangleB = List((-4, -4), (-4, 4), (4, 4), (4, -4))
  val rectangleC = List((7, 7), (7, 8), (8, 8), (8, 7))

  val pointD = (1, -1)

  val geomFactory = new GeometryFactory()

  def toLineRingCoordArray(rectangle: List[(Int, Int)]): Array[Coordinate] = {
    (rectangle :+ rectangle.head).map { case (x, y) => new Coordinate(x, y) }.toArray
  }

  behavior of "BroadcastSpatialJoin algorithm"

  it should "work for simple intersection" in {
    // A, B
    val geomLeft = List(
      (0L, geomFactory.createPolygon(toLineRingCoordArray(rectangleA)).asInstanceOf[Geometry]),
      (1L, geomFactory.createPolygon(toLineRingCoordArray(rectangleB))))

    // C
    val geomRight = List(
      (0L, geomFactory.createPolygon(toLineRingCoordArray(rectangleC)).asInstanceOf[Geometry])
    )

    val leftGeomRDD = sc.parallelize(geomLeft)
    val rightGeomRDD = sc.parallelize(geomRight)
    val matchedPairs = BroadcastSpatialJoin(sc, leftGeomRDD, rightGeomRDD, Intersects).collect().toList

    // only A intersects with C
    matchedPairs should be(List((0, 0)))
  }

}
