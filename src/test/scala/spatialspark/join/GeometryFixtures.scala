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

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.apache.spark.rdd.RDD


trait GeometryFixtures {
  this: SparkSpec =>

  // A intersects B and C, C is inside A, B and C does not intersect
  val rectangleA = List((0, 0), (0, 10), (10, 10), (10, 0))
  val rectangleB = List((-4, -4), (-4, 4), (4, 4), (4, -4))
  val rectangleC = List((7, 7), (7, 8), (8, 8), (8, 7))

  val pointD = (1, -1)

  def geomABWithId = sc.parallelize(List(
    (0L, toGeom(rectangleA)),
    (1L, toGeom(rectangleB))
  ))

  def geomCWithId = sc.parallelize(List(
    (0L, toGeom(rectangleC))
  ))

  def geomABCWithId = sc.parallelize(List(
    (0L, toGeom(rectangleA)),
    (1L, toGeom(rectangleB)),
    (2L, toGeom(rectangleC))
  ))

  def geomDWithId = sc.parallelize(List(
    (0L, toGeom(pointD))
  ))

  def emptyGeomWithId: RDD[(Long, Geometry)] = sc.parallelize(List())

  val geomFactory = new GeometryFactory()

  private def toLineRingCoordinatesArray(rectangle: List[(Int, Int)]): Array[Coordinate] = {
    (rectangle :+ rectangle.head).map { case (x, y) => new Coordinate(x, y) }.toArray
  }

  def toGeom(rectangle: List[(Int, Int)]): Geometry = {
    geomFactory.createPolygon(toLineRingCoordinatesArray(rectangle)).asInstanceOf[Geometry]
  }

  def toGeom(point: (Int, Int)): Geometry = {
    geomFactory.createPoint(new Coordinate(point._1, point._2)).asInstanceOf[Geometry]
  }

}
