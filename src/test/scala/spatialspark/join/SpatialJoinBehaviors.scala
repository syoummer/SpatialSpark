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

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers}
import spatialspark.operator.SpatialOperator._

trait SpatialJoinBehaviors {
  this: FlatSpec with Matchers with GeometryFixtures =>

  /**
   * Helper method for testing spatial join algorithm with two lists of geometries
   * and then swapping the lists and checking if results are still OK
   */
  private def testAlgorithmWithSwap(algorithm: => SpatialJoinAlgorithm,
                                    firstGeomWithId: RDD[(Long, Geometry)],
                                    secondGeomWithId: RDD[(Long, Geometry)],
                                    predicate: SpatialOperator,
                                    expectedResult: List[(Long, Long)]) = {
    val matchedPairs = algorithm.run(firstGeomWithId, secondGeomWithId, predicate)
    val matchedPairsReversed = algorithm.run(secondGeomWithId, firstGeomWithId, predicate)

    val expectedResultReversed = expectedResult.map {
      case (x, y) => (y, x)
    }

    matchedPairs should be(expectedResult)
    matchedPairsReversed should be(expectedResultReversed)
  }

  def spatialJoinAlgorithm(algorithm: SpatialJoinAlgorithm): Unit = {

    it should "work for rectangles intersection" in {
      // only A intersects with C
      testAlgorithmWithSwap(algorithm, geomABWithId, geomCWithId, Intersects, List((0L, 0L)))
    }

    it should "work for point and rectangle intersection" in {
      // only B intersects with D
      testAlgorithmWithSwap(algorithm, geomABCWithId, geomDWithId, Intersects, List((1L, 0L)))
    }

    it should "work for empty intersection" in {
      testAlgorithmWithSwap(algorithm, geomCWithId, geomDWithId, Intersects, List())
    }

    it should "work with empty lists" in {
      testAlgorithmWithSwap(algorithm, emptyGeomWithId, geomABWithId, Intersects, List())
    }

  }

}
