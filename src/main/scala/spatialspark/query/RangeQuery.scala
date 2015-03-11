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
package spatialspark.query

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.index.strtree.STRtree
import spatialspark.operator.SpatialOperator
import spatialspark.operator.SpatialOperator._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by Simin You on 10/24/14.
 */
class RangeQuery extends Serializable {

}

object RangeQuery {

  def apply(sc:SparkContext,
            geometryWithId:RDD[(Long, Geometry)],
            filterGeometry:Geometry,
            operator:SpatialOperator,
            radius:Double = 0) :RDD[(Long, Geometry)] = {

    if (operator == SpatialOperator.Contains)
      geometryWithId.filter(_._2.contains(filterGeometry))
    else if (operator == SpatialOperator.Within)
      geometryWithId.filter(_._2.within(filterGeometry))
    else if (operator == SpatialOperator.WithinD)
      geometryWithId.filter(_._2.isWithinDistance(filterGeometry, radius))
    else {
      //TODO: error for not support
      sc.emptyRDD
    }
  }
}
