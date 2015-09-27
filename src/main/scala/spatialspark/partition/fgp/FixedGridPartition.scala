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

package spatialspark.partition.fgp

import spatialspark.util.MBR
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class FixedGridPartition extends Serializable {

}

object FixedGridPartition {
  def apply(sc: SparkContext, extent: MBR, gridDimX: Int, gridDimY: Int): Array[MBR] = {
    val xSize = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val ySize = (extent.ymax - extent.ymin) / gridDimY.toDouble
    val results = for (i <- Array.range(0, gridDimX); j <- Array.range(0, gridDimY))
      yield new MBR(i * xSize + extent.xmin, j * ySize + extent.ymin,
        (i + 1) * xSize + extent.xmin, (j + 1) * ySize + extent.ymin)
    results
  }

  def genTileRDD(sc: SparkContext, extent: MBR, gridDimX: Int, gridDimY: Int): RDD[MBR] = {
    val xSize = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val ySize = (extent.ymax - extent.ymin) / gridDimY.toDouble
    val results = for (i <- Array.range(0, gridDimX); j <- Array.range(0, gridDimY))
      yield new MBR(i * xSize + extent.xmin, j * ySize + extent.ymin,
        (i + 1) * xSize + extent.xmin, (j + 1) * ySize + extent.ymin)
    sc.parallelize(wrapRefArray(results))
  }
}
