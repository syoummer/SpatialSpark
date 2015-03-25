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

package spatialspark.index

import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import spatialspark.util.MBR

import scala.util.Try
import spatialspark.util.MBR
/**
 * Created by Simin You on 3/20/15.
 * @author Simin You
 *
 */
object STIndex {
  /**
   * build Sort-tile based index
   */
  import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

  //reference: http://stackoverflow.com/questions/23995040/write-to-multiple-outputs-by-key-spark-one-spark-job
  class KeyBasedOutput[T >: Null, V <: AnyRef] extends MultipleTextOutputFormat[T , V] {
    override def generateFileNameForKeyValue(key: T, value: V, leaf: String) = {
      key.toString
    }
    override protected def generateActualKey(key: T, value: V) = {
      null
    }
  }

  case class Wrapped[A](elem: A)(implicit ordering: Ordering[A])
    extends Ordered[Wrapped[A]] {
    def compare(that: Wrapped[A]): Int = ordering.compare(this.elem, that.elem)
  }

  /**
   * Generate partitions from sample MBRs
   * @param sampleData sample MBRs
   * @param dimX number of splits on x
   * @param dimY number of splits on y
   * @return partitions in forms of MBRs
   */
  def generatePartitions(sampleData:RDD[MBR], dimX:Int, dimY:Int): Array[MBR] = {
    val numObjects = sampleData.count
    sampleData.cache()
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
    val tilesFinal = tiles.map(x => new MBR(x._1, x._2, x._3, x._4))
    tilesFinal.collect()
  }

  def build(sc:SparkContext, conf:IndexConf) = {
    val inputFile = conf.inputFile.trim().stripSuffix("/")
    val outputFile = conf.outputFile.trim().stripSuffix("/")
    val xDim = conf.gridDimX
    val yDim = conf.gridDimY
    val sampleRatio = conf.ratio
    val inputData = sc.textFile(inputFile).zipWithIndex()
    val inputWKT = inputData.map(x => (x._2, x._1.split(conf.separator).apply(conf.geometryIndex)))
    val inputGeometry = inputWKT.map(x => (x._1, Try(new WKTReader().read(x._2)))).filter(_._2.isSuccess)
      .map(x => (x._1, x._2.get))
    val inputMBR = inputGeometry.map(x => {val env = x._2.getEnvelopeInternal;
      (x._1, new MBR(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY))})

    //val extent:MBR = inputMBR.map(_._2).reduce(_ union _)
    val sampleMBR = inputMBR.map(_._2).sample(false, sampleRatio)
    //val partitions = SortTilePartition(sc, sampleMBR, extent, xDim, yDim).zipWithIndex
    val partitions = generatePartitions(sampleMBR, xDim, yDim).zipWithIndex

    def findPartition(partitions: Array[(MBR, Int)], env:MBR):Int = {
      var bestId = -1
      var bestCover = 0.0d

      for (partition <- partitions) {
        val currentId = partition._2
        val currentMBR = partition._1
        var currentCover = 0.0d
        if (currentMBR.intersects(env)) {
          val area = ((env.ymax min currentMBR.ymax) - (env.ymin max currentMBR.ymin)) *
           ((env.xmax min currentMBR.xmax) - (env.xmin max currentMBR.xmin))
          currentCover = -area
        }
        else {
          val center1 = currentMBR.center()
          val center2 = env.center()

          val distance = (center1._1 - center2._1) * (center1._1 - center2._1) +
            (center1._2 - center2._2) * (center1._2 - center2._2)
          currentCover = distance
        }
        if (bestId == -1) {
          bestId = currentId
          bestCover = currentCover
        } else {
          if (currentCover < bestCover) {
            bestId = currentId
            bestCover = currentCover
          }
        }
      }

      bestId
    }
    //for each geometry find its corresponding partition
    val matchedMBR = inputMBR.map(x => (x._1, findPartition(partitions, x._2).toLong, x._2))
    val matchedIds = matchedMBR.map(x => (x._1, x._2))
    val joined = inputData.map(_.swap).join(matchedIds)
    val joinedIds = joined.map(x => (x._2._2 , x._2._1)).partitionBy(new HashPartitioner(xDim * yDim))

    println("joinedId: " + joinedIds.count())
    joinedIds.saveAsHadoopFile(outputFile, classOf[String], classOf[String], classOf[KeyBasedOutput[String, String]])

    val mbrsForEachPartition = matchedMBR.map(x => (x._2, x._3)).groupByKey().map(x => (x._1, x._2.reduce(_ union _)))
    mbrsForEachPartition.map(x => x._1+"\t"+x._2.toString("\t")).repartition(1).saveAsTextFile(outputFile+"_index")
    //mbrsForEachPartition.map(x => x._1+"\t"+x._2.toText()).repartition(1).saveAsTextFile(outputFile+"_debug")
  }

  /**
   * query on the indexed dataset
   * @todo optimize partitions that completely fall within query window
   * @param sc SparkContext
   * @param inputFile input dataset location
   * @param geometryIndex geometry index for the input dataset
   * @param window query window
   * @param separator separator for input dataset, by default it is TAB
   * @return all lines of the dataset intersects with the query window
   */
  def query(sc:SparkContext, inputFile:String, geometryIndex:Int, window:MBR, separator:String = "\t"):RDD[String] = {
    val indexFilePath = inputFile + "_index"
    val indexFiles = sc.textFile(indexFilePath).map(x => x.split(separator))
      .map(x => (x(0), new MBR(x(1)toDouble, x(2).toDouble, x(3).toDouble, x(4).toDouble)))
    val indexFilesFiltered = indexFiles.filter(_._2.intersects(window)).map(_._1)
    val fileList = indexFilesFiltered.collect().toIndexedSeq.map(x=>inputFile + "/" + x)
    println("files: " + fileList.mkString(";"))
    val queryGeometry = new GeometryFactory() .toGeometry(window.toEnvelop())
    val lines = sc.textFile(fileList.mkString(",")).map(x => (Try(new WKTReader().read(x.split("\t").apply(geometryIndex))), x))
      .filter(_._1.isSuccess)
    val results = lines.map(x => (x._1.get, x._2)).filter(x => queryGeometry.intersects(x._1)).map(_._2)
    results
  }
}
