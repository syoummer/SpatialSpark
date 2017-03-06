/*
 *
 *  Copyright $year Simin You
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package spatialspark.index

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import spatialspark.index.serial.RTree.RTreeNode

/**
 * Created by Simin You on 7/27/15.
 */

class DistIndex extends Serializable {

  //TODO: refactor this...
  type MBR = (Double, Double, Double, Double)

  def intersect(a:MBR, b:MBR):Boolean = {
    !(a._1 > b._3 || a._3 < b._1 || a._2 > b._4 || a._4 < b._2)
  }

  /**
   * Generate partitions from sample MBRs
   * @param sampleData sample MBRs
   * @param dimX number of splits on x
   * @param dimY number of splits on y
   * @return partitions in forms of MBRs
   */
  private[this] def generatePartitions(sampleData:RDD[MBR], dimX:Long, dimY:Long)
    : Array[MBR] = {

    val numObjects = sampleData.count
    sampleData.cache()
    val numObjectsPerTile = math.ceil(numObjects.toDouble / (dimX * dimY)).toLong
    val numObjectsPerSlice = numObjectsPerTile * dimY

    //sort by center_x, slice, center_y
    val centroids = sampleData.map(x => ((x._1 + x._3) / 2.0, (x._2 + x._4) / 2.0))
    val objs = centroids.sortByKey(true).zipWithIndex().map(x => (x._1._1, x._1._2, x._2))
    val objectsSorted = objs.map(x=>((x._3/numObjectsPerSlice, x._2), x))
      .sortByKey(true).values

    //pack
    val tiles = objectsSorted.zipWithIndex().map(x => (x._2/numObjectsPerTile,
      (x._1._1, x._1._2, x._1._1, x._1._2, x._2/numObjectsPerSlice, x._2/numObjectsPerTile)))
      .reduceByKey((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4, a._5, a._6))
      .values

    val tilesFinal = tiles.map(x => (x._1, x._2, x._3, x._4))
    val results = tilesFinal.collect()

    sampleData.unpersist(false)

    results
  }

  private[index] def buildIndex(input:RDD[(MBR, Long)], ratio:Double = 0.1, xDim:Long = 32, yDim:Long = 32)
    : RDD[(Long, Double, Double, Double, Double, Seq[RTreeNode])] = {
    def findPartId(partitions:Array[(MBR, Long)], mbr: MBR):Long = {
      def MBRDist(a:MBR, b:MBR):Double = {
        if (intersect(a, b)) {
          val x0 = b._1 max a._1
          val x1 = b._3 min a._3
          val y0 = b._2 max a._2
          val y1 = b._4 min a._4
          0.0 - (x1 - x0) * (y1 - y0)
        }
        else {
          val x1 = a._1 + a._3
          val y1 = a._2 + a._4
          val x2 = b._1 + b._3
          val y2 = b._2 + b._4
          (x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2)
        }
      }
      val bestId = partitions.map(x => (x._2, MBRDist(x._1, mbr))).reduce((a, b) => if (a._2 < b._2) a else b)._1
      bestId
    }

    val sampledInput = input.sample(false, ratio).map(_._1)
    val partitions = generatePartitions(sampledInput, xDim, yDim).zipWithIndex.map(x => (x._1, x._2.toLong))

    val inputByPartitionId = input.map(x => (x._1, x._2, findPartId(partitions, x._1)))
    //idMap = inputByPartitionId.map(x => (x._2, x._3))
    val inputById = inputByPartitionId.groupBy(_._3).map {
      case (groupId, traverse) => {
        val data = traverse.toArray.map(x => (x._1, x._2))
        val tree = serial.RTree.createRtreeFromBulkLoading(data.map(x => ((x._1._1, x._1._2, x._1._3, x._1._4), x._2)))
          .map(x=>(x._1, x._2, x._3, x._4, x._5, x._6))
        val mbr = tree.apply(0)
        (groupId, mbr._1, mbr._2, mbr._3, mbr._4, tree)
      }
    }

    inputById
  }

  private[index] def buildIndex(input:RDD[(MBR, Long)], partitions:Array[(MBR, Long)])
    : RDD[(Long, Double, Double, Double, Double, Seq[RTreeNode])] = {
    def findPartId(partitions:Array[(MBR, Long)], mbr: MBR):Long = {
      def MBRDist(a:MBR, b:MBR):Double = {
        if (intersect(a, b)) {
          val x0 = b._1 max a._1
          val x1 = b._3 min a._3
          val y0 = b._2 max a._2
          val y1 = b._4 min a._4
          0.0 - (x1 - x0) * (y1 - y0)
        }
        else {
          val x1 = a._1 + a._3
          val y1 = a._2 + a._4
          val x2 = b._1 + b._3
          val y2 = b._2 + b._4
          (x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2)
        }
      }
      val bestId = partitions.map(x => (x._2, MBRDist(x._1, mbr))).reduce((a, b) => if (a._2 < b._2) a else b)._1
      bestId
    }

    val inputByPartitionId = input.map(x => (x._1, x._2, findPartId(partitions, x._1)))
    //idMap = inputByPartitionId.map(x => (x._2, x._3))
    val inputById = inputByPartitionId.groupBy(_._3).map {
      case (groupId, traverse) => {
        val data = traverse.toArray.map(x => (x._1, x._2))
        val tree = serial.RTree.createRtreeFromBulkLoading(data.map(x => ((x._1._1, x._1._2, x._1._3, x._1._4), x._2)))
          .map(x=>(x._1, x._2, x._3, x._4, x._5, x._6))
        val mbr = tree.apply(0)
        (groupId, mbr._1, mbr._2, mbr._3, mbr._4, tree)
      }
    }

    inputById
  }

  var index:RDD[(Long, Double, Double, Double, Double, Seq[RTreeNode])] = null
  //var idMap:RDD[(Long, Long)] = null


  def this(input:RDD[((Double, Double, Double, Double), Long)], ratio:Double = 0.1, xDim:Long = 32, yDim:Long = 32) = {
    this()
    this.index = buildIndex(input, ratio, xDim, yDim)
  }
  def this(input:RDD[((Double, Double, Double, Double), Long)], partitions:Array[((Double, Double, Double, Double), Long)]) = {
    this()
    this.index = buildIndex(input, partitions)
  }

  def this(path:String) = {
    this()
    load(path)
  }

  case class Part(id:Long, xmin:Double, ymin:Double, xmax:Double, ymax:Double, tree:Seq[(Double, Double, Double, Double, Long, Long)])

  def toDF() = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val indexDF = index.map(x => Part(x._1, x._2, x._3, x._4, x._5, x._6.toSeq.map(y => (y._1, y._2, y._3, y._4, y._5, y._6)))).toDF()
    indexDF
  }
  /**
   * Save as parquet file
   * @param path
   */
  def save(path:String, numPartitions:Int = 0) = {
    if (index == null) throw new NullPointerException("index does not exist")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val indexDF = index.map(x => Part(x._1, x._2, x._3, x._4, x._5, x._6.toSeq.map(y => (y._1, y._2, y._3, y._4, y._5, y._6)))).toDF()
    if (numPartitions > 0)
      indexDF.repartition(numPartitions).write.parquet(path + ".index")
    else
      indexDF.write.parquet(path + ".index")
  }

  /**
   * Load from parquet file
   * @param path
   */
  def load(path:String) = {
    try {
      val spark = SparkSession.builder().getOrCreate()
      spark.conf.set("spark.sql.parquet.filterPushdown", "true")

      val parquetFile = spark.read.parquet(path)
      index = parquetFile.rdd.map(row => (row.getLong(0), row.getDouble(1), row.getDouble(2), row.getDouble(3), row.getDouble(4),
        row.getAs[Seq[Row]](5).map(r =>
          (r.getDouble(0), r.getDouble(1), r.getDouble(2),
            r.getDouble(3), r.getLong(4), r.getLong(5)))))

    } catch {
      case e:Exception => throw new Exception("failed to load index from " + path + "\n because of " + e.getMessage)
    }
  }

  var cached:Boolean = false

  def cache() = if (!cached && index != null) {index.cache(); cached = true} else Unit
  def unpersist() = if (cached && index != null) index.unpersist(false) else Unit

  /**
   * range query
   * @param window Query window
   * @return ids intersect with query window
   */
  def query(window:MBR):RDD[Long] = {
    if (index == null) throw new NullPointerException("index does not exist")
    index.filter(x => intersect(window, (x._2, x._3, x._4, x._5))).flatMap(x => serial.RTree.queryRtree(x._6, window, 0))
  }

  /**
   * join two indexes using synchronous traversal
   * @param that
   * @return candidate pairs (MBR intersect)
   */
  def join(that:DistIndex):RDD[(Long, Long)] = {
    if (this.index == null || that.index == null)
      throw new Exception("missing indexes")
    val left = this.index
    val right = that.index
    //TODO: make it to plane-sweep rather than cartesian product
    val results = left.cartesian(right).filter(x => intersect((x._1._2, x._1._3, x._1._4, x._1._5),
                                                              (x._2._2, x._2._3, x._2._4, x._2._5)))
    results.flatMap(x => serial.RTree.join(x._1._6, x._2._6))
  }
}
