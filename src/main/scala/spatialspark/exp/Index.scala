package spatialspark.exp

import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.SparkSession
import spatialspark.index.DistIndex

/**
 * Created by Simin You on 7/27/15.
 */
object Index {

  val usage = """
    Standalone Implementation of Build Spatial Index on Spark using DataFrame
    Usage: rangequery  --input input
                       --geom geometry index for input data (default 0)
                       --separator field separator (default TAB)
                       --output output location
                       --help
              """


  case class Geom(rid:Long, geom:String)

  def main(args: Array[String]) {
    if (args.length == 0) println(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "--help" :: tail =>
          println(usage)
          sys.exit(0)
        case "--input" :: value :: tail =>
          nextOption(map ++ Map('input -> value), tail)
        case "--geom" :: value :: tail =>
          nextOption(map ++ Map('geom -> value.toInt), tail)
        case "--output" :: value :: tail =>
          nextOption(map = map ++ Map('output -> value), list = tail)
        case "--conf" :: value :: tail =>
          nextOption(map = map ++ Map('conf -> value), list = tail)
        case option :: tail => println("Unknown option "+option)
          sys.exit(1)
      }
    }
    val options = nextOption(Map(),arglist)

    val spark = SparkSession
      .builder()
      .appName("Build Index")
      .getOrCreate()

    //.setMaster("local[4]")
    //.setSparkHome("/Users/you/spark-1.1.0")
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrator", "me.simin.spark.spatial.util.KyroRegistrator")
    val inputFile = options.getOrElse('input, "").asInstanceOf[String]
    val outputFile = options.getOrElse('output, "").asInstanceOf[String]
    val geomIdx = options.getOrElse('geom, 0).asInstanceOf[Int]
    val separator = options.getOrElse('separator, "\t").asInstanceOf[String]
    val indexConf = options.getOrElse('conf, "0.1:32:32").asInstanceOf[String]

    val sampleRatio = indexConf.split(":").apply(0).toDouble
    val dimX = indexConf.split(":").apply(1).toInt
    val dimY = indexConf.split(":").apply(2).toInt

    val timerBegin = System.currentTimeMillis()

    val inputData = spark.sparkContext.textFile(inputFile).map(x => (new WKTReader).read(x.split(separator).apply(geomIdx)))
    val inputDataWithId = inputData.zipWithIndex().map(_.swap)
    val inputMBRWithId = inputDataWithId.map(x => {
      val env = x._2.getEnvelopeInternal
      (x._1, (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY))
    })
    inputMBRWithId.cache()
    val index = new DistIndex(inputMBRWithId.map(x => (x._2, x._1)), ratio = sampleRatio, xDim = dimX, yDim = dimY)

    import spark.implicits._
    val inputDataWithPartId = inputDataWithId.map(x => Geom(x._1, x._2.toText)).toDF()

    inputDataWithPartId.write.parquet(outputFile)

    index.save(outputFile)

    val timerEnd = System.currentTimeMillis()
    println("total time: " + (timerEnd - timerBegin) + " ms")

  }
}
