// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.storage.StorageLevel
import java.io.{ByteArrayInputStream, DataInputStream}

object KMeansExample {
  // args: [path] [iters] [k] [minPartitions] [repeatN] [cpuBurnMs]
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeansExample")
    val sc   = new SparkContext(conf)

    val path          = if (args.length >= 1) args(0) else "/mnt/datasets/kmeans/t10k-images.idx3-ubyte"
    val iters         = if (args.length >= 2) args(1).toInt else 60
    val k             = if (args.length >= 3) args(2).toInt else 10
    val minParts      = if (args.length >= 4) args(3).toInt else 64
    val repeatN       = if (args.length >= 5) args(4).toInt else 4
    val cpuBurnMs     = if (args.length >= 6) args(5).toInt else 0

    def burn(ms: Int): Unit = {
      if (ms <= 0) return
      val until = System.nanoTime() + ms.toLong * 1000000L
      var x = 0L
      while (System.nanoTime() < until) { x += 1 }
      if (x == -1) println("")
    }

    val parsedOnce = sc.binaryFiles(path, minPartitions = math.max(1, minParts)).flatMap { case (_, pds) =>
      val bytes = pds.toArray()
      val in = new DataInputStream(new ByteArrayInputStream(bytes))
      def readInt(): Int = in.readInt()

      val magic     = readInt(); require(magic == 2051, s"Expected magic 2051, got $magic")
      val numImages = readInt()
      val numRows   = readInt()
      val numCols   = readInt()
      val imageSize = numRows * numCols

      val buf = new Array[Byte](imageSize)
      val out = new Array[Vector](numImages)
      var i   = 0
      while (i < numImages) {
        in.readFully(buf)
        val doubles = new Array[Double](imageSize)
        var j = 0
        while (j < imageSize) { doubles(j) = (buf(j) & 0xFF).toDouble / 255.0; j += 1 }
        out(i) = Vectors.dense(doubles)
        i += 1
      }
      in.close()
      out.toIndexedSeq
    }

    val multiplied = (1 until repeatN).foldLeft(parsedOnce.rdd) { (rdd, _) => rdd.union(parsedOnce.rdd) }
      .repartition(minParts)
      .mapPartitions { it => if (cpuBurnMs > 0) burn(cpuBurnMs); it }

    val parsedData = multiplied.persist(StorageLevel.MEMORY_AND_DISK)
    val n = parsedData.count()
    require(n > 0, s"No images parsed from $path")

    val model = KMeans.train(parsedData, k, iters)
    val WSSSE = model.computeCost(parsedData)
    println(s"Images(effective)=$n | k=$k iters=$iters | WSSSE=$WSSSE")

    val clusterSizes = parsedData.map(v => model.predict(v)).countByValue().toSeq.sortBy(_._1)
    println("Cluster sizes:"); clusterSizes.foreach { case (cid, cnt) => println(f"  $cid%2d -> $cnt%8d") }

    sc.stop()
  }
}
// scalastyle:on println
