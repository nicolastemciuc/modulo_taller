// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import java.io.DataInputStream

object KMeansExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeansExample")
    val sc   = new SparkContext(conf)

    // Pass path as arg(0), or default to a common location
    val path = if (args.nonEmpty) args(0) else "/mnt/datasets/kmeans/t10k-images.idx3-ubyte"

    // --- Load MNIST IDX images (binary, big-endian) into an RDD[Vector] ---
    // File structure:
    // int magic(2051), int numImages, int numRows(28), int numCols(28), then images as unsigned bytes
    val parsedData = sc.binaryFiles(path, minPartitions = 1).flatMap { case (_, pds) =>
      val in = new DataInputStream(pds.open())
      def readInt(): Int = in.readInt() // big-endian
      try {
        val magic     = readInt()
        require(magic == 2051, s"Expected magic 2051 for images, got $magic")
        val numImages = readInt()
        val numRows   = readInt()
        val numCols   = readInt()
        val imageSize = numRows * numCols

        val buf = new Array[Byte](imageSize)
        (0 until numImages).iterator.map { _ =>
          in.readFully(buf)
          // Convert unsigned byte -> double in [0,1]
          val doubles = buf.map(b => (b & 0xFF).toDouble / 255.0)
          Vectors.dense(doubles)
        }
      } finally {
        in.close()
      }
    }.cache()

    // Sanity check: make sure we have data
    val n = parsedData.count()
    require(n > 0, s"No images parsed from $path")

    // --- K-Means ---
    // MNIST: try k=10 (digits), 20–50 iterations is usually fine
    val numClusters   = 10
    val numIterations = 40

    // Optionally: KMeans|| init is default in recent Spark; older versions can set explicitly:
    // val model = new KMeans().setK(numClusters).setMaxIterations(numIterations).setInitializationMode("k-means||").run(parsedData)
    val model = KMeans.train(parsedData, numClusters, numIterations)

    val WSSSE = model.computeCost(parsedData)
    println(s"Images: $n  |  k=$numClusters  iters=$numIterations  |  WSSSE=$WSSSE")

    // Quick cluster size summary
    val clusterSizes = parsedData.map(v => model.predict(v)).countByValue().toSeq.sortBy(_._1)
    println("Cluster sizes:")
    clusterSizes.foreach { case (cid, cnt) => println(f"  $cid%2d -> $cnt%6d") }

    sc.stop()
  }
}
// scalastyle:on println
