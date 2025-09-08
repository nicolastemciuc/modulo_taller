// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import java.io.{ByteArrayInputStream, DataInputStream}

object KMeansExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeansExample")
    val sc   = new SparkContext(conf)

    // Pass the IDX images path as arg(0) if you like
    val path = if (args.nonEmpty) args(0) else "/mnt/datasets/kmeans/t10k-images.idx3-ubyte"

    // Read the single binary IDX file on executors.
    // IMPORTANT: Read the entire file into a byte array FIRST so we can parse eagerly
    // and avoid "Stream is closed!" issues from lazy iterators.
    val parsedData = sc.binaryFiles(path, minPartitions = 1).flatMap { case (_, pds) =>
      val bytes = pds.toArray() // read fully on the executor
      val in = new DataInputStream(new ByteArrayInputStream(bytes))
      def readInt(): Int = in.readInt() // big-endian

      // Parse header
      val magic     = readInt()
      require(magic == 2051, s"Expected magic 2051 for images, got $magic")
      val numImages = readInt()
      val numRows   = readInt()
      val numCols   = readInt()
      val imageSize = numRows * numCols

      val buf = new Array[Byte](imageSize)
      val out = new Array[Vector](numImages)
      var i   = 0
      while (i < numImages) {
        in.readFully(buf)
        // unsigned byte -> double in [0,1]
        val doubles = new Array[Double](imageSize)
        var j = 0
        while (j < imageSize) {
          doubles(j) = (buf(j) & 0xFF).toDouble / 255.0
          j += 1
        }
        out(i) = Vectors.dense(doubles)
        i += 1
      }
      in.close()
      out.toIndexedSeq // eager, safe to return
    }.cache()

    // Sanity check forces materialization
    val n = parsedData.count()
    require(n > 0, s"No images parsed from $path")

    // K-Means (good starting point for MNIST)
    val numClusters   = 10
    val numIterations = 40
    val model = KMeans.train(parsedData, numClusters, numIterations)

    val WSSSE = model.computeCost(parsedData)
    println(s"Images: $n | k=$numClusters iters=$numIterations | WSSSE=$WSSSE")

    // Quick cluster size summary
    val clusterSizes = parsedData.map(v => model.predict(v)).countByValue().toSeq.sortBy(_._1)
    println("Cluster sizes:")
    clusterSizes.foreach { case (cid, cnt) => println(f"  $cid%2d -> $cnt%6d") }

    sc.stop()
  }
}
// scalastyle:on println
