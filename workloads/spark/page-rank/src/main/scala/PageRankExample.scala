import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object PageRankExample {
  // args: [inputPath] [numIter] [partitions] [cpuBurnMs]
  def main(args: Array[String]): Unit = {
    val inputPath  = if (args.length >= 1) args(0) else "/mnt/datasets/twitch_gamers/large_twitch_edges.txt"
    val numIter    = if (args.length >= 2) args(1).toInt else 40
    val partitions = if (args.length >= 3) args(2).toInt else 256
    val cpuBurnMs  = if (args.length >= 4) args(3).toInt else 0

    val spark = SparkSession.builder.appName("PageRankExample").getOrCreate()
    val sc = spark.sparkContext

    def burn(ms: Int): Unit = {
      if (ms <= 0) return
      val until = System.nanoTime() + ms.toLong * 1000000L
      var x = 0L
      while (System.nanoTime() < until) { x += 1 }
      if (x == -1) println("")
    }

    val lines = sc.textFile(inputPath, partitions)
    val tokenized = lines.map(_.trim).filter(_.nonEmpty).map(_.split("[,\\s]+"))
    val edges: RDD[Edge[Int]] = tokenized.flatMap { arr =>
      if (arr.length >= 2 && arr(0).forall(_.isDigit) && arr(1).forall(_.isDigit)) {
        try Some(Edge(arr(0).toLong, arr(1).toLong, 1)) catch { case _: NumberFormatException => None }
      } else None
    }.repartition(partitions)
     .mapPartitions { it => burn(cpuBurnMs); it }

    val sample = edges.take(1)
    require(sample.nonEmpty, s"No valid edges parsed from $inputPath")

    val graph: Graph[Int, Int] = Graph.fromEdges(edges, defaultValue = 0)
      .persist(StorageLevel.MEMORY_AND_DISK)
    graph.edges.count()

    val ranks: VertexRDD[Double] = graph.staticPageRank(numIter).vertices

    val top10 = ranks.map { case (vid, rank) => (rank, vid) }.top(10).map { case (r, v) => s"$v\t$r" }
    println(s"Top 10 vertices by PageRank (iters=$numIter):"); top10.foreach(println)

    spark.stop()
  }
}
