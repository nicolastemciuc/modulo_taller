/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 */

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageRankExample {
  def main(args: Array[String]): Unit = {
    // Path can be passed as the first arg; defaults to your dataset
    val inputPath = if (args.nonEmpty) args(0) else "/mnt/datasets/twitch_gamers/large_twitch_edges.txt"

    val spark = SparkSession.builder
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    val sc = spark.sparkContext

    // --- Read as raw text and parse robustly (CSV or whitespace) ---
    // Accept lines like: "u,v" or "u v" (two integer IDs). Skip headers/garbage.
    val lines = sc.textFile(inputPath)

    // Split on comma OR any whitespace; keep rows with at least two numeric tokens
    val tokenized = lines.map(_.trim).filter(_.nonEmpty).map(_.split("[,\\s]+"))

    // Filter out header and malformed lines, parse to Edge[Int] with unit weight=1
    val edges: RDD[Edge[Int]] = tokenized.flatMap { arr =>
      if (arr.length >= 2 && arr(0).forall(_.isDigit) && arr(1).forall(_.isDigit)) {
        try {
          Some(Edge(arr(0).toLong, arr(1).toLong, 1))
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }

    // Safety check: fail fast if we parsed nothing
    val edgeSample = edges.take(1)
    require(edgeSample.nonEmpty, s"No valid edges parsed from $inputPath (check delimiter/header).")

    val graph: Graph[Int, Int] = Graph.fromEdges(edges, defaultValue = 0)

    // --- PageRank ---
    // Use convergence-based PageRank; adjust tolerance if you want more/less precision
    val tol = 1e-4
    val ranks: VertexRDD[Double] = graph.pageRank(tol).vertices

    // Show top 10 vertices by rank
    val top10 = ranks
      .map { case (vid, rank) => (rank, vid) }
      .top(10) // uses ordering on rank
      .map { case (rank, vid) => s"$vid\t$rank" }

    println("Top 10 vertices by PageRank:")
    top10.foreach(println)

    spark.stop()
  }
}
