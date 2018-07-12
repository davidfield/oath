package com.example.demo.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import java.io._

object Analytics {

  def instance = this

  val conf = new SparkConf().setAppName("video").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  // Provider file as RDD
  val videoProvidersRdd = getVideoProviders(sc)
  videoProvidersRdd.foreach(println)

  // VideoData files as one single RDD
  val mergedVideoDataRdd = getMergedFiles().cache()

  // RDD containing the watch time for videos
  val watchTimeRdd = mergedVideoDataRdd.map(i => (
    MinVideo(Integer.valueOf(i.split(",")(0).substring(4)), i.split(",")(1).substring(9)), // KEY (minute, video)
    Integer.valueOf(i.split(",")(4).substring(5)))) // VALUE (time)

  case class MinVideo(min: Integer, video: String)
  
  case class MinProvider(min: Integer, provider: String)

  def getMaxVideoForMinute(min: Integer): String = {
    watchTimeRdd
      .filter { case (MinVideo(minute, video), _) => minute == min } // Filter for the minute we want
      .map(i => ((i._1).video, i._2)).reduceByKey(_ + _). // add up watch times for each video
      reduce((acc, value) => { if (acc._2 < value._2) value else acc }) // Get Max Value
      ._1 // return the video
  }

  def getMaxProviderForMinute(min: Integer): String = {
//    val provider = videoProvidersRdd.lookup(video).headOption.get
    watchTimeRdd
      .filter { case (MinVideo(minute, video), _) => minute == min } // Filter for the minute we want
    .map{ case (MinVideo(minute, video), watchtime) => 
      val provider = getProviderForVideo(video)
      (MinProvider(minute, provider.getOrElse("")), watchtime) 
      }
      .map(i => ((i._1).provider, i._2)).reduceByKey(_ + _). // add up watch times for each video
      reduce((acc, value) => { if (acc._2 < value._2) value else acc }) // Get Max Value
      ._1 // return the video
  }

  def getVideoProviders(sc: SparkContext): RDD[(String, String)] = {
    val providerVideosRdd = sc.textFile("src/main/resources/providers.txt")
    providerVideosRdd.map { l =>
      val provider = (l.split(",")(0)).substring(5)
      val video = ((l.split(",")(1)).substring(4))
      (video, provider)
    }
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getMergedFiles(): RDD[String] = {
    var rdd: RDD[String] = null;
    var first: Boolean = true

    var files = getListOfFiles("""src/main/resources""")
    files.foreach(println)

    files = files.filter(f => f.getName().startsWith("videodata"))
    for (file <- files) {
      if (first) {
        first = false
        rdd = sc.textFile(file.getAbsolutePath);
      } else {
        rdd = rdd.union(sc.textFile(file.getAbsolutePath))
      }
    }
    rdd.foreach(println)
    rdd
  }

  def getProviderForVideo(video: String): Option[String] = {
//    videoProvidersRdd.lookup(video).headOption
    val filteredRdd = videoProvidersRdd.filter{ case(v,p) => v==video}
    filteredRdd.count() match {
      case 1 => Some(filteredRdd.values.first)
      case 0 => None
      case x if (x>1) => None
    }
  }

  //def getCumWatchTimeRdd() {
  //mergedWatchStatisticsRdd.map(line => line.split(“,”))
  //.map( case(m, v, u,d,w) => (m, getProviderForVideo(v,) w,d)
  //.map { case (m, p, d) => ((m, p, d), w) }
  //.reduceByKey(_ + _)
  //}
  //
  //def getCumulativeWatchTime(min:Integer, provider:String, device:String) {
  //      cumWatchTimeRdd = mergedWatchStatisticsRdd.filter(( case( `min`, `provider`, `device`, w))
  //     cumWatchTimeRdd.count() match {
  //     case 1 => cumWatchTimeRdd.first().map((p,v) => Some(p))
  //     case 0 => {
  //        logger.error(s”Entry for min:$min, provider:$provider, device:$device,  not found”)
  //        None
  // }

}