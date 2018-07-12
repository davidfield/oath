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
  // VideoData files as one single RDD
  val mergedVideoDataRdd = getMergedFiles().cache()

  // RDD containing the watch time for videos  
  val watchTimeRdd = mergedVideoDataRdd.map(i=>(
      MinVideo(Integer.valueOf(i.split(",")(0).substring(4)), i.split(",")(1).substring(9)), // KEY
      Integer.valueOf(i.split(",")(4).substring(5)))) // VALUE
  
  
  case class MinVideo(min:Integer, video:String)
 
  def getMaxVideoForMin(min:Integer):String = {
    watchTimeRdd
    .filter{ case(MinVideo(minute, video), _) => minute==min} // Filter for the minute we want
    .map(i=> ((i._1).video, i._2)).reduceByKey(_+_). // add up watch times for each video
    reduce((acc,value) => { if(acc._2 < value._2) value else acc}) // Get Max Value
    ._1 // return the video
  }


  def getVideoProviders (sc:SparkContext):RDD[(String, String)] = {
  val providerVideosRdd = sc.textFile("src/main/resources/providers.txt")
  providerVideosRdd.map(l=>((l.split(",")(0)).substring(5),((l.split(",")(1)).substring(4))))
  }
  

def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
    } else {
        List[File]()
    }
}


def getMergedFiles():RDD[String] = {
   var rdd:RDD[String] = null;
   var first :Boolean = true
   
  var files = getListOfFiles("""src\main\resources""");
   files = files.filter(f => f.getName().startsWith("videodata"))
  for ( file <- files) {
     if (first) {
         first = false
         rdd = sc.textFile(file.getAbsolutePath);
         println("====="+file.getAbsolutePath)
      } else {
        println("*******"+file.getAbsolutePath)
         rdd = rdd.union(sc.textFile(file.getAbsolutePath))
    }
  }
  rdd.foreach(println)
  rdd
}


def getProviderForVideo(video:String) :Option[String] = {
   val filteredRdd = videoProvidersRdd.filter{case(p,v) => v==video}
   filteredRdd.count() match {
     case 1 => Some(filteredRdd.keys.first)
     case 0 => {
        None 
     }
     case x if (x>1) => {
        None 
     }
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