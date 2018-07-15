package com.example.demo.scala

import org.apache.spark.SparkContext

import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import java.io._

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


object Analytics {
  
  val providersPathAndFile = "src/main/resources/providers.txt"
  val pathForVideoDataFiles = """src/main/resources"""
  
  val videoDataFileMinuteKeyLength = "min".length+1
  val videoDataFileVideoKeyLength = "video_id".length+1
  val videoDataFileDeviceIdKeyLength = "device_id".length+1
  val videoDataFileWatchTimeKeyLength = "time".length+1
  
  case class MinuteVideo(min: Integer, video: String)
  
  case class MinuteProvider(min: Integer, provider: String)
  
  case class MinuteProviderDevice(min: Integer, provider: String, device:String)

  
  def instance = this

  val conf = new SparkConf().setAppName("video").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  // Provider file as RDD
  val videoProvidersRdd = getVideoProviders(sc)
  val videoProvidersMap = sc.broadcast(getVideoProviderMap(videoProvidersRdd.collect()))
  

  // VideoData files as one single RDD
  val mergedVideoDataRdd = getMergedFiles().cache()

  // RDD containing the watch time for videos
  val watchTimeRdd = mergedVideoDataRdd.map(i => (
    MinuteVideo(Integer.valueOf(i.split(",")(0).substring(4)), i.split(",")(1).substring(9)), // KEY (minute, video)
    Integer.valueOf(i.split(",")(4).substring(5)))).cache() // VALUE (time)
    
   // RDD containing the watch time for (minute, provider, device)
  val minuteProviderDeviceRdd = mergedVideoDataRdd.map(i => {
    val providerForVideo = videoProvidersMap.value.get(i.split(",")(1).substring(videoDataFileVideoKeyLength)).getOrElse("")
    (MinuteProviderDevice(Integer.valueOf(i.split(",")(0).substring(videoDataFileMinuteKeyLength)), providerForVideo, 
        i.split(",")(3).substring(videoDataFileDeviceIdKeyLength)), // KEY (minute, provider, device)
    Integer.valueOf(i.split(",")(4).substring(videoDataFileWatchTimeKeyLength))) // VALUE (time) 
    }).cache()


  def getMaxVideoForMinute(min: Integer): String = {
    watchTimeRdd
      .filter { case (MinuteVideo(minute, video), _) => minute == min } // Filter for the minute we want
      .map(i => ((i._1).video, i._2))
      .reduceByKey(_ + _) // add up watch times for each video
      .reduce((acc, value) => { if (acc._2 < value._2) value else acc }) // Get Max Value
      ._1 // return the video
  }

  def getMaxProviderForMinute(min: Integer): String = {
    watchTimeRdd
      .filter { case (MinuteVideo(minute, video), _) => minute == min } // Filter for the minute we want
      .map{ case (MinuteVideo(minute, video), watchtime) => 
        val provider = getProviderForVideo(video)
        (MinuteProvider(minute, provider.getOrElse("")), watchtime) 
      }
      .map(i => ((i._1).provider, i._2)).reduceByKey(_ + _). // add up watch times 
      reduce((acc, value) => { if (acc._2 < value._2) value else acc }) // Get Max Value
      ._1 // return the provider
  }

  
  def getTotalWatchTimeForMinuteProviderDevice(min: Integer, prov:String, dev:String):Integer = {
    minuteProviderDeviceRdd
    .filter { case (MinuteProviderDevice(minute, provider, device), _) => minute == min && provider == prov && device==dev} 
    .reduceByKey(_ + _) // add up watch times 
    .reduce((x, y) => { if (x._2 < y._2) y else x })._2
  }
    
  def getVideoProviders(sc: SparkContext): RDD[(String, String)] = {
    val providersFileProviderKeyLength = "prov".length+1
    val providersFileVideoKeyLength = "vid".length+1
    
    val providerVideosRdd = sc.textFile(providersPathAndFile)
    providerVideosRdd.map { l =>
      val provider = (l.split(",")(0)).substring(providersFileProviderKeyLength)
      val video = (l.split(",")(1)).substring(providersFileVideoKeyLength)
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

    var files = getListOfFiles(pathForVideoDataFiles)
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
    rdd
  }

  def getProviderForVideo(video: String): Option[String] = {
    videoProvidersRdd.lookup(video).headOption
  }
  
  def getVideoProviderMap(videoProvidersArray:Array[(String, String)]):Map[String, String] = {
    videoProvidersArray.toMap
  }



}