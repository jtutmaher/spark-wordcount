package org.bah

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

package object apachespark {
  //Main
  def main(args:Array[String])={
    
    //Generate Spark Conf and Context
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    
    //Load txt file
    val lines = sc.textFile("lunch.txt")
    val pairs = lines.map(s=> (s,1))
    val counts = pairs.reduceByKey((a,b)=>a+b)
    
    counts.saveAsTextFile("lunchCount.txt")
    
  }
}