package org.bah.apachespark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BigCount {
  def main(args:Array[String])={
    //Set Conf and Context for Spark
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)
    
    //Pull Lines from Big.txt
    val lines = sc.textFile("big.txt")
    
    //Pull Words from lines
    val newlines = lines.flatMap(lines=>lines.split(" "))
    val counts = newlines.map(word=>(word,1)).reduceByKey(_+_)
    
    //Save File
    counts.saveAsTextFile("BigCountSol.txt")
  }
  
}