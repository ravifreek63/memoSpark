package org.apache.spark.rdd
import scala.reflect.ClassTag
import org.apache.spark.{Partition, TaskContext}
import scala.util.Sorting
import collection.mutable.HashMap
import org.apache.spark.SparkContext


class IndexRDD [K: ClassTag](prev: RDD[K]) 
    extends RDD[K](prev)  {
  
  var self: RDD[(String)] = prev.map(key => key.toString())
  var _compressedMap: HashMap[String, Array[Int]] = HashMap[String, Array[Int]]()
  private val _intSize = 32
  self
    
  def getBitRep (offset: Int): Int = {
    1 << (_intSize - offset)		
  }
  
  // Compressed Key Index - Supports word count, by compressing keys 
  def compressedKeyIndex(flag: Boolean, keySize: Int, numPartitions: Int) :  HashMap[String, Array[Int]] = {    
    var compressedIndex = prev.getSC.runJob(self, (iter: Iterator[(String)]) => {
      var subWord = ""
      var wordList = List[String]()  
        while(iter.hasNext){
    	  var line = iter.next().toString
    	  var words = line.split("\\s+")
    	  words.foreach (word => {
    	    if (word.size <= keySize)
    	      subWord = word
    	     else 
    	      subWord = word.substring(0, keySize-1)
    	    wordList = wordList :+ subWord
    	  })    	  
      } 
      wordList.distinct
    }, 0 until self.partitions.size, flag)
    var map = HashMap[String, Array[Int]]()
    var index = -1
    // assertion that _compressedIndex.size == numPartitions
    compressedIndex.foreach (wordList => {
      index = index + 1
      var bitRep = getBitRep(index)
      val indexInArray = index / _intSize
      wordList.foreach (word => {
         // updateMap(word, getBitRep(index))
         if (map.contains(word)){
    	    var indexArray = map(word)
            indexArray(indexInArray) = indexArray(indexInArray) | bitRep
    	    map.put(word, indexArray)
    	  } else {
    	    val array = Array[Int](numPartitions)
    	    array(indexInArray) = bitRep
    	    map.put(word, array)
    	  }
      })
    })
    _compressedMap
  }
  
  
  
  // Gets the list of partitions over which the key is exists 
  def getListPartitions (intRange: Int, offset: Int) : List[Int] = {
    var list = List[Int]()
    for (count <- 0 to (_intSize-1)){
      if ((intRange & getBitRep(count)) > 0)
        list = list :+ (count+offset)
    } 
    list
  }
  
  
  def getCount(Key: String, KeySize: Int, flag: Boolean = true) : Int = {
    var count = 0
    var size = Key.size
    var subWord = ""
    if (Key.size <= KeySize)
      subWord = Key
    else  
      subWord = Key.substring(0, KeySize-1)
    var listPartitions = List[List[Int]]()
    if (_compressedMap.contains(subWord)){
    	val array = _compressedMap(subWord)
    	var index = 0
    	array.foreach(intRange => {
    		listPartitions = listPartitions :+ getListPartitions(intRange, index)
    		index = index + _intSize
    	})
   // flattening the sequence to get the list of partitions in which the word exists   
   val seqPartitions = listPartitions.flatten 
   // run count job now
	   count = prev.getSC.runJob (self, (iter: Iterator[(String)]) => {
	      var result = 0
	      while (iter.hasNext) {	
	        if (iter.next() contains Key) {
	          result += 1
	        }	        
	      }
	      result
	   }, seqPartitions, flag).sum
    }
    count
  }
     // Needs to be changed
   override def getPartitions: Array[Partition] = firstParent[K].partitions
   
   // Needs to be changed - may remain the same 
   override def compute(split: Partition, context: TaskContext) =
    firstParent[K].iterator(split, context)
}