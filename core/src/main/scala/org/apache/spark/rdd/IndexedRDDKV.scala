package org.apache.spark.rdd
import scala.reflect.ClassTag
import org.apache.spark.{Partition, TaskContext}
import scala.util.Sorting
import collection.mutable.HashMap
import org.apache.spark.SparkContext

class IndexedRDDKV[K: ClassTag](prev: RDD[K]) 
    extends RDD[K](prev) {
  
  // Initializing the number of partitions to -1 
  private var _numPartitions = -1
  
  private val keyTypeInt = 1
  private val keyTypeString = 2
  private val keyTypeTimeStamp = 3
 
  // Pair class is used to compare pairs of values together 
  case class Pair(key:String, value:String)
  
  // Defines a range of partitions
  class PartitionRange(start: String, end: String){
    def start () : String = start
    def end () : String = end 
  }
  
  def string2Int(s: String): Int = augmentString(s).toInt
  
  def isAllDigits(x: String) = x forall Character.isDigit
  
    def compareKeys (key1 : String, key2 : String) : Int  = {
    var keyType = -1
    if (isAllDigits(key1) == true)
      keyType = keyTypeInt
      else 
        keyType = keyTypeString
    var compareVal = -2
    if (keyType == keyTypeInt){
      if (string2Int(key1) < string2Int(key2))
       compareVal = -1        
       else if (string2Int(key1) > string2Int(key2)) 
        compareVal = 1 
        else 
         compareVal = 0
    } else if (keyType == keyTypeString){
    	compareVal = key1 compare key2
    } 
    compareVal
  }

  // This function defines an ordering over the pair of keys (Instance of a Pair Class)
  object PairOrdering extends Ordering[Pair] {
      def compare(a: Pair, b: Pair) = a.key.toString() compare b.key.toString()
  }

  // Flag which stores whether the elements are sorted or not 
  private var _doSort = false
  
  var _partitionIndex = Array[String]()
  var _rangePartitionIndex = Array[(String, String)]()
  
  // Initializing to get the number of partitions
  def initNumPartitions () : Int = {
    if (_numPartitions == -1)
       _numPartitions  = partitions.size 
    _numPartitions
  }
  
  var self: RDD[(String, String)] = prev.map {case (key, value) => (key.toString, value.toString)} 
  var rangePart: Array[PartitionRange] = Array[PartitionRange]()
  self
		  				/* Member Method Definitions Below */  
  

  /** The assumption here is that the partitions are sorted. 
   *  Then we can build a faster index.
   *  This function builds an index based on the initial values of each of the partition.
   *  The fastIndex maps <partitionIndex, startKey>
   */
  
  def indexPartitions() : Array[String] = {
    _partitionIndex = prev.getSC.runJob(self, (iter: Iterator[(String, String)]) => {
      iter.next()._1
    }, 0 until self.partitions.size, true)
    _partitionIndex
  }
  
  def rangePartitions(flag : Boolean): Array[(String, String)] = {        
    _rangePartitionIndex = prev.getSC.runJob(self, (iter: Iterator[(String, String)]) => {
      var smallest = ""
      var largest  = ""
      var current = ""
      if (iter.hasNext)
    	  current = iter.next()._1
      smallest = current
      largest = current 
      while (iter.hasNext){
        if (compareKeys(current, smallest) < 0) 
           smallest = current 
         else if (compareKeys(current, largest) > 0)
           largest = current
      }
      (smallest, largest)
    }, 0 until self.partitions.size, true)
    _rangePartitionIndex          
  }
  
  def partitionByRange(key: String) : Array[Int] = {
	var partitions = Array[Int]()
	var index = 0
	_rangePartitionIndex.foreach (s => {
	  if (compareKeys(key, s._1) >= 0 && compareKeys(key, s._2) <= 0){
	    partitions = partitions :+ index
	    index = index + 1
	  }
	})
	partitions
  }
  
  def partitionRanges(key: String) : (Int, Int) = {
    var startIndex = 0
    var endIndex = 0
    var startSet = false
    var endSet = false
    var index = 0 
    _partitionIndex.foreach(s=> {
      if (compareKeys(key, s) == -1){
        if(endSet == false){
          endIndex = index-1
          endSet = true
        }
      } else if (compareKeys(key, s) >= 0){          
             startIndex = index           
      }
      index = index + 1
    })
    if (endSet == false){
      endIndex = index-1
    }
    (startIndex, endIndex)
  }  
 
   def searchByKeyRangePartitioned(Key:String, flag: Boolean = true) : Array[Array[String]] = {
    val range = partitionByRange(Key)
    prev.getSC.runJob(self, (iter: Iterator[(String, String)]) => {
      var result: (String, String) = ("", "")
      var stringList = List[String]()
      while (iter.hasNext) {
        result = iter.next()        
        if(result._1 == Key)
          stringList = stringList :+ result._2
     }      
      stringList.toArray
    }, range, flag)        
  }

   
  // Searches for a specific key within the RDD set 
   def searchByKey(Key:String, flag: Boolean = true) : Array[Array[String]] = {
    val range = partitionRanges(Key)
    val index1 = range._1 // find out the number of partitions
    val index2 = range._2 // find out the number of partitions
    prev.getSC.runJob(self, (iter: Iterator[(String, String)]) => {
      var result: (String, String) = ("", "")
      var stringList = List[String]()
      while (iter.hasNext) {
        result = iter.next()        
        if(result._1 == Key)
          stringList = stringList :+ result._2
     }      
      stringList.toArray
    }, index1 until index2+1, flag)        
  }

  def unionRanges (Range1: (Int, Int), Range2: (Int, Int)) : (Int, Int) = {
    var start = -1
    var end = -1
    if (Range1._1 < Range2._1)
    	start = Range1._1
    	else 
    	  start = Range2._1
    if (Range1._2 > Range2._2)
    	end = Range1._2
    	else 
    	  end = Range2._2
   (start, end)
  } 
   
  def liesBetween (Key: String, SKey: String, LKey: String) : Boolean = {
    if (compareKeys(Key, SKey) >= 0 && compareKeys(Key, LKey) <=0)
     true
    else 
     false
  }
  
  def searchByKeyRange(Key1: String, Key2:String, flag:Boolean = true) : Array[Array[String]] = {
    val range1 = partitionRanges(Key1)
    val range2 = partitionRanges(Key2)
    val range = unionRanges(range1, range2)
      prev.getSC.runJob(self, (iter: Iterator[(String, String)]) => {
      var result: (String, String) = ("", "")
      var stringList = List[String]()
      while (iter.hasNext) {
        result = iter.next()
        if(liesBetween(result._1, Key1, Key2))
          stringList = stringList :+ result._2
     }      
      stringList.toArray
    }, range._1 until range._2+1, flag)        
  }  
  
   // Needs to be changed
   override def getPartitions: Array[Partition] = firstParent[K].partitions
   
   // Needs to be changed - may remain the same 
   override def compute(split: Partition, context: TaskContext) =
    firstParent[K].iterator(split, context)
}