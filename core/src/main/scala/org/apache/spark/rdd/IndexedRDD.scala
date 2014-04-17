package org.apache.spark.rdd
import scala.reflect.ClassTag
import org.apache.spark.{Partition, TaskContext}
import scala.util.Sorting
import collection.mutable.HashMap
import org.apache.spark.SparkContext

class IndexedRDD[K: ClassTag](prev: RDD[K]) 
    extends RDD[K](prev) {
  
  // Initializing the number of partitions to -1 
  private var _numPartitions = -1
 
  // Key Map which stores the index of the key, it is used to get the index of the partition in which the key resides
  private var keyMap: HashMap[String, Int] = new HashMap[String, Int]() // Maps string to index
  
  // Pair class is used to compare pairs of values together 
  case class Pair(key:String, value:String)
  
  // Defines a range of partitions
  class PartitionRange(start: String, end: String){
    def start () : String = start
    def end () : String = end 
  }
  
  def string2Int(s: String): Int = augmentString(s).toInt
  
  // This function defines an ordering over the pair of keys (Instance of a Pair Class)
  object PairOrdering extends Ordering[Pair] {
      def compare(a: Pair, b: Pair) = a.key.toString() compare b.key.toString()
  }

  // Array which contains the sorted results
  private var _array = Array[Pair]()
  
  // Flag which stores whether the elements are sorted or not 
  private var _doSort = false
  
  var _fastIndex = Array[String]()
  
  // Initializing to get the number of partitions
  def initNumPartitions () : Int = {
    if (_numPartitions == -1)
       _numPartitions  = partitions.size 
    _numPartitions
  }
  
  // Sorts a given RDD into an RDD using the keys in the array
  def sortByKey() : RDD[(String, String)] = {
      _array = prev.map {case (key, value) => new Pair(key.toString, value.toString)}.collect() // Collecting the elements of this RDD into an array 
      Sorting.quickSort(_array)(PairOrdering) // Sorting the elements of this array
      // default value of number of partitions set to 1
      _numPartitions = 1
      // this should be removed by understanding the code for parallelizing  - has to be improved
      this.context.parallelize(_array, _numPartitions).map(s => (s.key.toString(), s.value.toString())) // Parallelize the given array and converts it into an RDD sorted by keys      
  }
   
  var self: RDD[(String, String)] = prev.map {case (key, value) => (key.toString, value.toString)} 
  self
  var rangePart: Array[PartitionRange] = Array[PartitionRange]()
  
		  				/* Member Method Definitions Below */  
  
    // Builds the index on the keyMap
    def buildIndex(): HashMap[String, Int] = {    
     var index = 0
     _array.foreach (s => {
         keyMap.put(s.key.toString, index)
         index = index + 1
       }
     )
     println("keymap-size: " + keyMap.size)
     keyMap
   }
  
  // Gets the range of keys on each of the partitions
  /** The basic idea is to get the largest and the smallest key on each of the partition.
   *  This index is stored on the driver program itself. 
   *  Thereafter run jobs based on the start and end location.
   */
  def rangePartitions(flag: Boolean) : Array[String] = {
    var range = prev.getSC.runJob(self, (iter: Iterator[(String, String)]) => {
    
    var smallestKey = ""
    var currentKey = ""
    var largestKey = ""
      while (iter.hasNext) {
        if((currentKey compare smallestKey) < 0 || (smallestKey == ""))
          smallestKey = currentKey
         if((currentKey compare largestKey) > 0)
           largestKey = currentKey
        currentKey = iter.next()._1
      }
      new String(smallestKey + "," + largestKey)
    }, 0 until self.partitions.size, flag)
    range
  }
  
  
  // Gets the range of keys on each of the partitions
  /** The basic idea is to get the largest and the smallest key on each of the partition.
   *  This index is stored on the driver program itself. 
   *  Thereafter run jobs based on the start and end location.
   */
  def rangePartitionsInt(flag: Boolean) : Array[String] = {
    var range = prev.getSC.runJob(self, (iter: Iterator[(String, String)]) => {
    
    var smallestKey = "0"
    var currentKey = "0"
    var largestKey = "0"
      while (iter.hasNext) {
        if(string2Int(currentKey) < string2Int(smallestKey) || smallestKey == "0")
          smallestKey = currentKey
         if(string2Int(currentKey) > string2Int(largestKey))
           largestKey = currentKey
        currentKey = iter.next()._1
      }
      new String(smallestKey + "," + largestKey)
    }, 0 until self.partitions.size, flag)
    range
  }
  
  /** The assumption here is that the partitions are sorted. 
   *  Then we can build a faster index.
   *  This function builds an index based on the initial values of each of the partition.
   *  The fastIndex maps <partitionIndex, startKey>
   */
  
  def indexPartitions() : Array[String] = {
    _fastIndex = prev.getSC.runJob(self, (iter: Iterator[(String, String)]) => {
      iter.next()._1
    }, 0 until self.partitions.size, true)
    _fastIndex
  }
  
  def getIndexRangeSortedInt(key: String) : (Int, Int) = {
    var startIndex = 0
    var endIndex = 0
    var startSet = false
    var endSet = false
    var index = 0 
    _fastIndex.foreach(s=> {
      if (string2Int(key) < string2Int(s)){
        if(endSet == false){
          endIndex = index
          endSet = true
        }
      } else if (string2Int(key) >= string2Int(s)){
          if(startSet == false){
             startIndex = index+1
             startSet = true
          }
      }
      index = index + 1
    })
    if (endSet == false){
      endIndex = index-1
    }
    (startIndex, endIndex)
  }  
 
  
  def buildIndexNoSort(): HashMap[String, Int] = {
     var index = 0     
     self.foreach (s => {
         keyMap.put(s._1, index)
         index = index + 1
       }
     )
     println("keymap-size: " + keyMap.size)
     keyMap
  }

  // Searches within a key range within the RDD set  
   /*def searchByKeyRange(Key: String): Array[String] = {
    var range = getIndexRangeSortedInt (Key)
    var index1 = range._1var index 2 = range._2
    println("index1:" + index1)
    println("index2:" + index2)
    val array = prev.getSC.runJob(self, (iter: Iterator[(String, String)]) => {
      var stringList = List[String]()
      var result: (String, String) = ("", "")
      while (iter.hasNext) {        
        result = iter.next()
        println("result:" + result)
        if((result._1 compare Key1) >= 0 && (result._1 compare Key2) <= 0)
          stringList = stringList :+ result._2
      }      
      stringList.toArray
    }, index1 until index2 + 1, false) // Can we do an efficient way to bind the array of array of strings  
    // converting to an array of strings
    var b = Array[String]() 
    array.foreach(a => {a.foreach(str => b = b:+ str)})
    b
  }*/
  
  // Searches for a specific key within the RDD set 
   def searchByKey(Key:String, flag: Boolean) : Array[Array[String]] = {
    val range = getIndexRangeSortedInt(Key)
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

  // Gets the partition ranges for a specific string 
 /* def getPartitionRange(key: String) : PartitionRange = {    
    var startIndex = -1
    var endIndex = -1
    
    keyMap.foreach (s => {
       if ()    
    })
    val location = keyMap(key) // could do a binary lookup on the map, if only a subset of keys is stored  
    val partitionSize = keyMap.size/_numPartitions // TODO need to change the size function here   
    val partitionIndex = location/partitionSize // TODO need to find out how do we get the number of elements per partition
    partitionIndex
  }*/

   // Getter Method for the keyMap
   def getMap: HashMap[String, Int] = {
     keyMap
   }
   
   // Needs to be changed
   override def getPartitions: Array[Partition] = firstParent[K].partitions
   
   // Needs to be changed - may remain the same 
   override def compute(split: Partition, context: TaskContext) =
    firstParent[K].iterator(split, context)
}