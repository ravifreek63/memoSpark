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
  case class Pair(key:Any, value:Any)
  
  // This function defines an ordering over the pair of keys (Instance of a Pair Class)
  object PairOrdering extends Ordering[Pair] {
      def compare(a: Pair, b: Pair) = a.key.toString() compare b.key.toString()
  }

  // Array which contains the sorted results
  private var _array = Array[Pair]()
  
  // Flag which stores whether the elements are sorted or not 
  private var _doSort = false
  
  // Initializing to get the number of partitions
  def initNumPartitions () {
    if (_numPartitions == -1)
       _numPartitions  = partitions.size           
  }
  
  // Sorts a given RDD into an RDD using the keys in the array
  def sortByKey() : RDD[(String, String)] = {
      _array = prev.map {case (key, value) => new Pair(key, value)}.collect() // Collecting the elements of this RDD into an array 
      Sorting.quickSort(_array)(PairOrdering) // Sorting the elements of this array
      // default value of number of partitions set to 1
      _numPartitions = 1
      // this should be removed by understanding the code for parallelizing  - has to be improved
      this.context.parallelize(_array, _numPartitions).map(s => (s.key.toString(), s.value.toString())) // Parallelize the given array and converts it into an RDD sorted by keys      
  }
   
  var self: RDD[(String, String)] = sortByKey()
  self
  
  //initNumPartitions()
		  				
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
  
  

  // Searches within a key range within the RDD set  
   def searchByKeyRange(Key1: String, Key2: String): Array[String] = {
    val index1 = getPartitionIndex (Key1) // finds the initial partition index
    val index2 = getPartitionIndex (Key2) // finds the final partition index
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
  }
  
  // Searches for a specific key within the RDD set 
   def searchByKey(Key:String) : String = {
    val index = getPartitionIndex (Key) // find out the number of partitions
    println("partitionIndex:" + index)
    var result: (String, String) = ("", "")
    var array = prev.getSC.runJob(self, (iter: Iterator[(String, String)]) => {
      var result: (String, String) = ("", "")
      var flag = false
      while (!flag && iter.hasNext) {
        result = iter.next()
        println("resultPair:" +result._1 + "," + result._2)
        if(result._1 == Key)
          flag = true          
      }      
      result._2
    }, Seq(index), false)    
    if (array.size > 0)
      array(0)
      else 
        ""
  }

  // Gets the partitions location for a specific string 
  def getPartitionIndex(key: String) : Int = {    
    if (_numPartitions == -1){
       initNumPartitions ()       
    }     
    val location = keyMap(key) // could do a binary lookup on the map, if only a subset of keys is stored  
    val partitionSize = keyMap.size/_numPartitions // TODO need to change the size function here   
    val partitionIndex = location/partitionSize // TODO need to find out how do we get the number of elements per partition
    partitionIndex
  }

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