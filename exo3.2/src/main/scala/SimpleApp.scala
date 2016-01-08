import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def decoupage(ligne : Array) {
	val it = Iterator(ligne)
	var r = new Array()
	var c = it.next
	var ancien = c

	while (it.hasNext()) {
		c = it.next
		r.add(c,ancien)
		ancien = c 
	}
	return r
}

  def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Spark Integrale")
        val spark = new SparkContext(conf)
 	
	val textFile = spark.textFile("miser.txt")
	var c = 0;

	//Array("Hklgr bnj , yui gers. fes").map( s => s.split('.')).flatten.map(s => s.split(',')).flatten

	//val texts = textFile.flatMap(line => line.split(',')).flatMap(line => line.split('.')).flatMap(line  => line.split('!')).flatMap(line  => line.split('?')).flatMap(line => line.split('.'))
	
	val mots = textFile.map(line => decoupage(line.split(" ")))
	

	mots.saveAsTextFile("result.txt")
	
	//val counts = textFile.flatMap(line => line.split(" "))
        //         .map(word => (word, 1))
        //         .reduceByKey(_ + _)
	//counts.saveAsTextFile("hdfs://...") 
  }
}
