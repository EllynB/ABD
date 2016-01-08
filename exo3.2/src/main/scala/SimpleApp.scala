import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Spark Integrale")
        val spark = new SparkContext(conf)
 	
	val textFile = spark.textFile("miser.txt")
	var c = 0;

	//Array("Hklgr bnj , yui gers. fes").map( s => s.split('.')).flatten.map(s => s.split(',')).flatten

	val texts = textFile.flatMap(line => line.split(',')).flatMap(line => line.split('.')).flatMap(line  => line.split('!')).flatMap(line  => line.split('?')).flatMap(line => line.split('.'))
	val mots = texts.map(line => line
	texts.saveAsTextFile("result.txt")
	
	//val counts = textFile.flatMap(line => line.split(" "))
        //         .map(word => (word, 1))
        //         .reduceByKey(_ + _)
	//counts.saveAsTextFile("hdfs://...") 
  }
}
