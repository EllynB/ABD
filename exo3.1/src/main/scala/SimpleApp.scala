import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
	var a : Float =1
	var b : Float =10
	var n=500
	var h : Float =(b-a)/n
	val conf = new SparkConf().setAppName("Spark Integrale")
	val spark = new SparkContext(conf)
	val slices = 2 
	
	val t1 = System.currentTimeMillis()	
	var res = spark.parallelize(1 until n-1, slices).map { i =>
		1/(a+i*h)
	}.reduce(_ + _)
	
	res=res*h
	
	println("\n\n"+ res +" temps en ms =  " + (System.currentTimeMillis()-t1) +"\n")
	spark.stop()	
  }
}
