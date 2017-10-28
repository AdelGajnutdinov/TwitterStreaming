import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.sql.SparkSession

object TwitterStreamingLanguageClassifier {
  def main(args: Array[String]): Unit = {

    val jsonFile = "C:\\dataForScalaProjects\\tweets_[0-9]*\\part-00000";
    val modelOutput = "C:\\dataForScalaProjects\\kMeans";

    val conf = new SparkConf()
    conf.setAppName("spark-sreaming")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    //in order to avoid IO.Exception
    System.setProperty("hadoop.home.dir", "C:\\winutil\\")

    //Read json file to DF
    val tweetsDF = sparkSession.read.json(jsonFile)

    //Show thw scheme of DF
    tweetsDF.printSchema();

    //Select only the "name" column
    val tweets = tweetsDF.select("text").rdd.map(s => s.toString())

    //Get the features vector
    val features = tweets.map(s => featurize(s))

    val numClusters = 10
    val numIterations = 40

    // Train KMenas model and save it to file
    val model: KMeansModel = KMeans.train(features, numClusters, numIterations)
    model.save(sparkSession.sparkContext, modelOutput)
  }

  def featurize(s: String) = {
    val numFeatures = 1000
    val tf = new HashingTF(numFeatures)
    tf.transform(s.sliding(2).toSeq)
  }
}
