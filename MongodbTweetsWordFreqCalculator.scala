import org.apache.spark.sql.SparkSession
import com.mongodb.client.model.Indexes
import com.mongodb.client.model
import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.{and, geoWithinCenterSphere, gte, lte}
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.functions.{coalesce, col, to_timestamp, udf}
import java.text.SimpleDateFormat
import scala.collection.JavaConversions.iterableAsScalaIterable


object MongodbTweetsWordFreqCalculator extends App {

  val spark = SparkSession.builder().master("local[*]")
    .appName("scalaExam")
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/tweetsDB.tweets")
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/tweetsDB.tweets").getOrCreate()

  import spark.implicits._

  //////////// Aseel Hamayel For BIG DATA COURSE /////////////////////////////////////////////////
  /////////// Read the json data /////////////////////////////////////////////////////////////////

  val tweetsJson = spark.read
    .json("/Users/aseelhamayel/Downloads/boulder_flood_geolocated_tweets/boulder_flood_geolocated_tweets.json")

  /////////// change  Timestamp to Date object  ////////////

  var reformatTimestampDF = tweetsJson.withColumn(
    "created_at",
    coalesce(to_timestamp(col("created_at"), "E MMM dd HH:mm:ss Z yyyy")))

  val timestamp = to_timestamp(reformatTimestampDF("created_at"), "MM-dd-yyyy HH:mm:ss")

  reformatTimestampDF = reformatTimestampDF.withColumn("created_at", timestamp)

  /////////// save the data from json to mongodb collection called tweets ///////////////////

  MongoSpark.save(reformatTimestampDF)

  /////// create indexes for geo-coordinates , created_at and text

  val mongoClient = new MongoClient()
  val database = mongoClient.getDatabase("tweetsDB")
  val collection = database.getCollection("tweets")
  //collection.createIndex(Indexes.geo2dsphere("geo.coordinates"))
  collection.createIndex(Indexes.geo2dsphere("coordinates.coordinates"))
  collection.createIndex(Indexes.ascending("created_at"))
  collection.createIndex(Indexes.text("text"))


  ////// Inputs /////////

  println("please enter the word you want :")
  val word = scala.io.StdIn.readLine()

  println("please enter r :")
  val radius = scala.io.StdIn.readInt()

  println("please enter longitude  :")
  val lon = scala.io.StdIn.readDouble()

  println("please enter latitude :")
  val lat = scala.io.StdIn.readDouble()

  println("please enter  start date as format  yyyy-MM-dd : ")
  val start = scala.io.StdIn.readLine()

  println("please enter  end date as format  yyyy-MM-dd : ")
  val end = scala.io.StdIn.readLine()

  val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val startDate = date.parse(start)

  val endDate = date.parse(end)


  //////////////// using mongodb library by sending a normal mongoDB query to filter by time and space//////////////

  val spatioTemporalFilter = collection.find(and(model.Filters.regex("text", word), gte("created_at", startDate), lte("created_at", endDate),
    geoWithinCenterSphere("coordinates.coordinates", lon, lat, radius)))

  val finalSpatioTemporalFilter = spatioTemporalFilter.toArray

  ////////////////////// Find the frequency of word in the filtered tweets /////////////////
  val textOfTweets = finalSpatioTemporalFilter.map(tweet => tweet.getString("text"))

  val finalCount = textOfTweets.flatMap(line => line.split(" ")).aggregate(0)(
    (counter, element) => if (element == word) counter + 1 else counter, _ + _)

  println("********************************************************")
  println("The the number of occurrences of word you entered = " + finalCount)

  /////// using MongoSpark, by collecting tweets and filtering them spatio-temporally using dataframe apis.///////////

  def within(radius: Double) = udf((lat: Double, long: Double) => ???)

  val mongoDBDF = MongoSpark.load(spark)

  val newMongoDF = mongoDBDF.where(within(radius)($"lat", $"long")).toDF()

  val newDF = newMongoDF.filter(mongoDBDF("text").contains(word))
    .filter(mongoDBDF("created_at").geq(startDate) && mongoDBDF("created_at").leq(startDate))

  val textOfTweetsDF = newDF.select("text").map(element => element.toString()).collect().toArray

  val finalCountDF = textOfTweetsDF.flatMap(line => line.split(" "))
    .aggregate(0)((counter, element) => if (element == word) counter + 1 else counter, _ + _)


  println("********************************************************")
  println("The the number of occurrences of word you entered = " + finalCountDF)


}

