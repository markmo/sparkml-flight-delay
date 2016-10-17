import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTree, RandomForest}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by markmo on 18/10/2016.
  */
object SparkApp extends App {

  val conf = new SparkConf().setAppName("sparkml-flight-delay").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def parseFlight(str: String) = {
    val r = str.split(",")
    Flight(
      r(0).toInt - 1, r(1).toInt - 1, r(2), r(3), r(4).toInt, r(5), r(6),
      r(7), r(8), r(9).toDouble, r(10).toDouble, r(11).toDouble, r(12).toDouble,
      r(13).toDouble, r(14).toDouble, r(15).toDouble, r(16).toInt
    )
  }

  val dataPath = getClass.getResource("rita2014jan.csv").getPath
  val rdd = sc.textFile(dataPath)
  val flights = rdd.map(parseFlight).cache()

  val carrierIndex =
    flights
      .map(_.carrier).distinct
      .collect
      .zipWithIndex
      .toMap

  val originIndex =
    flights
      .map(_.origin).distinct
      .collect
      .zipWithIndex
      .toMap

  val destinationIndex =
    flights
      .map(_.destination).distinct
      .collect
      .zipWithIndex
      .toMap

  val features = flights map { flight =>
    val dayOfMonth = flight.dayOfMonth.toDouble
    val dayOfWeek = flight.dayOfWeek.toDouble
    val departureTime = flight.crsDepartureTime
    val arrivalTime = flight.crsArrivalTime
    val carrier = carrierIndex(flight.carrier)
    val elapsedTime = flight.crsElapsedTime
    val origin = originIndex(flight.origin).toDouble
    val destination = destinationIndex(flight.destination).toDouble
    val delayed = if (flight.departureDelayMinutes > 40) 1.0 else 0.0
    Array(delayed, dayOfMonth, dayOfWeek, departureTime, arrivalTime, carrier, elapsedTime, origin, destination)
  }

  val labeled = features map { x =>
    LabeledPoint(x(0), Vectors.dense(x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
  }
  val delayed = labeled.filter(_.label == 0).randomSplit(Array(0.85, 0.15))(1)
  val notDelayed = labeled.filter(_.label != 0)
  val all = delayed ++ notDelayed

  val splits = all.randomSplit(Array(0.7, 0.3))
  val (train, test) = (splits(0), splits(1))

  val categoricalFeaturesInfo = ((0 -> 31) :: (1 -> 7) :: (4 -> carrierIndex.size) :: (6 -> originIndex.size) :: (7 -> destinationIndex.size) :: Nil).toMap

  val model = DecisionTree.trainClassifier(
    input = train,
    numClasses = 2,
    categoricalFeaturesInfo,
    impurity = "gini",
    maxDepth = 9,
    maxBins = 7000
  )

  println(model.toDebugString)

  val predictions = test map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }

  val wrong = predictions filter {
    case (label, prediction) => label != prediction
  }

  val accuracy = 1 - (wrong.count.toDouble / test.count)

  println(s"accuracy model1: " + accuracy)

  val model2 = RandomForest.trainClassifier(
    input = train,
    numClasses = 2,
    categoricalFeaturesInfo,
    numTrees = 20,
    featureSubsetStrategy = "auto",
    impurity = "gini",
    maxDepth = 9,
    maxBins = 7000,
    seed = 1234
  )

  val predictions2 = test map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }

  val wrong2 = predictions filter {
    case (label, prediction) => label != prediction
  }

  val accuracy2 = 1 - (wrong.count.toDouble / test.count)

  println(s"accuracy model2: " + accuracy)

  sc.stop()

}

case class Flight(dayOfMonth: Int,
                  dayOfWeek: Int,
                  carrier: String,
                  tailNumber: String,
                  flightNumber: Int,
                  originId: String,
                  origin: String,
                  destinationId: String,
                  destination: String,
                  crsDepartureTime: Double, // crs - central reservation system
                  actualDepartureTime: Double,
                  departureDelayMinutes: Double,
                  crsArrivalTime: Double,
                  actualArrivalTime: Double,
                  arrivalDelayMinutes: Double,
                  crsElapsedTime: Double,
                  distance: Int)