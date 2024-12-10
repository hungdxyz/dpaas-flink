


import org.apache.flink.api.scala._
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.recommendation.ALS


object ex30_ML {
  def main(args: Array[String]) {



    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val rawUserArtistData = env.readCsvFile[(Int, Int, Double)]("/Users/swethakolalapudi/flinkJar/audio/user_artist_data.txt","\n"," ");

    val als = ALS()
      .setIterations(10)
      .setNumFactors(10)
      .setBlocks(100)
      .setTemporaryPath("/Users/swethakolalapudi/flinkJar/temp")

    val parameters = ParameterMap()
      .add(ALS.Lambda, 0.9)
      .add(ALS.Seed, 42L)

    als.fit(rawUserArtistData, parameters)

    val testingDS: DataSet[(Int, Int)] = env.readCsvFile[(Int, Int)]("/Users/swethakolalapudi/flinkJar/audio/test_data.txt","\n"," ")

    val predictedRatings = als.predict(testingDS)

    predictedRatings.print()


  }
}
