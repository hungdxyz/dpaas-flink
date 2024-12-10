

import org.apache.flink.api.scala._


object ex29_ScalaHello {
  def main(args: Array[String]) {



    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val data = env.readTextFile("/Users/swethakolalapudi/flinkJar/courses.txt");

    val courseLengths = data.map { _.toLowerCase.split("\\W+") }
      .map { x => (x(2),x(1).toInt) }
      .groupBy(0)
      .sum(1)

    // execute and print result
    courseLengths.print()

  }
}
