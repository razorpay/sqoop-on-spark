package io.delta.connectors.spark.JDBC

import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption}

object ImportRunner {

  def main(args: Array[String]): Unit = {
    val config: ImportRunnerConfig = new ImportRunnerConfig(args)

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("sql-delta-import")
      .getOrCreate()

    val importConfig = ImportConfig(
      config.source(),
      config.destination(),
      config.splitBy(),
      config.chunks())

    val transform = new DataTransform(Seq.empty)

    val importer: JDBCImport = JDBCImport(
      jdbcUrl = config.jdbcUrl(),
      importConfig = importConfig,
      dataTransform = transform)

    importer.run()
  }
}

class ImportRunnerConfig(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner("\nOptions:\n")
  footer(
    """Usage:
      |spark-submit {spark options} --class "io.delta.connectors.spark.JDBC.ImportRunner" sql-delta-import.jar OPTIONS
      |""".stripMargin)

  override def mainOptions: Seq[String] = Seq("jdbcUrl", "source", "destination", "splitBy")

  val jdbcUrl: ScallopOption[String] = opt[String](required = true)
  val source: ScallopOption[String] = opt[String](required = true)
  val destination: ScallopOption[String] = opt[String](required = true)
  val splitBy: ScallopOption[String] = opt[String](required = true)
  val chunks: ScallopOption[Int] = opt[Int](default = Some(10))

  verify()
}
