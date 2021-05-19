/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.connectors.spark.jdbc

import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
 * Spark app that wraps functionality of JDBCImport and exposes configuration as command line args
 */
object ImportRunner extends App {

  val config = new ImportRunnerConfig(args)

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("sqoop-on-spark")
    .getOrCreate()

  val importConfig = ImportConfig(
    config.inputTable(),
    config.query(),
    config.boundaryQuery(),
    config.outputTable(),
    config.splitBy(),
    config.chunks(),
    config.partitionBy(),
    config.database()
  )

  val transforms = new DataTransforms(Seq.empty)

  JDBCImport(
    scope = config.scope(),
    importConfig = importConfig,
    dataTransforms = transforms
  ).run()
}

class ImportRunnerConfig(arguments: Seq[String]) extends ScallopConf(arguments) {
  override def mainOptions: Seq[String] =
    Seq("scope", "inputTable", "outputTable", "splitBy", "database")

  val scope: ScallopOption[String] = opt[String](required = true)
  val database: ScallopOption[String] = opt[String](required = true)
  val inputTable: ScallopOption[String] = opt[String](required = true)
  val query: ScallopOption[String] = opt[String](default = null)
  val boundaryQuery: ScallopOption[String] = opt[String](default = null)
  val outputTable: ScallopOption[String] = opt[String](required = true)
  val splitBy: ScallopOption[String] = opt[String](required = true)
  val chunks: ScallopOption[Int] = opt[Int](default = Some(10))
  val partitionBy: ScallopOption[String] = opt[String](default = Some("created_date"))

  verify()
}
