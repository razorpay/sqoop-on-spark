// scalastyle:off
/*
 * Copyright (2021) Scribd Inc.
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
// scalastyle:on

package io.delta.connectors.spark.JDBC

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class that accepts Seq of DataFrame => DataFrame functions and applies them
 * one by one on input DataFrame
 */
class DataTransforms(f: Seq[DataFrame => DataFrame])(implicit spark: SparkSession) {

  /**
   * Executes functions against DataFrame
   *
   * @param df - input DataFrame against which functions need to be executed
   * @return - modified by Seq of functions DataFrame
   */
  def runTransform(df: DataFrame): DataFrame = f.foldLeft(df)((v, f) => f(v))
}
