package io.delta.connectors.spark.JDBC

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class that accepts Seq of DataFrame => DataFrame functions and applies them
 * one by one on input DataFrame
 **/
class DataTransform(f: Seq[DataFrame => DataFrame])(implicit spark: SparkSession) {

  /**
   * Executes functions against DataFrame
   *
   * @param df - input DataFrame against which functions need to be executed
   * @return - modified by Seq of functions DataFrame
   **/
  def runTransform(df: DataFrame): DataFrame = f.foldLeft(df)((v, f) => f(v))

}