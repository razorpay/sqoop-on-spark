package io.delta.connectors.spark.JDBC

import java.util.Properties
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, _}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

package object spark {

  val log: Logger = LogManager.getLogger(getClass)

  /**
   * Transforms all columns of specified datatype in input dataframe according to
   * function passed, intended usage is inside of DataFrame.transform
   *
   *  @param transformFunc - transformation function to be applied to DataFrame
   **/
  def transformColumn[T <: DataType](transformFunc: Column => Column = identity)
  (implicit tag: ClassTag[T]): DataFrame => DataFrame =
    originalDF => {
      val columns: Array[String] = originalDF.schema.fields
        .collect {
          case StructField(name, _: T, _, _) => name
        }
    columns.foldLeft(originalDF)((df, colName) =>
      df.withColumn(colName, transformFunc(col(colName))))
  }

  def castToType[T <: DataType](column: Column, colType: T): Column =
    when(column.isNotNull, column.cast(colType))

  /**
   * Converts Scala's Map to Java's Properties
   *
   *  @param m - input Map to be converted to Java's Properties
   **/
  implicit def mapToProperties(m: Map[String, String]): Properties = {
    val properties = new Properties()
    properties.putAll(m.asJava)
    properties
  }

}
