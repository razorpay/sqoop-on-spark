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

package com.razorpay.spark.jdbc

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Connection, DriverManager}

class ImportTest extends AnyFunSuite with BeforeAndAfterAll {

  private def initDataSource(conn: Connection) = {
    conn.prepareStatement("create schema test").executeUpdate()
    conn
      .prepareStatement(
        """
    create table test.tbl(
      id TINYINT,
      status SMALLINT,
      ts TIMESTAMP,
      title VARCHAR)"""
      )
      .executeUpdate()
    conn
      .prepareStatement(
        """
    insert into test.tbl(id, status, ts, title ) VALUES
    (1, 2, parsedatetime('01-02-2021 01:02:21', 'dd-MM-yyyy hh:mm:ss'),'lorem ipsum'),
    (3, 4, parsedatetime('03-04-2021 03:04:21', 'dd-MM-yyyy hh:mm:ss'),'lorem'),
    (5, 6, parsedatetime('05-06-2021 05:06:21', 'dd-MM-yyyy hh:mm:ss'),'ipsum'),
    (7, 8, parsedatetime('07-08-2021 07:08:21', 'dd-MM-yyyy hh:mm:ss'),'Lorem Ipsum')
    """
      )
      .executeUpdate()
  }

  implicit lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("spark session")
    .config("spark.sql.shuffle.partitions", "10")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  val url = "jdbc:h2:mem:testdb;DATABASE_TO_UPPER=FALSE"

  DriverManager.registerDriver(new org.h2.Driver())

  val conn = DriverManager.getConnection(url)
  initDataSource(conn)

  override def afterAll() {
    spark.catalog.clearCache()
    spark.sharedState.cacheManager.clearCache()
    conn.close()
  }

  val chunks = 2

  test("import data into a table") {
    spark.sql("DROP TABLE IF EXISTS tbl")
    spark.sql("""
      CREATE TABLE tbl (id INT, status INT, title STRING)
      LOCATION "spark-warehouse/tbl"
    """)

    JDBCImport(
      url,
      ImportConfig(
        inputTable = "tbl",
        query = None,
        boundaryQuery = None,
        outputTable = "output.tbl",
        splitBy = Some("id"),
        chunks = chunks,
        partitionBy = None,
        database = "test",
        mapColumns = None
      )
    ).run()

    // since we imported data without any optimizations number of
    // read partitions should equal number of chunks used during import
    assert(spark.table("tbl").rdd.getNumPartitions == chunks)

    val imported = spark
      .sql("select * from tbl")
      .collect()
      .sortBy(a => a.getAs[Int]("id"))

    assert(imported.length == 4)
    assert(imported.map(a => a.getAs[Int]("id")).toSeq == Seq(1, 3, 5, 7))
    assert(imported.map(a => a.getAs[Int]("status")).toSeq == Seq(2, 4, 6, 8))
    assert(
      imported.map(a => a.getAs[String]("title")).toSeq ==
        Seq("lorem ipsum", "lorem", "ipsum", "Lorem Ipsum")
    )
  }
}
