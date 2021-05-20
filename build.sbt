/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import ReleaseTransformations._

parallelExecution in ThisBuild := false
scalastyleConfig in ThisBuild := baseDirectory.value / "scalastyle-config.xml"
crossScalaVersions in ThisBuild := Seq("2.12.8", "2.11.12")

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

val sparkVersion = "3.0.0"
val hadoopVersion = "2.7.2"
val hiveVersion = "2.3.7"
val deltaVersion = "0.5.0"

lazy val commonSettings = Seq(
  organization := "io.delta",
  scalaVersion := "2.12.8",
  fork := true,
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalacOptions += "-target:jvm-1.8",
  // Configurations to speed up tests and reduce memory footprint
  javaOptions in Test ++= Seq(
    "-Dspark.ui.enabled=false",
    "-Dspark.ui.showConsoleProgress=false",
    "-Dspark.databricks.delta.snapshotPartitions=2",
    "-Dspark.sql.shuffle.partitions=5",
    "-Ddelta.log.cacheSize=3",
    "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
    "-Xmx1024m"
  ),
  compileScalastyle := scalastyle.in(Compile).toTask("").value,
  (compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value,
  testScalastyle := scalastyle.in(Test).toTask("").value,
  (test in Test) := ((test in Test) dependsOn testScalastyle).value
)

lazy val releaseSettings = Seq(
  publishMavenStyle := true,
  releaseCrossBuild := true,
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
  pomExtra :=
    <url>https://github.com/delta-io/connectors</url>
      <scm>
        <url>git@github.com:delta-io/connectors.git</url>
        <connection>scm:git:git@github.com:delta-io/connectors.git</connection>
      </scm>
      <developers>
        <developer>
          <id>tdas</id>
          <name>Tathagata Das</name>
          <url>https://github.com/tdas</url>
        </developer>
        <developer>
          <id>scottsand-db</id>
          <name>Scott Sandre</name>
          <url>https://github.com/scottsand-db</url>
        </developer>
        <developer>
          <id>windpiger</id>
          <name>Jun Song</name>
          <url>https://github.com/windpiger</url>
        </developer>
        <developer>
          <id>zsxwing</id>
          <name>Shixiong Zhu</name>
          <url>https://github.com/zsxwing</url>
        </developer>
      </developers>,
  bintrayOrganization := Some("delta-io"),
  bintrayRepository := "delta",
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    publishArtifacts,
    setNextVersion,
    commitNextVersion
  )
)

lazy val skipReleaseSettings = Seq(
  publishArtifact := false,
  publish := ()
)

// Don't release the root project
publishArtifact := false

publish := ()

// Looks some of release settings should be set for the root project as well.
releaseCrossBuild := true

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion
)

lazy val sqlDeltaImport = (project in file("sql-delta-import"))
  .settings (
    name := "sql-delta-import",
    commonSettings,
    publishArtifact := scalaBinaryVersion.value == "2.12",
    publishArtifact in Test := false,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.rogach" %% "scallop" % "3.5.1",
      "org.scalatest" %% "scalatest" % "3.1.1" % "test",
      "com.h2database" % "h2" % "1.4.200" % "test",
      "org.apache.spark" % "spark-catalyst_2.12" % sparkVersion % "test",
      "org.apache.spark" % "spark-core_2.12" % sparkVersion % "test",
      "org.apache.spark" % "spark-sql_2.12" % sparkVersion % "test",
      "mysql" % "mysql-connector-java" % "8.0.25",
      "org.postgresql" % "postgresql" % "42.2.20",
      "com.databricks" %% "dbutils-api" % "0.0.5"
    )
  )
  .settings(releaseSettings)
