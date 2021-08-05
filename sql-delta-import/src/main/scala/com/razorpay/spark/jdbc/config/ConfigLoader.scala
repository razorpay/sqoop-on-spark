package com.razorpay.spark.jdbc.config

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File
import scala.io.Source
import scala.sys.process._

/**
 * Custom config loader class which checks which environment the app is running in
 * and loads the config file accordingly.
 */
object ConfigLoader {

  var config: Config = _

  def load(): Config = {
    if (config == null) {
      val envFilePath = "/opt/env"

      if (new File(envFilePath).exists()) {
        val env: String = Source.fromFile(envFilePath).getLines().mkString.toLowerCase

        val configFilePath = "/tmp/application.conf.vault"
        val configS3Path = "s3://rzp-config/sqoop-config/application.conf.vault"

        val configS3PathResolved = {
          if (env == "stage") {
            configS3Path.replace("rzp-config", "rzp-stage-config")
          } else {
            configS3Path
          }
        }

        val table_name: String = f"unicreds_${env}_config"

        val ssecKey: String =
          s"/usr/local/bin/unicreds -t $table_name -k alias/qbunicreds -r ap-south-1 get s3_c_key" !!

        s"aws s3 cp --region ap-south-1 --sse-c --sse-c-key $ssecKey $configS3PathResolved $configFilePath" !!

        config = ConfigFactory.parseFile(new File(configFilePath))
      } else {
        config = ConfigFactory.load()
      }
    }

    config
  }
}
