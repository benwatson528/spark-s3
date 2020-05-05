package uk.co.hadoopathome.sparks3;

import org.apache.spark.sql.SparkSession;

public class SparkRunner {

  public static void processSpark(String[] args, SparkSession sparkSession) {
    String inputFile = args[0];
    String outputDir = args[1];
    sparkSession
        .read()
        .option("header", true)
        .option("inferSchema", true)
        .csv(inputFile)
        .drop("age")
        .write()
        .option("header", true)
        .format("csv")
        .save(outputDir);
  }
}
