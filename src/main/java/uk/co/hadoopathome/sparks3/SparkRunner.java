package uk.co.hadoopathome.sparks3;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkRunner.class);

  public static void processSpark(String[] args, SparkSession sparkSession) {
    String inputFile = args[0];
    String outputDir = args[1];
    LOGGER.info("Processing input file {}", inputFile);
    sparkSession
        .read()
        .option("header", true)
        .option("inferSchema", true)
        .csv(inputFile)
        .drop("age")
        .write()
        .format("csv")
        .save(outputDir);
    LOGGER.info("Data written to {}", outputDir);
  }
}
