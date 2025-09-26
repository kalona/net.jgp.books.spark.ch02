package net.jgp.books.spark.ch02.lab100_csv_to_db;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

/**
 * CSV to a relational database.
 * 
 * @author jgp
 */
public class CsvToRelationalDatabaseApp {

  private static final Logger log = LoggerFactory.getLogger(CsvToRelationalDatabaseApp.class);

    /**
   * main() is your entry point to the application.
   *
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    CsvToRelationalDatabaseApp app = new CsvToRelationalDatabaseApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
      var spark = SparkSession.builder()
        .appName("CSV to DB")
        .master("local")
        .getOrCreate();

    // Step 1: Ingestion
    // ---------

    // Reads a CSV file with header, called authors.csv, stores it in a
    // dataframe

    Dataset<Row> df = spark.read()
        .format("csv")
        .option("header", "true")
        .load("data/authors.csv");

    // Step 2: Transform
    // ---------

    // Creates a new column called "name" as the concatenation of lname, a
    // virtual column containing ", " and the fname column

    df = df.withColumn(
        "name",
        concat(df.col("lname"), lit(", "), df.col("fname")));

    // Step 3: Save
    // ---------

    // The connection URL, assuming your PostgreSQL instance runs locally on
    // the
    // default port, and the database we use is "spark_labs"

    // Properties to connect to the database, the JDBC driver is part of our
    // pom.xml

    //    Properties file example:
    //    url=jdbc:postgresql://
    //    user=postgres
    //    password=postgres
    //    driver=org.postgresql.Driver

    Properties prop = new Properties();
    try (FileInputStream input = new FileInputStream("src/main/resources/db.properties")) {
      prop.load(input);


    } catch (IOException e) {
      log.error("Error loading properties file", e);
    }
//      // Print all properties loaded from the file
//      for (String key : prop.stringPropertyNames()) {
//          System.out.println(key + "=" + prop.getProperty(key));
//      }

    // Write in a table called ch02
    df.write()
        .mode(SaveMode.Overwrite)
        .jdbc(prop.getProperty("url"), "spark.ch02", prop);

    System.out.println("Process complete");
    spark.stop();
  }
}
