package chapter5;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Learning Spark SQL Dataframe API")
                .master("local")
                .getOrCreate();


        String studentsFile = "src/main/resources/chapter5/students.csv";

        Dataset<Row> studentDf = spark.read().format("csv")
                .option("inferSchema", "true") // Make sure to use string version of true
                .option("header", true)
                .load(studentsFile);

        String gradeChartFile = "src/main/resources/chapter5/grade_chart.csv";

        Dataset<Row> gradesDf = spark.read().format("csv")
                .option("inferSchema", "true") // Make sure to use string version of true
                .option("header", true)
                .load(gradeChartFile);

        Dataset<Row> filterdDf = studentDf.join(gradesDf, studentDf.col("GPA").equalTo(gradesDf.col("GPA")))
                .filter(gradesDf.col("gpa").gt(3.0).and(gradesDf.col("gpa").lt(4.5))
                        .or(gradesDf.col("gpa").equalTo(1.0)))
                .select("student_name",
                        "favorite_book_title",
                        "letter_grade");

        filterdDf.show(10);

    }
}
