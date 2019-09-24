val sqlContext = new org.apache.spark.sql.SQLContext(sc);

val ds = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/Users/kunalsharma/Downloads/AB_NYC_2019.csv")
