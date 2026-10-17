from pyspark.sql  import SparkSession
from delta import configure_spark_with_delta_pip


builder = (
    SparkSession.builder.appName("Smoke Test")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.range(5).write.format("delta").mode("overwrite").save("/tmp/smoke_delta")
spark.read.format("delta").load("/tmp/smoke_delta").show()
spark.sql("DESCRIBE HISTORY delta.`/tmp/smoke_delta`").select("version","operation").show()