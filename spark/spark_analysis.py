"""PySpark implementation of the same six aggregations (DataFrame API). Usage: spark_analysis.py <raw_dir> <out_dir>"""
import sys, os
from pyspark.sql import SparkSession, functions as F
src, out = sys.argv[1], sys.argv[2]; os.makedirs(out, exist_ok=True)
sp = (SparkSession.builder.master("local[*]").appName("MovieInsights")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.session.timeZone", "UTC").getOrCreate())
sp.sparkContext.setLogLevel("ERROR")
p = F.split("value", "::")
r = (sp.read.text(f"{src}/ratings.dat").select(p.alias("p"))
       .select(F.col("p")[0].alias("user"), F.col("p")[1].alias("movie"),
               F.col("p")[2].cast("int").alias("rating"), F.col("p")[3].cast("long").alias("ts"))
       .dropna())                                               # drop malformed rows
m = (sp.read.text(f"{src}/movies.dat").select(p.alias("p"))
       .select(F.col("p")[0].alias("movie"),
               F.when((F.size("p") > 2) & (F.length(F.col("p")[2]) > 0), F.col("p")[2]).otherwise("Unknown").alias("g"))
       .select("movie", F.explode(F.split("g", "\\|")).alias("genre")))
j = r.join(F.broadcast(m), "movie")                             # broadcast = map-side join
specs = {"movie": r.groupBy(F.col("movie").alias("k")), "user": r.groupBy(F.col("user").alias("k")),
         "month": r.groupBy(F.date_format(F.from_unixtime("ts"), "yyyy-MM").alias("k")),
         "genre": j.groupBy(F.col("genre").alias("k")),
         "genre_rating": j.groupBy(F.concat_ws("|", "genre", "rating").alias("k")),
         "user_genre": j.groupBy(F.concat_ws("|", "user", "genre").alias("k"))}
for name, g in specs.items():
    with open(f"{out}/{name}.tsv", "w") as f:
        for k, c, s in g.agg(F.count("*"), F.sum("rating")).collect(): f.write(f"{k}\t{c}\t{s}\n")
sp.stop()
