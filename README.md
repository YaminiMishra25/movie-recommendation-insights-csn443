# Movie Recommendation Insights

CSN443 Big Data Analytics — Project 7

An end-to-end movie-rating analytics pipeline using Hadoop Streaming MapReduce, Apache Spark, HiveQL, and an interactive dashboard. It analyses the public MovieTweetings dataset for most-rated movies, highest-rated genres, active users, genre-wise preferences, and monthly trends.

## Open in GitHub Codespaces

1. Create a GitHub repository and upload this project, including the `.devcontainer` folder.
2. Open the repository in a Codespace. The devcontainer installs Python dependencies and Java 17 automatically.
3. In the Codespaces terminal, run:

   ```bash
   python scraper/fetch_data.py
   python run_pipeline.py
   python -m http.server 8000 --directory dashboard
   ```

4. Open the forwarded port 8000 to view the dashboard.

`run_pipeline.py` also downloads the data automatically if `data/raw/ratings.dat` is missing. The large ratings and users files are deliberately not committed to GitHub; they are reproducibly downloaded from MovieTweetings. The small `movies.dat` catalogue is included.

## Repository layout

```text
scraper/fetch_data.py       Download MovieTweetings and optional OMDb metadata
mapreduce/mr_jobs.py        Hadoop Streaming mapper, combiner, and reducer
spark/spark_analysis.py     Equivalent PySpark aggregations
hive/queries.hql            HiveQL version of the core analyses
run_pipeline.py             Local MapReduce simulation, Spark validation, and dashboard build
dashboard/                  Self-contained HTML dashboard
data/raw/movies.dat         Movie catalogue; ratings/users are downloaded on demand
results/                    Included result snapshots and dashboard data
docs/Project_Report.pdf     Full project report
```

## Local run

Requirements: Python 3.10+, Java 17+, and a modern browser.

```bash
python -m pip install -r requirements.txt
python scraper/fetch_data.py
python run_pipeline.py
python -m http.server 8000 --directory dashboard
```

The pipeline runs six jobs (`movie`, `genre`, `user`, `genre_rating`, `user_genre`, and `month`), compares MapReduce-style results with Spark results, prints the benchmark and validation status, and rebuilds `dashboard/index.html`.

## Optional cluster and enrichment runs

```bash
./run_hadoop.sh                 # Hadoop Streaming; requires HADOOP_HOME and HDFS
hive -f hive/queries.hql        # HiveQL; requires Hive and the HDFS input layout
OMDB_KEY=your_key python scraper/fetch_data.py --enrich 200
```

## Design notes

- The generic mapper emits `key, 1, rating`; the reducer sums `(count, rating_sum)`, so it can also act as a combiner.
- Genre analysis uses a map-side join in Hadoop and a broadcast join in Spark.
- The code handles malformed rows, missing genres, movies absent from the catalogue, and small genre samples.
- Rankings use a Bayesian weighted rating with a prior weight of 100 rather than relying on raw averages alone.
- The ratings are user preferences from MovieTweetings, not official box-office totals or IMDb title aggregates.

## Data source

MovieTweetings: <https://github.com/sidooms/MovieTweetings>
