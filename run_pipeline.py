"""End-to-end local pipeline: MapReduce (streaming sim) -> Spark -> cross-validation -> benchmark -> dashboard."""
import subprocess, time, json, os, sys
from pathlib import Path
R, OUT = "data/raw", "results"
JOBS = ["movie", "genre", "user", "genre_rating", "user_genre", "month"]
ENV = {**os.environ, "LC_ALL": "C", "MOVIES_FILE": f"{R}/movies.dat"}

def ensure_data():
    """Download the current MovieTweetings snapshot when ratings are absent."""
    ratings = Path(R) / "ratings.dat"
    movies = Path(R) / "movies.dat"
    if ratings.exists() and movies.exists():
        return
    print("Input data is missing; downloading MovieTweetings...")
    subprocess.run([sys.executable, "scraper/fetch_data.py"], check=True)

def sh(cmd):
    t = time.perf_counter(); subprocess.run(cmd, shell=True, check=True, env=ENV, executable="/bin/bash")
    return round(time.perf_counter() - t, 2)

def load(path):
    d = {}
    for l in open(path): k, c, s = l.rstrip("\n").split("\t"); d[k] = (int(c), int(s))
    return d

os.makedirs(f"{OUT}/mapreduce", exist_ok=True)
ensure_data()
PYTHON = sys.executable
t_mr = sum(sh(f"{PYTHON} mapreduce/mr_jobs.py map {j} < {R}/ratings.dat | sort -t $'\\t' -k1,1 | "
              f"{PYTHON} mapreduce/mr_jobs.py reduce > {OUT}/mapreduce/{j}.tsv") for j in JOBS)
t_sp = sh(f"{PYTHON} spark/spark_analysis.py {R} {OUT}/spark 2>/dev/null")
mr = {j: load(f"{OUT}/mapreduce/{j}.tsv") for j in JOBS}
sp = {j: load(f"{OUT}/spark/{j}.tsv") for j in JOBS}
match = {j: mr[j] == sp[j] for j in JOBS}
print("MR time", t_mr, "Spark time", t_sp, "outputs identical:", match)

titles = {}
for l in open(f"{R}/movies.dat", encoding="utf-8", errors="ignore"):
    p = l.rstrip("\n").split("::")
    if len(p) >= 2: titles[p[0]] = (p[1], p[2].split("|") if len(p) > 2 and p[2] else [])
N = sum(c for c, _ in mr["user"].values()); C = sum(s for _, s in mr["user"].values()) / N
avg = lambda c, s: round(s / c, 2)
M = 100  # prior weight for IMDb-style Bayesian average
mv = [(k, c, s) for k, (c, s) in mr["movie"].items()]
def row(k, c, s): return {"id": k, "title": titles.get(k, (k,))[0], "genres": titles.get(k, ("", []))[1],
                          "n": c, "avg": avg(c, s), "wr": round((c / (c + M)) * s / c + (M / (c + M)) * C, 3)}
movies = [row(*x) for x in sorted(mv, key=lambda x: -x[1])[:300]]
top_rated = [row(*x) for x in mv if x[1] >= 200]; top_rated.sort(key=lambda r: -r["wr"])
genres = [{"genre": k, "n": c, "avg": avg(c, s)} for k, (c, s) in mr["genre"].items()]
hist = {}
for k, (c, s) in mr["genre_rating"].items():
    g, r = k.rsplit("|", 1); hist.setdefault(g, [0] * 11)[int(r)] = c
fav, per_user = {}, {}
for k, (c, s) in mr["user_genre"].items():
    u, g = k.split("|", 1); per_user.setdefault(u, {})[g] = c
for u, d in per_user.items():
    if mr["user"][u][0] >= 20: g = max(d, key=d.get); fav[g] = fav.get(g, 0) + 1
users = []
for u, (c, s) in sorted(mr["user"].items(), key=lambda x: -x[1][0])[:25]:
    users.append({"id": u, "n": c, "avg": avg(c, s), "fav": max(per_user[u], key=per_user[u].get)})
months = [{"m": k, "n": c, "avg": avg(c, s)} for k, (c, s) in sorted(mr["month"].items()) if c >= 200]
data = {"kpi": {"ratings": N, "users": len(mr["user"]), "movies": len(mr["movie"]), "catalogue": len(titles),
                "avg": round(C, 2), "heavy": sum(fav.values())},
        "movies": movies, "top_rated": top_rated[:25], "genres": sorted(genres, key=lambda g: -g["avg"]),
        "hist": hist, "fav": fav, "users": users, "months": months,
        "bench": {"mr": t_mr, "spark": t_sp, "match": all(match.values()), "jobs": len(JOBS)}}
json.dump(data, open(f"{OUT}/dashboard_data.json", "w"))
tpl = open("dashboard/template.html", encoding="utf-8").read()
open("dashboard/index.html", "w", encoding="utf-8").write(tpl.replace("/*DATA*/null", json.dumps(data)))
print("dashboard built", len(json.dumps(data)) // 1024, "KB")
