#!/usr/bin/env python3
"""Hadoop Streaming jobs. One mapper emits  key<TAB>1<TAB>rating ; one reducer sums (count, rating_sum) per key.
Reducer input and output share one format, so it doubles as a COMBINER (less shuffle traffic).
Jobs: movie | genre | user | genre_rating | user_genre | month.
Genre jobs use a map-side join: movies.dat ships to every node (-files), cheaper than a reduce-side join."""
import sys, os, datetime

def load_genres(path=os.environ.get("MOVIES_FILE", "movies.dat")):
    d = {}
    for line in open(path, encoding="utf-8", errors="ignore"):
        p = line.rstrip("\n").split("::")
        if len(p) >= 2:
            d[p[0]] = p[2].split("|") if len(p) > 2 and p[2] else ["Unknown"]   # edge case: missing genre
    return d

def mapper(job):
    genres = load_genres() if "genre" in job else None
    for line in sys.stdin:
        p = line.rstrip("\n").split("::")
        try: user, movie, rating, ts = p[0], p[1], int(p[2]), int(p[3])
        except (ValueError, IndexError): continue                                # edge case: malformed row
        if job == "movie":   keys = [movie]
        elif job == "user":  keys = [user]
        elif job == "month": keys = [datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m")]
        else:
            g = genres.get(movie)
            if not g: continue                                                   # movie missing from catalogue
            keys = {"genre": g, "genre_rating": [f"{x}|{rating}" for x in g],
                    "user_genre": [f"{user}|{x}" for x in g]}[job]
        for k in keys: print(f"{k}\t1\t{rating}")

def reducer():
    cur, c, s = None, 0, 0
    for line in sys.stdin:                      # input arrives sorted by key
        k, n, r = line.rstrip("\n").split("\t")
        if k != cur:
            if cur is not None: print(f"{cur}\t{c}\t{s}")
            cur, c, s = k, 0, 0
        c += int(n); s += int(r)
    if cur is not None: print(f"{cur}\t{c}\t{s}")

if __name__ == "__main__":
    mapper(sys.argv[2]) if sys.argv[1] == "map" else reducer()
