"""Data acquisition. MovieTweetings = real IMDb ratings that users tweeted (IMDb-sourced, updated continuously).
Optional enrichment: OMDb API (IMDb metadata) for the most-rated titles.  Usage: python scraper/fetch_data.py [--enrich N]"""
import os, sys, json, time, urllib.request
BASE = "https://raw.githubusercontent.com/sidooms/MovieTweetings/master/latest/"
RAW = "data/raw"

def download():
    os.makedirs(RAW, exist_ok=True)
    for f in ("movies", "ratings", "users"):
        urllib.request.urlretrieve(f"{BASE}{f}.dat", f"{RAW}/{f}.dat")
        print("downloaded", f)

def enrich(n, key=os.getenv("OMDB_KEY")):
    """Fetch IMDb rating/poster/runtime via OMDb (free key: omdbapi.com) for top-n most rated movies."""
    from collections import Counter
    top = Counter(l.split("::")[1] for l in open(f"{RAW}/ratings.dat")).most_common(n)
    out = {}
    for mid, _ in top:
        url = f"https://www.omdbapi.com/?i=tt{mid}&apikey={key}"
        try: out[mid] = json.load(urllib.request.urlopen(url, timeout=15))
        except Exception as e: print("skip", mid, e)
        time.sleep(0.2)                       # polite rate limit
    json.dump(out, open(f"{RAW}/omdb.json", "w"))

if __name__ == "__main__":
    download()
    if "--enrich" in sys.argv: enrich(int(sys.argv[sys.argv.index("--enrich") + 1]))
