"""Download Goodbooks-10k (ratings.csv, books.csv) into data/goodbooks/."""
import urllib.request

from common.settings import DATA_DIR

BASE = "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/"
FILES = ["ratings.csv", "books.csv"]


def main():
    out = DATA_DIR / "goodbooks"
    out.mkdir(parents=True, exist_ok=True)
    for name in FILES:
        target = out / name
        if target.exists():
            print(f"skip {target} (already there)")
            continue
        print(f"downloading {BASE + name} ...")
        urllib.request.urlretrieve(BASE + name, target)
        print(f"  -> {target} ({target.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
