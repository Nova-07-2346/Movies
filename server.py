import os
import json
import time
from copy import deepcopy
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlsplit
import re
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Thread, local
from flask import Flask, jsonify, render_template, request 
from flask_sock import Sock
from simple_websocket.errors import ConnectionClosed
from pymongo import MongoClient 
from pymongo.errors import PyMongoError
import certifi
from flask_cors import CORS


MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://novans2424_db_user:NfOtLzAA8FRAc42e@cluster0.cfexnxm.mongodb.net/")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "iptv")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "app_state")
MONGODB_DOCUMENT_ID = os.getenv("MONGODB_DOCUMENT_ID", "maindata")
MAX_WORKERS = 8
LINK_CHECK_INTERVAL_SECONDS = 10
REMOTE_CONTROL_ALLOWED_ORIGINS = {
    origin.strip().rstrip("/")
    for origin in os.getenv("REMOTE_CONTROL_ALLOWED_ORIGINS", "").split(",")
    if origin.strip()
}
REMOTE_CONTROL_ALLOWED_ORIGINS.add("null")

Main_data = {
  "default_mov": [
    "https://moviesda.ski/dorothy-2026-tamil-movie/",
    "https://moviesda.ski/meesaya-murukku-2-2026-tamil-movie/",
    "https://moviesda.ski/enna-vilai-2026-tamil-movie/",
    "https://moviesda.ski/romanchakam-2026-tamil-movie/",
    "https://moviesda.ski/jana-nayagan-2026-tamil-movie/"
  ],
  "movies": [ ]
}
main_data_lock = Lock()
mongo_client_lock = Lock()
mongo_client = None
app = Flask(__name__)
CORS(app)
sock = Sock(app)
catalog_clients = set()
catalog_clients_lock = Lock()
catalog_broadcast_lock = Lock()
full_catalog_refresh_lock = Lock()


@app.after_request
def add_remote_control_cors_headers(response):
    origin = request.headers.get("Origin")
    if origin and (
        origin == "null" or origin.rstrip("/") in REMOTE_CONTROL_ALLOWED_ORIGINS
    ):
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        response.headers["Access-Control-Max-Age"] = "600"
        response.headers.add("Vary", "Origin")
    return response


def _catalog_snapshot():
    with main_data_lock:
        return deepcopy(Main_data)


def _default_movies_snapshot():
    with main_data_lock:
        return deepcopy(Main_data.get("default_mov", []))


def _persist_default_movies(urls):
    update_cloud_data({"default_mov": list(urls)})


def _movie_source_url(url):
    if not isinstance(url, str):
        return False
    try:
        parsed = urlsplit(url.strip())
    except ValueError:
        return False
    hostname = (parsed.hostname or "").lower().rstrip(".")
    return (
        parsed.scheme in {"http", "https"}
        and hostname != ""
        and (hostname == "moviesda.ski" or hostname.endswith(".moviesda.ski"))
        and parsed.username is None
        and parsed.password is None
    )


def _request_movie_url():
    payload = request.get_json(silent=True)
    url = request.args.get("url")
    if not url and isinstance(payload, dict):
        url = payload.get("url")
    if not _movie_source_url(url):
        return None
    return url.strip()


def _refresh_movie_url(url):
    refreshed_movie = update_movie_video_links(url)
    if not _movie_has_video_links(refreshed_movie):
        app.logger.warning("Could not refresh movie links from %s", url)
        return False

    return _apply_refreshed_movie(url, refreshed_movie)


def _apply_refreshed_movie(url, refreshed_movie, broadcast=True):
    refreshed_movie = dict(refreshed_movie)
    refreshed_movie["source_url"] = url
    with main_data_lock:
        try:
            movie_index = Main_data["default_mov"].index(url)
        except ValueError:
            return False

        movies = Main_data.setdefault("movies", [])
        while len(movies) < len(Main_data["default_mov"]):
            movies.append({})
        movies[movie_index] = refreshed_movie
    if broadcast:
        _broadcast_catalog()
    return True


def _movie_has_video_links(movie):
    return movie.get("name") != "Error" and any(
        key not in {"name", "poster", "source_url"} and value
        for key, value in movie.items()
    )


def _refresh_all_movie_urls():
    urls = _default_movies_snapshot()
    if not urls:
        return

    refreshed_count = 0
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(urls))) as executor:
        futures = {
            executor.submit(update_movie_video_links, url): url
            for url in urls
        }
        for future in as_completed(futures):
            url = futures[future]
            try:
                movie = future.result()
            except Exception:
                app.logger.exception("Could not refresh movie links from %s", url)
                continue

            if not _movie_has_video_links(movie):
                app.logger.warning("Could not refresh movie links from %s", url)
                continue
            if _apply_refreshed_movie(url, movie):
                refreshed_count += 1

    app.logger.info(
        "Refreshed video links for %d of %d default movie URLs.",
        refreshed_count,
        len(urls),
    )


def _start_full_catalog_refresh():
    if not full_catalog_refresh_lock.acquire(blocking=False):
        return False

    def refresh_worker():
        try:
            _refresh_all_movie_urls()
        finally:
            full_catalog_refresh_lock.release()

    try:
        Thread(target=refresh_worker, daemon=True).start()
    except RuntimeError:
        full_catalog_refresh_lock.release()
        raise
    return True


def _stream_link_has_error(link):
    try:
        with requests.get(
            link,
            headers={"User-Agent": "Mozilla/5.0"},
            allow_redirects=True,
            stream=True,
            timeout=(5, 10),
        ) as response:
            if response.status_code >= 400:
                return True

            body = bytearray()
            for chunk in response.iter_content(chunk_size=2048):
                if not chunk:
                    continue
                body.extend(chunk[:8192 - len(body)])
                content = body.decode("utf-8", errors="ignore")
                if re.search(
                    r"error\s*:\s*404|404\s+not\s+found",
                    content,
                    re.IGNORECASE,
                ):
                    return True
                if len(body) >= 8192:
                    break
            return False
    except requests.RequestException as error:
        app.logger.warning("Unable to check video link %s: %s", link, error)
        return True


def _find_failed_stream_link(movies):
    links = [
        link
        for movie in movies
        for key, link in movie.items()
        if key not in {"name", "poster", "source_url"}
        and isinstance(link, str)
        and link.startswith(("http://", "https://"))
    ]
    if not links:
        return None

    executor = ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(links)))
    futures = {executor.submit(_stream_link_has_error, link): link for link in links}
    found_failure = False
    try:
        for future in as_completed(futures):
            if future.result():
                failed_link = futures[future]
                found_failure = True
                for pending in futures:
                    if pending is not future:
                        pending.cancel()
                return failed_link
    finally:
        executor.shutdown(wait=not found_failure, cancel_futures=found_failure)
    return None


def _broadcast_catalog():
    payload = json.dumps({
        "type": "catalog",
        "data": _catalog_snapshot(),
    })
    with catalog_broadcast_lock:
        with catalog_clients_lock:
            clients = tuple(catalog_clients)

        disconnected = []
        for client in clients:
            try:
                client.send(payload)
            except (ConnectionClosed, OSError):
                disconnected.append(client)

        if disconnected:
            with catalog_clients_lock:
                for client in disconnected:
                    catalog_clients.discard(client)


@app.get("/api/movies")
def get_main_data():
    with main_data_lock:
        data = deepcopy(Main_data)
    return jsonify(data)


@app.post("/api/add_movie_from_page")
def add_movie_from_page():
    url = _request_movie_url()
    if not url:
        return jsonify({"error": "Provide a valid Moviesda movie-page URL."}), 400

    with main_data_lock:
        if url in Main_data["default_mov"]:
            return jsonify({"message": "Movie already exists"}), 200

        Main_data["default_mov"].append(url)
        Main_data.setdefault("movies", []).append({
            "name": "Loading movie links...",
            "poster": "",
            "source_url": url,
        })
        default_urls = list(Main_data["default_mov"])

    _persist_default_movies(default_urls)
    _broadcast_catalog()
    Thread(target=_refresh_movie_url, args=(url,), daemon=True).start()
    return jsonify({
        "message": "Movie added. Its video links are being refreshed.",
        "url": url,
    }), 202


@app.post("/api/edit_movie_from_page")
def edit_movie_from_page():
    payload = request.get_json(silent=True)
    old_url = payload.get("old_url") if isinstance(payload, dict) else None
    new_url = payload.get("new_url") if isinstance(payload, dict) else None
    if not _movie_source_url(old_url) or not _movie_source_url(new_url):
        return jsonify({"error": "Provide valid old_url and new_url movie-page URLs."}), 400

    old_url = old_url.strip()
    new_url = new_url.strip()
    with main_data_lock:
        if old_url not in Main_data["default_mov"]:
            return jsonify({"error": "Movie URL was not found."}), 404
        if new_url != old_url and new_url in Main_data["default_mov"]:
            return jsonify({"error": "The new movie URL already exists."}), 409

        movie_index = Main_data["default_mov"].index(old_url)
        Main_data["default_mov"][movie_index] = new_url
        movies = Main_data.setdefault("movies", [])
        while len(movies) <= movie_index:
            movies.append({})
        movies[movie_index] = {
            "name": "Loading movie links...",
            "poster": "",
            "source_url": new_url,
        }
        default_urls = list(Main_data["default_mov"])

    _persist_default_movies(default_urls)
    _broadcast_catalog()
    Thread(target=_refresh_movie_url, args=(new_url,), daemon=True).start()
    return jsonify({
        "message": "Movie URL updated. Its video links are being refreshed.",
        "url": new_url,
    }), 202


@app.post("/api/remove_movie_from_page")
def remove_movie_from_page():
    url = _request_movie_url()
    if not url:
        return jsonify({"error": "Provide a valid Moviesda movie-page URL."}), 400
    with main_data_lock:
        if url not in Main_data["default_mov"]:
            return jsonify({"message": "Movie not found"}), 404

        movie_index = Main_data["default_mov"].index(url)
        Main_data["default_mov"].pop(movie_index)
        movies = Main_data.setdefault("movies", [])
        if movie_index < len(movies):
            movies.pop(movie_index)
        default_urls = list(Main_data["default_mov"])

    _persist_default_movies(default_urls)
    _broadcast_catalog()
    return jsonify({"message": "Movie removed successfully"}), 200


@app.post("/api/refresh_movie_links")
def refresh_movie_links():
    url = _request_movie_url()
    if not url:
        return jsonify({"error": "Provide a valid Moviesda movie-page URL."}), 400

    if url not in _default_movies_snapshot():
        return jsonify({"error": "Movie URL is not in the default movie list."}), 404

    Thread(target=_refresh_movie_url, args=(url,), daemon=True).start()
    return jsonify({"message": "Movie video links are being refreshed.", "url": url}), 202


# change the default movie list order use index
@app.post("/api/reorder_movies")
def reorder_movies():
    payload = request.get_json(silent=True)
    new_order = payload.get("new_order") if isinstance(payload, dict) else None
    if not isinstance(new_order, list):
        return jsonify({"error": "Invalid 'new_order' parameter"}), 400

    with main_data_lock:
        current_order = list(Main_data["default_mov"])
        if (
            any(not _movie_source_url(url) for url in new_order)
            or len(new_order) != len(current_order)
            or set(new_order) != set(current_order)
        ):
            return jsonify({"error": "New order does not match existing movies"}), 400

        movies_by_url = {
            url: Main_data.get("movies", [])[index]
            for index, url in enumerate(current_order)
            if index < len(Main_data.get("movies", []))
        }
        Main_data["default_mov"] = new_order
        Main_data["movies"] = [
            movies_by_url.get(url, {"name": "Loading movie links...", "source_url": url})
            for url in new_order
        ]
        default_urls = list(new_order)

    _persist_default_movies(default_urls)
    _broadcast_catalog()
    return jsonify({"message": "Movies reordered successfully"}), 200

@sock.route("/ws/catalog")
def catalog_websocket(ws):
    with catalog_clients_lock:
        catalog_clients.add(ws)
    try:
        ws.send(json.dumps({"type": "catalog", "data": _catalog_snapshot()}))
        while True:
            try:
                ws.receive(timeout=30)
            except TimeoutError:
                continue
    except (ConnectionClosed, OSError):
        pass
    finally:
        with catalog_clients_lock:
            catalog_clients.discard(ws)


@app.get("/")
def index():
    return render_template("index.html")

@app.get("/video")
def vid():
    return render_template("video.html")


class PageFetcher:
    def __init__(self):
        self._local = local()
        self._sessions = []
        self._lock = Lock()

    def get_soup(self, url):
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({"User-Agent": "Mozilla/5.0"})
            self._local.session = session
            with self._lock:
                self._sessions.append(session)

        response = session.get(url, timeout=20)
        response.raise_for_status()
        return BeautifulSoup(response.content, "html.parser")

    def close_sessions(self):
        with self._lock:
            sessions, self._sessions = self._sessions, []
        for session in sessions:
            session.close()

    def close(self):
        self.close_sessions()


def parallel_map(items, worker, fetcher):
    iterator = iter(items)
    sentinel = object()

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            pending = deque()

            for _ in range(MAX_WORKERS):
                item = next(iterator, sentinel)
                if item is sentinel:
                    break
                pending.append(executor.submit(worker, item, fetcher))

            while pending:
                yield pending.popleft().result()
                item = next(iterator, sentinel)
                if item is not sentinel:
                    pending.append(executor.submit(worker, item, fetcher))
    finally:
        fetcher.close_sessions()


def fetch_json_data():
    document = _mongo_collection().find_one({"_id": MONGODB_DOCUMENT_ID})
    if document is None:
        return None
    data = document.get("data")
    if not isinstance(data, dict):
        raise ValueError("MongoDB Main_data document must contain an object in 'data'")
    default_movies = data.get("default_mov", [])
    if not isinstance(default_movies, list) or any(
        not isinstance(url, str) or not url.strip() for url in default_movies
    ):
        raise ValueError("MongoDB default_mov must be a list of non-empty URLs")
    return {"default_mov": [url.strip() for url in default_movies]}


def initialize_main_data():
    global Main_data
    stored_data = fetch_json_data()
    if stored_data is None:
        initial_data = {"default_mov": _default_movies_snapshot()}
        update_cloud_data(initial_data)
        with main_data_lock:
            Main_data = {"default_mov": initial_data["default_mov"], "movies": []}
        print("MongoDB collection was empty; initialized it with default movie URLs.")
        return

    with main_data_lock:
        Main_data = {"default_mov": stored_data["default_mov"], "movies": []}
    update_cloud_data(stored_data)
    _broadcast_catalog()
    print("Loaded default movie URLs from MongoDB.")


def update_videos_data(data):
    global Main_data
    default_movies = data.get("default_mov", [])
    main_data = {
        "default_mov": list(default_movies),
        "movies": []
    }
    for movie in default_movies:
        results = update_movie_video_links(movie)
        results["source_url"] = movie
        main_data["movies"].append(results)
    with main_data_lock:
        Main_data = main_data
    _broadcast_catalog()


def update_movie_video_links(current_movie):
    results1 = []
    results2 = []
    results3 = []
    results4 = []
    results6 = []
    poster = ""
    fetcher = PageFetcher()
    print(f"current_movie : {current_movie} ", flush=True)
    try:
        # LEVEL 1: Movie page
        soup = fetcher.get_soup(current_movie)

        for folder in soup.select("div.f a[href]"):
            results1.append({
                "name": folder.get_text(" ", strip=True),
                "url": urljoin(current_movie, folder["href"])
            })
            
        # Get poster
        poster_tag = soup.select_one("picture img")
        if poster_tag:
            poster_src = poster_tag.get("src", "").strip()
            if poster_src:
                poster = urljoin(current_movie, poster_src)

        # Get movie name from <h1>
        h1_tag = soup.select_one("h1")
        raw_movie_name = h1_tag.get_text(" ", strip=True) if h1_tag else "Unknown"
        # Clean up the movie name (e.g., "Dorothy (2026) Tamil Movie" -> "Dorothy")
        clean_name = re.split(r'\s*[\(\-]\s*', raw_movie_name)[0].strip()

        def fetch_subfolders(each_link, page_fetcher):
            try:
                soup = page_fetcher.get_soup(each_link["url"])
                return [{
                        "name": link.get_text(" ", strip=True),
                        "url": urljoin(each_link["url"], link["href"])
                    } for link in soup.select("div.f a[href]")]
            except requests.RequestException as e:
                print(f"Level 2 error: {each_link['url']}: {e}")
                return []

        for links in parallel_map(results1, fetch_subfolders, fetcher):
            results2.extend(links)
        results1.clear()

        def fetch_level3(each_link, page_fetcher):
            page_url = each_link["url"]
            try:
                soup = page_fetcher.get_soup(page_url)
                links = soup.select("div.f a[href]")
                if not links:
                    main = soup.find("main") or soup.find("body")
                    links = main.select("a[href]") if main else []

                page_results = []
                for link in links:
                    href = link.get("href", "").strip()
                    name = link.get_text(" ", strip=True)
                    if not href or not name:
                        continue
                    full_url = urljoin(page_url, href)
                    if (
                        full_url.startswith(("http://", "https://"))
                        and not full_url.endswith("#")
                        and "/tamil-movies/" not in full_url
                    ):
                        page_results.append({"name": name, "url": full_url})
                return page_results
            except requests.RequestException as e:
                print(f"Level 3 error: {page_url}: {e}")
                return []

        # LEVEL 3: Extract links from subfolder pages
        seen_level3 = set()
        for page_results in parallel_map(results2, fetch_level3, fetcher):
            for result in page_results:
                if result["url"] not in seen_level3:
                    seen_level3.add(result["url"])
                    results3.append(result)
        results2.clear()
        
        def fetch_download_pages(item, page_fetcher):
            page_url = item["url"]
            try:
                soup = page_fetcher.get_soup(page_url)
                page_results = []
                for link in soup.select("div.dlink a[href]"):
                    href = link.get("href", "").strip()
                    if not href:
                        continue
                    download_url = urljoin(page_url, href)
                    if download_url.startswith(("http://", "https://")):
                        page_results.append({
                            "name": link.get_text(" ", strip=True) or item["name"],
                            "url": download_url,
                            "source_name": item["name"],
                            "source_url": page_url
                        })
                return page_results
            except requests.RequestException as e:
                print(f"Level 4 error: {page_url}: {e}")
                return []

        # LEVEL 4: Open each Level 3 page and extract div.dlink links
        seen_level4 = set()
        for page_results in parallel_map(results3, fetch_download_pages, fetcher):
            for result in page_results:
                if result["url"] not in seen_level4:
                    seen_level4.add(result["url"])
                    results4.append(result)
        results3.clear()

        def fetch_final_link(item, page_fetcher):
            page_url = "https://movies.downloadpage.xyz/download/page/" + item["url"].split("/")[-1]
            try:
                soup = page_fetcher.get_soup(page_url)
                final_link = soup.select_one("div.dlink a[href]")
                if final_link:
                    href = final_link.get("href", "").strip()
                    if href.startswith(("http://", "https://")):
                        return item, href
            except requests.RequestException as e:
                print(f"Level 6 error: {page_url}: {e}")
            return item, None

        # LEVEL 6: Load download pages and get final download links
        seen_level6 = set()
        for item, href in parallel_map(results4, fetch_final_link, fetcher):
            if href and href not in seen_level6:
                seen_level6.add(href)
                results6.append((item, href))
        results4.clear()

        # Build dynamic output dictionary matching your target format
        movie_data = {
            "name": clean_name,
            "poster": poster,
            "source_url": current_movie
        }

        # Pair items from results4 and results6 to extract any resolution and size dynamically
        for item4, final_url in results6:
            item_name = (item4.get("name", "") + " " + item4.get("source_name", "")).lower()
            
            # Dynamic Regex to find resolutions (360p, 720p, 1080p, 4k, etc.) and file sizes
            res_match = re.search(r'(\d{3,4}p|4k|2160p)', item_name)
            size_match = re.search(r'\(?(\d+(?:\.\d+)?)\s*(mb|gb)\)?', item_name)
            
            res_key = res_match.group(1) if res_match else "file"
            size_key = f"_{size_match.group(1)}{size_match.group(2)}" if size_match else ""
            
            base_key = f"{res_key}{size_key}"
            
            # Handle key collisions safely if multiple links share the same tag
            key = base_key
            counter = 1
            while key in movie_data:
                key = f"{base_key}_{counter}"
                counter += 1
                
            movie_data[key] = final_url

        return movie_data

    except requests.RequestException as e:
        print(f"Movie page error: {current_movie}: {e}")
        return {"name": "Error", "poster": "", "source_url": current_movie}

    finally:
        fetcher.close()


def update_cloud_data(data):
    if not isinstance(data, dict):
        raise ValueError("MongoDB data must be an object")
    default_movies = data.get("default_mov")
    if not isinstance(default_movies, list) or any(
        not isinstance(url, str) or not url.strip() for url in default_movies
    ):
        raise ValueError("MongoDB default_mov must be a list of non-empty URLs")
    _mongo_collection().replace_one(
        {"_id": MONGODB_DOCUMENT_ID},
        {
            "_id": MONGODB_DOCUMENT_ID,
            "data": {"default_mov": [url.strip() for url in default_movies]},
        },
        upsert=True
    )
    print("Default movie URLs updated successfully in MongoDB.")


def _mongo_collection():
    global mongo_client
    if not MONGODB_URI:
        raise RuntimeError("Set the MONGODB_URI environment variable.")

    with mongo_client_lock:
        if mongo_client is None:
            mongo_client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=5000,
                tlsCAFile=certifi.where()
            )
    return mongo_client[MONGODB_DATABASE][MONGODB_COLLECTION]

def run_update_loop():
    try:
        app.logger.info("Updater thread starting")

        initialize_main_data()
        app.logger.info("Main data initialized")

        print("start_updater", flush=True)
        update_videos_data({"default_mov": _default_movies_snapshot()})

        while True:
            print("loop start", flush=True)

            with main_data_lock:
                movies = deepcopy(Main_data.get("movies", []))

            failed_link = _find_failed_stream_link(movies)

            if failed_link:
                app.logger.warning(
                    "Unavailable video link: %s. Refreshing catalog.",
                    failed_link,
                )
                if not _start_full_catalog_refresh():
                    app.logger.info("Catalog refresh already running.")

            try:
                cloud_data = fetch_json_data()
                if (
                    cloud_data
                    and cloud_data["default_mov"] != _default_movies_snapshot()
                ):
                    app.logger.info("Default movie list changed; rebuilding catalog.")
                    update_videos_data(cloud_data)

            except (PyMongoError, ValueError, KeyError) as exc:
                app.logger.exception("MongoDB movie check failed: %s", exc)

            time.sleep(LINK_CHECK_INTERVAL_SECONDS)

            try:
                requests.get(
                    "https://moviepage.onrender.com/",
                    timeout=5,
                )
            except requests.RequestException:
                app.logger.exception("Keep-alive request failed")

    except Exception:
        app.logger.exception("Updater thread crashed during startup or execution")


if __name__ == "__main__":
    Thread(target=run_update_loop, daemon=True, name="movie-updater").start()

    app.run(
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "5000")),
        threaded=True,
        use_reloader=False,
        debug=True,
    )
else:
    Thread(target=run_update_loop, daemon=True, name="movie-updater").start()
