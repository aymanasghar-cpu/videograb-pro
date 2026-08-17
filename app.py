import os
import json
import uuid
import threading
import subprocess
import sys
import time
import re
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, request, jsonify, render_template, send_from_directory, Response
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

if getattr(sys, 'frozen', False):
    # When running as a PyInstaller executable
    BASE_DIR = Path(sys.executable).parent
    BUNDLE_DIR = Path(sys._MEIPASS)
else:
    # When running normally
    BASE_DIR = Path(__file__).parent
    BUNDLE_DIR = BASE_DIR

# Reconfigure Flask to find templates in the bundled directory
app.template_folder = str(BUNDLE_DIR / "templates")

DOWNLOADS_DIR = BASE_DIR / "downloads"
COOKIES_DIR = BASE_DIR / "cookies"
COOKIES_FILE = COOKIES_DIR / "cookies.txt"

DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
COOKIES_DIR.mkdir(parents=True, exist_ok=True)

# Current save directory (can be changed by user)
SAVE_DIR = DOWNLOADS_DIR

# In-memory task tracking
tasks = {}
# Maps task_id -> {idx: subprocess} for parallel stop support
active_procs = {}   # task_id -> dict of {idx: proc}


def get_yt_dlp_cmd():
    """Return yt-dlp command."""
    return [sys.executable, "-m", "yt_dlp"]


def detect_platform(url):
    """Detect platform from URL for special handling."""
    url_lower = url.lower()
    if "tiktok.com" in url_lower:
        return "tiktok"
    if "instagram.com" in url_lower:
        return "instagram"
    if "twitter.com" in url_lower or "x.com" in url_lower:
        return "twitter"
    if "facebook.com" in url_lower or "fb.watch" in url_lower:
        return "facebook"
    return "generic"


def download_tiktok_direct(task_id, idx, url, fmt, output_path):
    """Direct stream download for TikTok videos (bypasses anti-bot challenges)."""
    try:
        api_res = requests.post("https://www.tikwm.com/api/", data={"url": url, "hd": 1}, timeout=10).json()
        if api_res.get("code") != 0 or not api_res.get("data"):
            return False, "TikWM API returned error"

        data = api_res["data"]
        raw_title = data.get("title") or f"tiktok_{data.get('id', 'video')}"
        clean_title = re.sub(r'[\\/*?:"<>|]', "", raw_title).strip()[:90]
        if not clean_title:
            clean_title = f"tiktok_{data.get('id', int(time.time()))}"

        ext = "mp3" if fmt == "mp3" else "mp4"
        filename = f"{clean_title}.{ext}"
        dest_file = output_path / filename

        if fmt == "mp3":
            stream_url = data.get("music")
        else:
            stream_url = data.get("hdplay") or data.get("play")

        if not stream_url:
            return False, "No stream URL found in TikTok response"

        if stream_url.startswith("/"):
            stream_url = "https://www.tikwm.com" + stream_url

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Referer": "https://www.tiktok.com/"
        }

        resp = requests.get(stream_url, headers=headers, stream=True, timeout=30)
        if resp.status_code != 200:
            return False, f"Failed to download video stream (HTTP {resp.status_code})"

        total_bytes = int(resp.headers.get("content-length", 0))
        if total_bytes > 0:
            tasks[task_id]["results"][idx]["total_size"] = f"{total_bytes / (1024 * 1024):.2f}MB"

        downloaded_bytes = 0
        start_time = time.time()
        last_calc_time = start_time
        last_calc_bytes = 0

        with open(dest_file, "wb") as f:
            for chunk in resp.iter_content(chunk_size=256 * 1024):  # 256KB chunks for speed
                if tasks[task_id].get("status") == "stopped":
                    f.close()
                    try:
                        dest_file.unlink(missing_ok=True)
                    except Exception:
                        pass
                    tasks[task_id]["results"][idx]["status"] = "stopped"
                    tasks[task_id]["results"][idx]["error"] = "Stopped by user"
                    return True, "Stopped"

                if chunk:
                    f.write(chunk)
                    downloaded_bytes += len(chunk)

                    now = time.time()
                    if now - last_calc_time >= 0.35:
                        diff_time = now - last_calc_time
                        diff_bytes = downloaded_bytes - last_calc_bytes
                        speed_val = (diff_bytes / diff_time) / (1024 * 1024)
                        tasks[task_id]["results"][idx]["speed"] = f"{speed_val:.2f}MiB/s"

                        if total_bytes > 0:
                            pct = round((downloaded_bytes / total_bytes) * 100, 1)
                            tasks[task_id]["results"][idx]["progress"] = pct
                            rem_bytes = total_bytes - downloaded_bytes
                            rem_secs = rem_bytes / (diff_bytes / diff_time) if diff_bytes > 0 else 0
                            mins = int(rem_secs // 60)
                            secs = int(rem_secs % 60)
                            tasks[task_id]["results"][idx]["eta"] = f"{mins:02d}:{secs:02d}"

                        last_calc_time = now
                        last_calc_bytes = downloaded_bytes

        tasks[task_id]["results"][idx]["progress"] = 100
        tasks[task_id]["results"][idx]["status"] = "done"
        tasks[task_id]["results"][idx]["filename"] = filename
        return True, "Done"

    except Exception as e:
        return False, str(e)


def _extract_youtube_id(url):
    """Extract YouTube video ID from various URL formats."""
    import urllib.parse
    parsed = urllib.parse.urlparse(url)
    if "youtu.be" in parsed.hostname:
        return parsed.path.lstrip("/").split("/")[0]
    if "youtube.com" in parsed.hostname:
        qs = urllib.parse.parse_qs(parsed.query)
        return qs.get("v", [None])[0]
    return None


def _stream_download_file(task_id, idx, stream_url, dest_file, filename, expected_total_bytes=0):
    """Helper to download a file from a stream URL with live progress, MBs, speed, and ETA tracking."""
    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        resp = requests.get(stream_url, headers=hdrs, stream=True, timeout=180)
        if resp.status_code != 200:
            return False

        header_len = int(resp.headers.get("content-length", 0))
        total_bytes = header_len if header_len > 0 else expected_total_bytes

        if total_bytes > 0:
            tasks[task_id]["results"][idx]["total_size"] = f"{total_bytes / (1024 * 1024):.1f} MB"

        downloaded_bytes = 0
        tasks[task_id]["results"][idx]["filename"] = filename
        tasks[task_id]["results"][idx]["status"] = "downloading"
        start_time = time.time()
        last_calc_time = start_time
        last_calc_bytes = 0
        last_pct = 0.0  # Never allow progress bar to go backwards

        # Adaptive estimation for chunked streams without content-length
        # Start generous to avoid constant resets
        estimated_total = total_bytes if total_bytes > 0 else max(20 * 1024 * 1024, 1)

        with open(dest_file, "wb") as f:
            for chunk in resp.iter_content(chunk_size=512 * 1024):  # 512KB chunks for smooth & rapid updates
                if tasks[task_id].get("status") == "stopped":
                    f.close()
                    dest_file.unlink(missing_ok=True)
                    return True

                if chunk:
                    f.write(chunk)
                    downloaded_bytes += len(chunk)

                    now = time.time()
                    if now - last_calc_time >= 0.25:  # 4 UI updates/sec
                        diff_time = now - last_calc_time
                        diff_bytes = downloaded_bytes - last_calc_bytes
                        speed_val = (diff_bytes / diff_time) / (1024 * 1024) if diff_time > 0 else 0
                        tasks[task_id]["results"][idx]["speed"] = f"{speed_val:.2f} MB/s"

                        dl_mb = downloaded_bytes / (1024 * 1024)
                        tasks[task_id]["results"][idx]["downloaded_size"] = f"{dl_mb:.1f} MB"

                        if total_bytes > 0:
                            # Exact content-length available
                            pct = min(round((downloaded_bytes / total_bytes) * 100, 1), 99.9)
                            pct = max(pct, last_pct)  # Never go backwards
                            tasks[task_id]["results"][idx]["progress"] = pct
                            last_pct = pct
                            if speed_val > 0:
                                rem_bytes = total_bytes - downloaded_bytes
                                rem_secs = rem_bytes / (speed_val * 1024 * 1024)
                                mins = int(rem_secs // 60)
                                secs = int(rem_secs % 60)
                                tasks[task_id]["results"][idx]["eta"] = f"{mins:02d}:{secs:02d}"
                        else:
                            # Chunked stream: grow estimated_total only when near the limit
                            if downloaded_bytes > estimated_total * 0.88:
                                estimated_total = int(downloaded_bytes * 1.30)
                            # Smooth monotonic progress capped at 95%
                            raw_pct = round((downloaded_bytes / estimated_total) * 95, 1)
                            pct = max(raw_pct, last_pct)  # Never go backwards
                            pct = min(pct, 95.0)
                            tasks[task_id]["results"][idx]["progress"] = pct
                            last_pct = pct
                            tasks[task_id]["results"][idx]["total_size"] = f"~{estimated_total / (1024 * 1024):.0f} MB"
                            # ETA for chunked streams using overall speed
                            total_elapsed = now - start_time
                            if total_elapsed > 0 and downloaded_bytes > 0:
                                avg_speed_bytes = downloaded_bytes / total_elapsed
                                # Estimate remaining as difference from current estimated total
                                rem_bytes_est = max(estimated_total - downloaded_bytes, 0)
                                if avg_speed_bytes > 0:
                                    rem_secs = rem_bytes_est / avg_speed_bytes
                                    mins = int(rem_secs // 60)
                                    secs = int(rem_secs % 60)
                                    tasks[task_id]["results"][idx]["eta"] = f"{mins:02d}:{secs:02d}"

                        last_calc_time = now
                        last_calc_bytes = downloaded_bytes

        final_mb = downloaded_bytes / (1024 * 1024)
        tasks[task_id]["results"][idx]["downloaded_size"] = f"{final_mb:.1f} MB"
        tasks[task_id]["results"][idx]["total_size"] = f"{final_mb:.1f} MB"
        tasks[task_id]["results"][idx]["status"] = "done"
        tasks[task_id]["results"][idx]["progress"] = 100
        tasks[task_id]["results"][idx]["eta"] = "00:00"
        return True
    except Exception:
        return False



def download_youtube_direct(task_id, idx, url, fmt, output_path):
    """Fast YouTube downloader — tries instant stream APIs first to eliminate wait times.
    Order: 1) Piped (instant)  2) Invidious (instant)  3) Cobalt  4) Loader.to (slow but reliable)
    """
    video_id = _extract_youtube_id(url)
    if not video_id:
        return False, "Could not extract YouTube video ID"

    want_audio_only = (fmt == "mp3")
    tasks[task_id]["results"][idx]["status"] = "downloading"
    tasks[task_id]["results"][idx]["speed"] = "Fetching stream..."
    tasks[task_id]["results"][idx]["progress"] = 3

    # ── Attempt 1: Piped API (instant stream URL, no wait) ────────────────
    piped_instances = [
        "https://pipedapi.kavin.rocks",
        "https://pipedapi.r4fo.com",
        "https://api.piped.projectsegfault.com",
        "https://pipedapi.tokhmi.xyz",
    ]
    for piped in piped_instances:
        try:
            tasks[task_id]["results"][idx]["speed"] = f"Connecting via Piped..."
            r = requests.get(f"{piped}/streams/{video_id}", timeout=8)
            if r.status_code == 200:
                data = r.json()
                title = re.sub(r'[\\/*?"<>|]', "", data.get("title", f"yt_{video_id}")).strip()[:80] or f"yt_{video_id}"

                if want_audio_only:
                    streams = data.get("audioStreams", [])
                    streams = sorted(streams, key=lambda s: s.get("bitrate", 0), reverse=True)
                    ext = "mp3"
                else:
                    streams = data.get("videoStreams", [])
                    target_h = 1080 if fmt in ("1080", "best") else (720 if fmt == "720" else 1080)
                    combined = [s for s in streams if s.get("videoOnly") is False]
                    if combined:
                        streams = sorted(combined, key=lambda s: abs(int(s.get("height", 0)) - target_h))
                    else:
                        streams = sorted(streams, key=lambda s: abs(int(s.get("height", 0)) - target_h))
                    ext = "mp4"

                if streams:
                    stream_url = streams[0].get("url")
                    if stream_url:
                        filename = f"{title}.{ext}"
                        dest_file = output_path / filename
                        tasks[task_id]["results"][idx]["filename"] = filename
                        ok = _stream_download_file(task_id, idx, stream_url, dest_file, filename)
                        if ok:
                            return True, "Success via Piped"
        except Exception:
            continue

    # ── Attempt 2: Invidious API (instant stream URL, no wait) ───────────
    invidious_instances = [
        "https://inv.nadeko.net",
        "https://invidious.nerdvpn.de",
        "https://yt.artemislena.eu",
        "https://vid.puffyan.us",
    ]
    for inv in invidious_instances:
        try:
            tasks[task_id]["results"][idx]["speed"] = "Connecting via Invidious..."
            r = requests.get(f"{inv}/api/v1/videos/{video_id}?fields=title,adaptiveFormats,formatStreams", timeout=8)
            if r.status_code == 200:
                data = r.json()
                title = re.sub(r'[\\/*?"<>|]', "", data.get("title", f"yt_{video_id}")).strip()[:80] or f"yt_{video_id}"

                if want_audio_only:
                    streams = [s for s in data.get("adaptiveFormats", []) if "audio" in s.get("type", "")]
                    streams = sorted(streams, key=lambda s: int(s.get("bitrate", "0")), reverse=True)
                    ext = "mp3"
                else:
                    streams = data.get("formatStreams", [])
                    if not streams:
                        streams = [s for s in data.get("adaptiveFormats", []) if "video" in s.get("type", "")]
                    ext = "mp4"

                if streams:
                    stream_url = streams[0].get("url")
                    if stream_url:
                        filename = f"{title}.{ext}"
                        dest_file = output_path / filename
                        tasks[task_id]["results"][idx]["filename"] = filename
                        ok = _stream_download_file(task_id, idx, stream_url, dest_file, filename)
                        if ok:
                            return True, "Success via Invidious"
        except Exception:
            continue

    # ── Attempt 3: Cobalt API ─────────────────────────────────────────────
    try:
        tasks[task_id]["results"][idx]["speed"] = "Connecting via Cobalt..."
        payload = {"url": url}
        if want_audio_only:
            payload["downloadMode"] = "audio"
            payload["audioFormat"] = "mp3"
        else:
            payload["downloadMode"] = "auto"
            payload["videoQuality"] = "1080" if fmt in ("1080", "best") else "720"

        hdrs = {"Accept": "application/json", "Content-Type": "application/json"}
        r = requests.post("https://api.cobalt.tools", json=payload, headers=hdrs, timeout=12)
        if r.status_code == 200:
            data = r.json()
            stream_url = data.get("url")
            if stream_url:
                ext = "mp3" if want_audio_only else "mp4"
                filename = data.get("filename") or f"yt_{video_id}.{ext}"
                dest_file = output_path / filename
                tasks[task_id]["results"][idx]["filename"] = filename
                ok = _stream_download_file(task_id, idx, stream_url, dest_file, filename)
                if ok:
                    return True, "Success via Cobalt"
    except Exception:
        pass

    # ── Attempt 4: Loader.to (slower — polls until stream is ready) ────────
    try:
        tasks[task_id]["results"][idx]["speed"] = "Preparing via Loader..."
        format_code = "mp3" if want_audio_only else ("1080" if fmt in ("1080", "best") else "720")
        api_url = f"https://loader.to/ajax/download.php?format={format_code}&url={url}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://en.loader.to/"
        }
        r = requests.get(api_url, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get("success"):
                title_raw = data.get("title") or f"yt_{video_id}"
                title = re.sub(r'[\\/*?"<>|]', "", title_raw).strip()[:80] or f"yt_{video_id}"
                tasks[task_id]["results"][idx]["filename"] = f"{title}.{'mp3' if want_audio_only else 'mp4'}"
                prog_url = data.get("progress_url")
                if prog_url:
                    for wait_i in range(25):
                        time.sleep(1.0)
                        if tasks[task_id].get("status") == "stopped":
                            return True, "Stopped"
                        tasks[task_id]["results"][idx]["progress"] = min(5 + wait_i * 2, 30)
                        tasks[task_id]["results"][idx]["speed"] = f"Preparing stream ({wait_i+1}/25)..."
                        pr = requests.get(prog_url, headers=headers, timeout=10)
                        if pr.status_code == 200:
                            pdata = pr.json()
                            dl_url = pdata.get("download_url")
                            if dl_url:
                                ext = "mp3" if want_audio_only else "mp4"
                                filename = f"{title}.{ext}"
                                dest_file = output_path / filename
                                ok = _stream_download_file(task_id, idx, dl_url, dest_file, filename)
                                if ok:
                                    return True, "Success via Loader.to"
                                break
    except Exception:
        pass

    return False, "All YouTube fallback APIs failed"


def build_ydl_args(url, fmt, output_path, use_cookies=False):
    """Build yt-dlp argument list with platform-specific flags and speed optimisations."""
    platform = detect_platform(url)

    args = get_yt_dlp_cmd() + [
        "--no-playlist",
        "--newline",
        "-o", str(output_path / "%(title)s.%(ext)s"),
        "--no-warnings",
        "--concurrent-fragments", "8",   # parallel fragment downloads for speed
        "--buffer-size", "16K",
        "--http-chunk-size", "10M",
    ]

    # Platform-specific flags
    if "youtube.com" in url.lower() or "youtu.be" in url.lower():
        args += [
            "--extractor-args", "youtube:player_client=android,ios,mweb,web_creator",
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ]
    else:
        args += [
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ]

    if platform == "tiktok":
        args += [
            "--extractor-args", "tiktok:api_hostname=api16-normal-c-useast1a.tiktokv.com",
        ]

    if platform == "instagram":
        args += ["--extractor-args", "instagram:player_client=web"]

    # Format selection
    if fmt == "mp3":
        args += ["-x", "--audio-format", "mp3"]
    elif fmt == "best":
        args += ["-f", "bestvideo+bestaudio/best", "--merge-output-format", "mp4"]
    elif fmt == "720":
        args += ["-f", "bestvideo[height<=720]+bestaudio/best[height<=720]", "--merge-output-format", "mp4"]
    elif fmt == "1080":
        args += ["-f", "bestvideo[height<=1080]+bestaudio/best[height<=1080]", "--merge-output-format", "mp4"]
    else:
        args += ["-f", "best", "--merge-output-format", "mp4"]

    # Cookies
    if use_cookies and COOKIES_FILE.exists() and COOKIES_FILE.stat().st_size > 0:
        args += ["--cookies", str(COOKIES_FILE)]

    args.append(url)
    return args


def _download_one(task_id, idx, url, fmt, use_cookies, save_dir):
    """Download a single URL, update tasks[task_id]['results'][idx] in place."""
    if tasks[task_id].get("status") == "stopped":
        tasks[task_id]["results"][idx]["status"] = "stopped"
        tasks[task_id]["results"][idx]["error"] = "Stopped by user"
        return

    tasks[task_id]["results"][idx]["status"] = "downloading"

    platform = detect_platform(url)

    # TikTok: fast direct API first
    if platform == "tiktok":
        success, msg = download_tiktok_direct(task_id, idx, url, fmt, save_dir)
        if success:
            return

    args = build_ydl_args(url, fmt, save_dir, use_cookies)
    try:
        proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace"
        )
        # Register this subprocess so /stop can kill it
        active_procs.setdefault(task_id, {})[idx] = proc

        output_lines = []
        for line in proc.stdout:
            if tasks[task_id].get("status") == "stopped":
                proc.kill()
                break
            line = line.strip()
            output_lines.append(line)

            # Parse progress + speed + ETA + total size + downloaded size
            if "[download]" in line and "%" in line:
                pct_m = re.search(r'(\d+(?:\.\d+)?)%', line)
                if pct_m:
                    try:
                        tasks[task_id]["results"][idx]["progress"] = float(pct_m.group(1))
                    except Exception:
                        pass
                size_m = re.search(r'of\s+~?(\d+(?:\.\d+)?\s*\w+)', line)
                if size_m:
                    tasks[task_id]["results"][idx]["total_size"] = size_m.group(1).strip()
                    try:
                        pct_val = tasks[task_id]["results"][idx].get("progress", 0)
                        tot_match = re.match(r'(\d+(?:\.\d+)?)\s*([A-Za-z]+)', size_m.group(1).strip())
                        if tot_match:
                            num = float(tot_match.group(1))
                            unit = tot_match.group(2)
                            dl_num = (pct_val / 100.0) * num
                            tasks[task_id]["results"][idx]["downloaded_size"] = f"{dl_num:.1f} {unit}"
                    except Exception:
                        pass
                speed_m = re.search(r'at\s+~?(\d+(?:\.\d+)?\s*\S+/(?:s|sec))', line)
                if speed_m:
                    tasks[task_id]["results"][idx]["speed"] = speed_m.group(1).strip()
                eta_m = re.search(r'ETA\s+(\S+)', line)
                if eta_m:
                    tasks[task_id]["results"][idx]["eta"] = eta_m.group(1).strip()

            # Detect filename
            if "[download] Destination:" in line:
                fname = line.replace("[download] Destination:", "").strip()
                tasks[task_id]["results"][idx]["filename"] = Path(fname).name
            if "[Merger] Merging formats into" in line or "[ExtractAudio] Destination:" in line:
                fname = line.split('"')
                if len(fname) > 1:
                    tasks[task_id]["results"][idx]["filename"] = Path(fname[1]).name
            if "has already been downloaded" in line:
                fname_part = line.split("[download]")[1].strip().split(" has")[0].strip()
                tasks[task_id]["results"][idx]["filename"] = Path(fname_part).name

        proc.wait()

        if tasks[task_id].get("status") == "stopped":
            tasks[task_id]["results"][idx]["status"] = "stopped"
            tasks[task_id]["results"][idx]["error"] = "Stopped by user"
        elif proc.returncode == 0:
            tasks[task_id]["results"][idx]["status"] = "done"
            tasks[task_id]["results"][idx]["progress"] = 100
        else:
            err_text = "\n".join(output_lines[-6:])
            # If YouTube bot error on datacenter IP, attempt fallback
            if ("Sign in to confirm" in err_text or "bot" in err_text.lower()) and ("youtube.com" in url.lower() or "youtu.be" in url.lower()):
                success, msg = download_youtube_direct(task_id, idx, url, fmt, save_dir)
                if success:
                    return
            tasks[task_id]["results"][idx]["status"] = "failed"
            tasks[task_id]["results"][idx]["error"] = err_text

    except Exception as e:
        if tasks[task_id].get("status") != "stopped":
            tasks[task_id]["results"][idx]["status"] = "failed"
            tasks[task_id]["results"][idx]["error"] = str(e)
    finally:
        active_procs.get(task_id, {}).pop(idx, None)


def run_download(task_id, urls, fmt, use_cookies, save_dir=None):
    """Run downloads. For bulk, uses ThreadPoolExecutor with 3 concurrent workers."""
    global SAVE_DIR
    if save_dir is None:
        save_dir = SAVE_DIR

    tasks[task_id]["status"] = "running"

    # Pre-populate all results as pending so the frontend can show the full list immediately
    tasks[task_id]["results"] = [
        {"url": u, "status": "pending", "progress": 0,
         "filename": None, "error": None, "speed": None, "eta": None, "total_size": None, "downloaded_size": None}
        for u in urls
    ]

    is_bulk = len(urls) > 1

    if is_bulk:
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(_download_one, task_id, idx, url, fmt, use_cookies, save_dir): idx
                for idx, url in enumerate(urls)
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    pass
    else:
        _download_one(task_id, 0, urls[0], fmt, use_cookies, save_dir)

    if tasks[task_id].get("status") != "stopped":
        tasks[task_id]["status"] = "done"


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/get-save-dir", methods=["GET"])
def get_save_dir():
    """Return the current download folder."""
    global SAVE_DIR
    return jsonify({"folder": str(SAVE_DIR)})


@app.route("/select-folder", methods=["POST"])
def select_folder():
    """Open a native folder-picker dialog using PowerShell."""
    global SAVE_DIR
    try:
        ps_script = (
            "Add-Type -AssemblyName System.windows.forms; "
            "$f = New-Object System.Windows.Forms.FolderBrowserDialog; "
            "$f.Description = 'Select Download Folder'; "
            "$f.SelectedPath = '" + str(SAVE_DIR).replace("'", "''") + "'; "
            "if ($f.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) { Write-Output $f.SelectedPath }"
        )
        cmd = ["powershell", "-NoProfile", "-Command", ps_script]
        
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60, creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        chosen = proc.stdout.strip()
        
        if chosen and os.path.isdir(chosen):
            SAVE_DIR = Path(chosen)
            SAVE_DIR.mkdir(parents=True, exist_ok=True)
            return jsonify({"success": True, "folder": str(SAVE_DIR)})
            
        return jsonify({"success": False, "folder": str(SAVE_DIR), "error": "Dialog closed or no folder selected"})
    except Exception as e:
        print("Picker exception:", str(e))
        return jsonify({"success": False, "error": str(e), "folder": str(SAVE_DIR)})


@app.route("/info", methods=["POST"])
def get_info():
    data = request.get_json() or {}
    url = data.get("url", "").strip()
    use_cookies = data.get("use_cookies", False)

    if not url:
        return jsonify({"error": "URL required"}), 400

    platform = detect_platform(url)

    # Instant metadata for TikTok via direct API (< 0.5s)
    if platform == "tiktok":
        try:
            tik_res = requests.post("https://www.tikwm.com/api/", data={"url": url, "hd": 1}, timeout=8).json()
            if tik_res.get("code") == 0 and tik_res.get("data"):
                tdata = tik_res["data"]
                bytes_size = tdata.get("size") or tdata.get("hd_size")
                size_mb = round(bytes_size / (1024 * 1024), 2) if bytes_size else None
                duration_sec = tdata.get("duration", 0)
                mins = int(duration_sec // 60)
                secs = int(duration_sec % 60)
                return jsonify({
                    "title": tdata.get("title") or "TikTok Video",
                    "thumbnail": tdata.get("cover") or tdata.get("origin_cover"),
                    "duration": f"{mins}:{secs:02d}",
                    "size_mb": size_mb,
                    "uploader": tdata.get("author", {}).get("nickname") or tdata.get("author", {}).get("unique_id"),
                    "platform": "tiktok"
                })
        except Exception:
            pass

    # Instant metadata for YouTube via oEmbed (< 0.3s)
    if "youtube.com" in url.lower() or "youtu.be" in url.lower():
        try:
            oembed = requests.get(f"https://www.youtube.com/oembed?url={url}&format=json", timeout=4).json()
            return jsonify({
                "title": oembed.get("title", "YouTube Video"),
                "thumbnail": oembed.get("thumbnail_url", ""),
                "duration": "HD Video",
                "size_mb": None,
                "uploader": oembed.get("author_name", "YouTube"),
                "platform": "youtube"
            })
        except Exception:
            pass

    cmd = get_yt_dlp_cmd() + ["--dump-json", "--no-playlist"]
    if "youtube.com" in url.lower() or "youtu.be" in url.lower():
        cmd += [
            "--extractor-args", "youtube:player_client=android_vr,android_creator,android_embedded,android",
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ]
    else:
        cmd += [
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ]

    if platform == "tiktok":
        cmd += ["--extractor-args", "tiktok:api_hostname=api16-normal-c-useast1a.tiktokv.com"]
    if platform == "instagram":
        cmd += ["--extractor-args", "instagram:player_client=web"]

    if use_cookies and COOKIES_FILE.exists() and COOKIES_FILE.stat().st_size > 0:
        cmd += ["--cookies", str(COOKIES_FILE)]

    cmd.append(url)

    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15
        )
        if proc.returncode != 0:
            # YouTube oEmbed fallback if yt-dlp metadata fetch gets bot challenge
            if "youtube.com" in url.lower() or "youtu.be" in url.lower():
                try:
                    oembed = requests.get(f"https://www.youtube.com/oembed?url={url}&format=json", timeout=6).json()
                    return jsonify({
                        "title": oembed.get("title", "YouTube Video"),
                        "thumbnail": oembed.get("thumbnail_url", ""),
                        "duration": "HD Video",
                        "size_mb": None,
                        "uploader": oembed.get("author_name", "YouTube"),
                        "platform": "youtube"
                    })
                except Exception:
                    pass

            err_msg = proc.stderr or "Could not fetch video info."
            if "ERROR:" in err_msg:
                err_msg = err_msg.split("ERROR:")[-1].strip()
            return jsonify({"error": err_msg}), 400

        info = json.loads(proc.stdout)

        bytes_size = info.get("filesize") or info.get("filesize_approx")
        size_mb = round(bytes_size / (1024 * 1024), 2) if bytes_size else None

        duration_sec = info.get("duration")
        duration_str = None
        if duration_sec:
            mins = int(duration_sec // 60)
            secs = int(duration_sec % 60)
            duration_str = f"{mins}:{secs:02d}"

        thumbnail = info.get("thumbnail")
        if not thumbnail and info.get("thumbnails"):
            thumbnail = info.get("thumbnails")[-1]["url"]

        return jsonify({
            "title": info.get("title"),
            "thumbnail": thumbnail,
            "duration": duration_str,
            "size_mb": size_mb,
            "uploader": info.get("uploader") or info.get("channel"),
            "platform": platform
        })
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Request timed out fetching video info."}), 408
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/download", methods=["POST"])
def start_download():
    data = request.get_json()
    url = data.get("url", "").strip()
    fmt = data.get("format", "best")
    use_cookies = data.get("use_cookies", False)

    if not url:
        return jsonify({"error": "URL required"}), 400

    task_id = str(uuid.uuid4())
    tasks[task_id] = {"status": "pending", "results": [], "type": "single"}

    t = threading.Thread(target=run_download, args=(task_id, [url], fmt, use_cookies), daemon=True)
    t.start()

    return jsonify({"task_id": task_id})


@app.route("/bulk-download", methods=["POST"])
def bulk_download():
    data = request.get_json()
    urls_raw = data.get("urls", "")
    fmt = data.get("format", "best")
    use_cookies = data.get("use_cookies", False)

    urls = [u.strip() for u in urls_raw.strip().splitlines() if u.strip()]
    if not urls:
        return jsonify({"error": "At least one URL required"}), 400

    task_id = str(uuid.uuid4())
    tasks[task_id] = {"status": "pending", "results": [], "type": "bulk"}

    t = threading.Thread(target=run_download, args=(task_id, urls, fmt, use_cookies), daemon=True)
    t.start()

    return jsonify({"task_id": task_id})


@app.route("/stop/<task_id>", methods=["POST"])
def stop_task(task_id):
    """Stop a running download task and kill all active subprocesses."""
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    if task["status"] not in ("running", "pending"):
        return jsonify({"error": "Task is not running"}), 400

    tasks[task_id]["status"] = "stopped"

    # Kill every active subprocess for this task
    procs = active_procs.get(task_id, {})
    for proc in procs.values():
        try:
            proc.kill()
        except Exception:
            pass

    return jsonify({"success": True, "message": "Download stopped."})


@app.route("/status/<task_id>")
def get_status(task_id):
    """SSE endpoint to stream task status."""
    def generate():
        while True:
            task = tasks.get(task_id)
            if not task:
                yield f"data: {json.dumps({'error': 'Task not found'})}\n\n"
                break
            yield f"data: {json.dumps(task)}\n\n"
            if task["status"] in ("done", "stopped"):
                break
            time.sleep(0.3)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/save-cookies", methods=["POST"])
def save_cookies():
    data = request.get_json()
    content = data.get("cookies", "").strip()
    COOKIES_FILE.write_text(content, encoding="utf-8")
    return jsonify({"success": True, "message": "Cookies saved!"})


@app.route("/get-cookies", methods=["GET"])
def get_cookies():
    if COOKIES_FILE.exists():
        content = COOKIES_FILE.read_text(encoding="utf-8")
    else:
        content = ""
    return jsonify({"cookies": content})


@app.route("/clear-cookies", methods=["POST"])
def clear_cookies():
    COOKIES_FILE.write_text("", encoding="utf-8")
    return jsonify({"success": True})


@app.route("/files/<path:filename>")
def serve_file(filename):
    return send_from_directory(SAVE_DIR, filename, as_attachment=True)


@app.route("/retry-item", methods=["POST"])
def retry_item():
    data = request.get_json() or {}
    task_id = data.get("task_id")
    idx = data.get("idx")
    fmt = data.get("format", "best")
    use_cookies = data.get("use_cookies", False)

    if not task_id or idx is None:
        return jsonify({"error": "task_id and idx required"}), 400

    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404

    results = task.get("results", [])
    if idx < 0 or idx >= len(results):
        return jsonify({"error": "Invalid item index"}), 400

    url = results[idx]["url"]
    results[idx] = {
        "url": url,
        "status": "pending",
        "progress": 0,
        "filename": None,
        "error": None,
        "speed": None,
        "eta": None,
        "total_size": None,
        "downloaded_size": None,
    }
    task["status"] = "running"

    def _do_retry():
        _download_one(task_id, idx, url, fmt, use_cookies, SAVE_DIR)
        all_finished = all(r.get("status") in ("done", "failed", "stopped") for r in tasks[task_id].get("results", []))
        if all_finished and tasks[task_id].get("status") != "stopped":
            tasks[task_id]["status"] = "done"

    threading.Thread(target=_do_retry, daemon=True).start()
    return jsonify({"success": True})


@app.route("/list-downloads")
def list_downloads():
    files = []
    try:
        for f in SAVE_DIR.iterdir():
            if f.is_file():
                size_mb = round(f.stat().st_size / (1024 * 1024), 2)
                files.append({"name": f.name, "size_mb": size_mb})
    except Exception:
        pass
    files.sort(key=lambda x: x["name"])
    return jsonify({"files": files})


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace') if hasattr(sys.stdout, 'reconfigure') else None
    port = int(os.environ.get("PORT", 5000))
    print("=" * 50)
    print("  VideoGrab Pro — by M. Mughees")
    print(f"  Open: http://127.0.0.1:{port}")
    print("=" * 50)
    app.run(debug=False, host="0.0.0.0", port=port, threaded=True)
