"""Cached, serialized benchmark downloads and useful control errors."""
import asyncio
import logging
import math
import re
import threading
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from time import monotonic
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit

from fastapi import APIRouter, HTTPException, Request

from . import lab_controls as controls
from .lab_controls import ControlledRuntime

base = controls.base
router = APIRouter()
log = logging.getLogger("deals.lab")
context = threading.local()
raw_fetch = base.fetch
request_lock = threading.Lock()
last_request = 0.0






def retry_delay(headers, attempt):
    delays = []
    value = headers.get("Retry-After", "")
    try:
        delays.append(float(value))
    except (ValueError, TypeError):
        try:
            stamp = parsedate_to_datetime(value)
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            delays.append((stamp - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            pass
    for value in re.findall(r"(?:^|;)\s*t\s*=\s*(\d+)", headers.get("RateLimit", "")):
        delays.append(float(value))
    delays = [d for d in delays if math.isfinite(d)]
    return max(1, max(delays) + 1) if delays else min(300, 20 * 2**attempt)


def _page_fetch(url, payload=None):
    global last_request
    host = urlsplit(url)
    if payload is not None or host.scheme != "https" or host.hostname not in {
        "datasets-server.huggingface.co", "raw.githubusercontent.com"
    }:
        # Never automatically retry agent RPCs or POST mutations.
        return raw_fetch(url, payload)

    cache = base.ROOT / "data/lab/.download-cache" / (
        base.hashlib.sha256(url.encode()).hexdigest() + ".json"
    )
    stored = base.read_json(cache, {})
    if isinstance(stored, dict) and stored.get("url") == url and "data" in stored:
        progress("Reusing a downloaded source page")
        return stored["data"]

    with request_lock:
        for attempt in range(6):
            wait_for(
                max(0, 1.5 - (monotonic() - last_request)),
                "Spacing source requests"
            )
            progress(
                "Downloading " + host.hostname
                + " · attempt " + str(attempt + 1) + "/6"
            )
            last_request = monotonic()
            try:
                data = raw_fetch(url)
                wait_for(0, "Download completed")
                base.write_json(cache, {"url": url, "data": data})
                return data
            except HTTPError as error:
                status, headers = error.code, error.headers or {}
                error.close()
                if status not in {429, 500, 502, 503, 504}:
                    raise RuntimeError(
                        "Dataset source returned HTTP " + str(status)
                    ) from None
                if attempt == 5:
                    raise RuntimeError(
                        "Dataset source still returns HTTP " + str(status)
                        + "; six attempts used. Downloaded pages are cached. "
                        "Retry Load later."
                    ) from None
                wait_for(
                    retry_delay(headers, attempt),
                    "Source HTTP " + str(status) + " · waiting before retry"
                )
            except (URLError, TimeoutError) as error:
                if attempt == 5:
                    raise RuntimeError(
                        "Dataset connection failed after six attempts; "
                        "downloaded pages are cached: " + str(error)
                    ) from None
                wait_for(
                    min(60, 5 * 2**attempt),
                    "Source connection interrupted"
                )


# BEGIN_DOWNLOAD_PROGRESS

def download_check():
    job = getattr(context, "job", None)
    if job is None:
        return
    if job.get("download_cancel_requested") or getattr(
        context, "stop", threading.Event()
    ).is_set():
        raise RuntimeError("Download cancelled; cached pages are preserved")
    if not hasattr(context, "deadline"):
        context.deadline = monotonic() + 600
    if monotonic() >= context.deadline:
        raise RuntimeError(
            "Download time budget reached; Load resumes from cached pages"
        )


def progress(message):
    job = getattr(context, "job", None)
    if job is None:
        return
    meter = job.setdefault("download", {
        "completed_pages": 0,
        "cached_pages": 0,
        "http_requests": 0,
        "started_at": base.time(),
        "total_label": {
            "math": "14",
            "bbh": "20",
            "mmlu_pro": "up to 121"
        }.get(job.get("dataset"), "?")
    })
    if message.startswith("Downloading ") and "attempt " in message:
        meter["http_requests"] += 1
    if message.startswith("Reusing a downloaded"):
        context.cache_hit = True
    page = getattr(context, "page_label", "")
    job["progress"] = (
        str(job.get("dataset", "data"))
        + " | " + str(meter["completed_pages"])
        + "/" + meter["total_label"]
        + " pages done | " + page + " | " + message
    )


def wait_for(seconds, message):
    download_check()
    if (
        hasattr(context, "deadline")
        and seconds > context.deadline - monotonic()
    ):
        raise RuntimeError(
            "Source wait exceeds remaining download budget; "
            "retry Load later using cached pages"
        )
    event = getattr(context, "stop", None) or threading.Event()
    deadline = monotonic() + max(0, seconds)
    while True:
        download_check()
        if event.is_set():
            raise RuntimeError(
                "Download interrupted; cached pages are preserved"
            )
        remaining = deadline - monotonic()
        if remaining <= 0:
            return
        progress(
            message + " | waiting "
            + str(math.ceil(remaining)) + "s"
        )
        event.wait(min(0.25, remaining))


def reliable_fetch(url, payload=None):
    from urllib.parse import parse_qs
    host = urlsplit(url)
    tracked = (
        payload is None
        and host.scheme == "https"
        and host.hostname in {
            "datasets-server.huggingface.co",
            "raw.githubusercontent.com"
        }
        and getattr(context, "job", None) is not None
    )
    if not tracked:
        return _page_fetch(url, payload)

    download_check()
    query = parse_qs(host.query)
    context.page_label = (
        query.get("config", [""])[0]
        + "/" + query.get("split", [""])[0]
        + " offset " + query.get("offset", ["0"])[0]
    ) if host.hostname == "datasets-server.huggingface.co" else (
        host.path.rsplit("/", 1)[-1]
    )

    context.cache_hit = False
    progress("Reading source page; attempts apply to this page only")
    result = _page_fetch(url, payload)
    download_check()

    meter = context.job["download"]
    meter["completed_pages"] += 1
    meter["cached_pages"] += int(context.cache_hit)
    progress(
        "Page ready"
        + (" from disk cache" if context.cache_hit else " and cached")
    )
    return result


@router.post("/lab-api/datasets/stop")
async def stop_downloads(request: Request, data: dict):
    runtime = request.app.state.runtime
    controls.coordinator(runtime)

    async with runtime.control_lock:
        requested = data.get("job_ids")
        if requested is not None and (
            not isinstance(requested, list)
            or not all(isinstance(x, str) for x in requested)
        ):
            raise HTTPException(
                422, "job_ids must be a list of strings"
            )

        stopped = []
        for job in runtime.lab_jobs.values():
            if (
                job.get("mode") != "download"
                or job["status"] not in {
                    "queued", "running", "stopping"
                }
            ):
                continue
            if requested is not None and job["id"] not in requested:
                continue

            queued = job["status"] == "queued"
            job.update(
                download_cancel_requested=True,
                status="cancelled" if queued else "stopping",
                progress=(
                    "Download cancelled; cached pages kept"
                    if queued else
                    "Stopping download; waiting for the current HTTP request"
                )
            )
            stopped.append(job["id"])

        return {"accepted": True, "job_ids": stopped}

# END_DOWNLOAD_PROGRESS
base.fetch = reliable_fetch


@router.post('/lab-api/datasets')
async def datasets(request: Request, data: dict):
    runtime = request.app.state.runtime
    controls.coordinator(runtime)
    name = data.get('dataset')
    if name not in base.CATALOG:
        raise HTTPException(422, 'Unknown dataset')
    async with runtime.control_lock:
        active = [j for j in runtime.lab_jobs.values() if j['status'] in {'queued', 'running', 'stopping'}]
        if runtime.clearing or any((j.get('mode') != 'download' for j in active)):
            raise HTTPException(409, 'Wait for the evaluation or history clearing to finish')
        existing = next((j for j in active if j.get('dataset') == name), None)
        if existing:
            return existing
        if 'warmup' in data:
            if active:
                raise HTTPException(409, 'Wait for active downloads before importing')
            try:
                base.validate_manifest(data)
            except (ValueError, TypeError, AttributeError) as error:
                raise HTTPException(422, str(error)) from None
            base.write_json(base.ROOT / 'data/lab' / (name + '.json'), data)
            return {'status': 'imported', 'dataset': name}
        loaded = base.read_json(base.ROOT / 'data/lab' / (name + '.json'), None)
        if isinstance(loaded, dict) and loaded.get('dataset') == name:
            try:
                base.validate_manifest(loaded)
            except (ValueError, TypeError, AttributeError):
                pass
            else:
                return {'status': 'ready', 'dataset': name, 'warmup': len(loaded['warmup']), 'test': len(loaded['test'])}
        job = {'id': 'data-' + base.uuid4().hex[:8], 'mode': 'download', 'dataset': name, 'status': 'queued', 'progress': 'Waiting for the download slot', 'results': [], 'created_at': base.time()}
        runtime.lab_jobs[job['id']] = job
        if not hasattr(runtime, 'dataset_download_lock'):
            runtime.dataset_download_lock = asyncio.Lock()

        async def download():
            stop = threading.Event()

            def worker():
                context.job, context.stop = (job, stop)
                try:
                    return base.download_data(name)
                finally:
                    context.__dict__.clear()
            try:
                async with runtime.dataset_download_lock:
                    if job.get('download_cancel_requested'):
                        return
                    job.update(status='running', progress='Downloading ' + name)
                    result = await asyncio.to_thread(worker)
                    job.update(status='completed', progress=str(result))
            except asyncio.CancelledError:
                job.update(status='interrupted', progress='Download interrupted; cached pages are preserved')
                raise
            except Exception as error:
                if job.get('download_cancel_requested'):
                    job.update(status='cancelled', progress='Download cancelled; cached pages preserved')
                else:
                    log.exception('Dataset download failed: %s', name)
                    job.update(status='failed', error=str(error), progress='Download paused; Load retries using cached pages')
            finally:
                stop.set()
        runtime.spawn(download())
        return job


@router.post("/lab-api/history/clear")
async def clear_history(request: Request, data: dict):
    try:
        return await controls.clear_history(request, data)
    except HTTPException:
        raise
    except Exception as error:
        log.exception("History clear failed")
        raise HTTPException(
            500,
            "History clear failed: "
            + type(error).__name__ + ": " + str(error)
        ) from None


_live_cases = {}


def _case_lookup():
    """Case-id -> case across loaded manifests, reloaded when files change."""
    lookup = {}
    for name in base.CATALOG:
        path = base.ROOT / "data/lab" / (name + ".json")
        try:
            stamp = path.stat().st_mtime
        except OSError:
            _live_cases.pop(name, None)
            continue
        cached = _live_cases.get(name)
        if not cached or cached[0] != stamp:
            data = base.read_json(path, {}) or {}
            cases = {
                case["id"]: case
                for phase in ("warmup", "test")
                for case in data.get(phase, []) or []
                if isinstance(case, dict) and "id" in case
            }
            cached = _live_cases[name] = (stamp, cases)
        lookup.update(cached[1])
    return lookup


@router.post("/lab-api/live-grade")
async def live_grade(data: dict):
    """Grade in-progress answers with the same grader the reports use."""
    items = data.get("items")
    if not isinstance(items, list) or len(items) > 5000:
        raise HTTPException(422, "items must be a list of up to 5000 answers")
    lookup = _case_lookup()
    results = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key", ""))
        case = lookup.get(item.get("case_id"))
        answer = item.get("answer")
        results[key] = (
            None if case is None or not isinstance(answer, str)
            else bool(base.grade(case, answer))
        )
    return {"results": results}


router.include_router(controls.router)
