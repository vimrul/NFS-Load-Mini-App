import os
import time
import uuid
import threading
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

APP_NAME = "nfs-load-mini"

DEFAULT_BASE_DIR = os.environ.get("NFS_BASE_DIR", "/mnt/nfs")  # mounted NFS path
MAX_SAMPLES = int(os.environ.get("MAX_LAT_SAMPLES", "200000"))  # cap memory usage

app = FastAPI(title=APP_NAME)

JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()


class StartJobRequest(BaseModel):
    base_dir: str = Field(default=DEFAULT_BASE_DIR, description="Directory on mounted NFS")
    sub_dir: str = Field(default="loadtest", description="Subdirectory inside base_dir")
    mode: str = Field(default="mixed", description="read|write|mixed")
    duration_sec: int = Field(default=60, ge=1, le=86400)
    concurrency: int = Field(default=10, ge=1, le=2000)
    file_size_kb: int = Field(default=4, ge=1, le=102400)  # up to 100MB
    files_per_worker: int = Field(default=100, ge=1, le=100000)
    read_ratio: int = Field(default=70, ge=0, le=100, description="Only for mixed mode")
    fsync_each_write: bool = Field(default=False, description="Force durability per write")
    cleanup: bool = Field(default=False, description="Delete files created by this job at end")


def _percentile(values: List[float], p: float) -> Optional[float]:
    if not values:
        return None
    values_sorted = sorted(values)
    k = int(round((p / 100.0) * (len(values_sorted) - 1)))
    k = max(0, min(k, len(values_sorted) - 1))
    return values_sorted[k]


def _write_file(path: str, data: bytes, do_fsync: bool) -> float:
    t0 = time.perf_counter()
    with open(path, "wb") as f:
        f.write(data)
        f.flush()
        if do_fsync:
            os.fsync(f.fileno())
    return (time.perf_counter() - t0) * 1000.0


def _read_file(path: str) -> float:
    t0 = time.perf_counter()
    with open(path, "rb") as f:
        _ = f.read()
    return (time.perf_counter() - t0) * 1000.0


def _worker_loop(job_id: str, worker_id: int, job_dir: str, req: StartJobRequest, stop_at: float) -> Dict[str, Any]:
    # Prepare worker file set
    wid_dir = os.path.join(job_dir, f"w{worker_id}")
    os.makedirs(wid_dir, exist_ok=True)

    file_paths = [os.path.join(wid_dir, f"f{i}.bin") for i in range(req.files_per_worker)]
    payload = os.urandom(req.file_size_kb * 1024)

    lat_all: List[float] = []
    lat_read: List[float] = []
    lat_write: List[float] = []
    ops = 0
    errors = 0

    # seed writes so reads have content
    try:
        for p in file_paths[: min(10, len(file_paths))]:
            _write_file(p, payload, req.fsync_each_write)
    except Exception:
        pass

    i = 0
    while time.time() < stop_at:
        path = file_paths[i % len(file_paths)]
        i += 1

        try:
            if req.mode == "write":
                ms = _write_file(path, payload, req.fsync_each_write)
                lat_write.append(ms)
                lat_all.append(ms)
            elif req.mode == "read":
                # if file missing, write once then read later
                if not os.path.exists(path):
                    _write_file(path, payload, req.fsync_each_write)
                ms = _read_file(path)
                lat_read.append(ms)
                lat_all.append(ms)
            else:
                # mixed: decide based on read_ratio
                r = (i * 37) % 100  # simple deterministic spread
                if r < req.read_ratio:
                    if not os.path.exists(path):
                        _write_file(path, payload, req.fsync_each_write)
                    ms = _read_file(path)
                    lat_read.append(ms)
                    lat_all.append(ms)
                else:
                    ms = _write_file(path, payload, req.fsync_each_write)
                    lat_write.append(ms)
                    lat_all.append(ms)

            ops += 1

            # cap samples to avoid memory blow
            if len(lat_all) > MAX_SAMPLES:
                lat_all = lat_all[-MAX_SAMPLES:]
            if len(lat_read) > MAX_SAMPLES:
                lat_read = lat_read[-MAX_SAMPLES:]
            if len(lat_write) > MAX_SAMPLES:
                lat_write = lat_write[-MAX_SAMPLES:]

        except Exception:
            errors += 1

        # update shared job counters occasionally (cheap)
        if ops % 200 == 0:
            with JOBS_LOCK:
                job = JOBS.get(job_id)
                if job:
                    job["ops_done"] += 200

    return {
        "ops": ops,
        "errors": errors,
        "lat_all": lat_all,
        "lat_read": lat_read,
        "lat_write": lat_write,
        "worker_dir": wid_dir,
    }


def _run_job(job_id: str, req: StartJobRequest):
    job_dir = os.path.join(req.base_dir, req.sub_dir, job_id)
    os.makedirs(job_dir, exist_ok=True)

    started = time.time()
    stop_at = started + req.duration_sec

    with JOBS_LOCK:
        JOBS[job_id]["status"] = "running"
        JOBS[job_id]["job_dir"] = job_dir
        JOBS[job_id]["started_at"] = started

    results = []
    lat_all: List[float] = []
    lat_read: List[float] = []
    lat_write: List[float] = []
    total_ops = 0
    total_errors = 0

    # threads are fine for file I/O
    with ThreadPoolExecutor(max_workers=req.concurrency) as ex:
        futures = [
            ex.submit(_worker_loop, job_id, wid, job_dir, req, stop_at)
            for wid in range(req.concurrency)
        ]
        for f in as_completed(futures):
            r = f.result()
            results.append(r)
            total_ops += r["ops"]
            total_errors += r["errors"]
            lat_all.extend(r["lat_all"])
            lat_read.extend(r["lat_read"])
            lat_write.extend(r["lat_write"])

            # cap global samples
            if len(lat_all) > MAX_SAMPLES:
                lat_all = lat_all[-MAX_SAMPLES:]
            if len(lat_read) > MAX_SAMPLES:
                lat_read = lat_read[-MAX_SAMPLES:]
            if len(lat_write) > MAX_SAMPLES:
                lat_write = lat_write[-MAX_SAMPLES:]

    ended = time.time()
    duration = max(0.001, ended - started)
    ops_per_sec = total_ops / duration

    report = {
        "job_id": job_id,
        "mode": req.mode,
        "base_dir": req.base_dir,
        "job_dir": job_dir,
        "duration_sec": req.duration_sec,
        "concurrency": req.concurrency,
        "file_size_kb": req.file_size_kb,
        "files_per_worker": req.files_per_worker,
        "read_ratio": req.read_ratio if req.mode == "mixed" else None,
        "fsync_each_write": req.fsync_each_write,
        "total_ops": total_ops,
        "ops_per_sec": round(ops_per_sec, 2),
        "errors": total_errors,
        "lat_ms": {
            "all": {
                "avg": round(sum(lat_all) / len(lat_all), 3) if lat_all else None,
                "p95": _percentile(lat_all, 95),
                "p99": _percentile(lat_all, 99),
                "samples": len(lat_all),
            },
            "read": {
                "avg": round(sum(lat_read) / len(lat_read), 3) if lat_read else None,
                "p95": _percentile(lat_read, 95),
                "p99": _percentile(lat_read, 99),
                "samples": len(lat_read),
            },
            "write": {
                "avg": round(sum(lat_write) / len(lat_write), 3) if lat_write else None,
                "p95": _percentile(lat_write, 95),
                "p99": _percentile(lat_write, 99),
                "samples": len(lat_write),
            },
        },
        "started_at": started,
        "ended_at": ended,
    }

    # optional cleanup (only this job's dir)
    if req.cleanup:
        try:
            for root, dirs, files in os.walk(job_dir, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(job_dir)
            report["cleanup_done"] = True
        except Exception:
            report["cleanup_done"] = False

    with JOBS_LOCK:
        JOBS[job_id]["status"] = "done"
        JOBS[job_id]["report"] = report
        JOBS[job_id]["ended_at"] = ended

@app.get("/", response_class=HTMLResponse)
def ui():
    here = os.path.dirname(__file__)
    with open(os.path.join(here, "ui.html"), "r", encoding="utf-8") as f:
        return f.read()

@app.post("/api/jobs")
def start_job(req: StartJobRequest):
    job_id = str(uuid.uuid4())
    with JOBS_LOCK:
        JOBS[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "ops_done": 0,
            "created_at": time.time(),
            "request": req.dict(),
        }

    t = threading.Thread(target=_run_job, args=(job_id, req), daemon=True)
    t.start()

    return {"job_id": job_id, "status": "queued"}

@app.get("/api/jobs/{job_id}")
def job_status(job_id: str):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return {"error": "not_found"}
        # keep response small
        return {
            "job_id": job["job_id"],
            "status": job["status"],
            "ops_done": job.get("ops_done", 0),
            "created_at": job.get("created_at"),
            "started_at": job.get("started_at"),
            "ended_at": job.get("ended_at"),
            "job_dir": job.get("job_dir"),
        }

@app.get("/api/jobs/{job_id}/report")
def job_report(job_id: str):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return {"error": "not_found"}
        if job.get("status") != "done":
            return {"error": "not_ready", "status": job.get("status")}
        return job.get("report", {})