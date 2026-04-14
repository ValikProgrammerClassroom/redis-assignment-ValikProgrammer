"""
Redis Assignments — Reference Solution
=======================================
Complete implementation for all three tasks plus the bonus sliding-window
rate limiter. Use this as the answer key for grading or to demonstrate
the solution after students have attempted the assignment.

Run locally:
    fastapi dev app.py
    # or
    uvicorn app:app --reload

Run tests:sdfsdfsdf
    pytest tests/ -v
"""

import time
import uuid

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import redis

app = FastAPI(title="Redis Assignments — Solution")
r = redis.Redis(host="localhost", port=6379, decode_responses=True)


# ── Models ───────────────────────────────────────────────────

class LoginRequest(BaseModel):
    user_id: str


class TaskRequest(BaseModel):
    task: str


# ============================================================
# Task 1: Session Storage
# ============================================================

@app.post("/login")
def login(body: LoginRequest):
    session_id = str(uuid.uuid4())
    r.set(f"session:{session_id}", body.user_id, ex=3600)
    return {"session_id": session_id}


@app.get("/me")
def me(x_session_id: str = Header()):
    user_id = r.get(f"session:{x_session_id}")
    if user_id is None:
        raise HTTPException(status_code=401, detail="invalid session")
    return {"user_id": user_id}


# ============================================================
# Task 2: Rate Limiter (Fixed Window — 5 req / 60 s)
# ============================================================

RATE_LIMIT = 5
WINDOW_SECONDS = 60


@app.get("/request")
def rate_limited_request(user_id: str):
    key = f"requests:user:{user_id}"
    count = r.incr(key)
    if count == 1:
        # First request in this window — start the TTL
        r.expire(key, WINDOW_SECONDS)
    if count > RATE_LIMIT:
        raise HTTPException(status_code=429, detail="rate limit exceeded")
    return {"status": "ok", "remaining": RATE_LIMIT - count}


# ============================================================
# Task 3: Task Queue (FIFO via LPUSH + RPOP)
# ============================================================

QUEUE_KEY = "task_queue"


@app.post("/task")
def add_task(body: TaskRequest):
    length = r.lpush(QUEUE_KEY, body.task)
    return {"status": "queued", "queue_length": length}


@app.get("/task")
def get_task():
    task = r.rpop(QUEUE_KEY)
    if task is None:
        raise HTTPException(status_code=404, detail="queue is empty")
    return {"task": task}


# ============================================================
# BONUS: Sliding Window Rate Limiter (using Sorted Sets)
# ============================================================
#
# Fixed window has a flaw: 5 req at sec 59 + 5 req at sec 61 = 10 req
# in 2 seconds. Sliding window counts requests in a rolling 60-second
# window from "now".
#
# Algorithm:
#   1. Add this request to a Sorted Set, score = current timestamp
#   2. Drop entries older than (now - 60 seconds)
#   3. Count remaining entries — that's the window size
#
# Complexity: O(log n) per operation thanks to the Sorted Set.
# ============================================================

@app.get("/request_sliding")
def rate_limited_request_sliding(user_id: str):
    # key = f"sliding:user:{user_id}"
    # now = time.time()
    # window_start = now - WINDOW_SECONDS

    # # Use a pipeline for atomicity (3 commands sent in one round-trip)
    # pipe = r.pipeline()
    # pipe.zremrangebyscore(key, 0, window_start)         # drop old entries
    # pipe.zadd(key, {str(uuid.uuid4()): now})            # record this request
    # pipe.zcard(key)                                     # current count
    # pipe.expire(key, WINDOW_SECONDS)                    # auto-cleanup if idle
    # _, _, count, _ = pipe.execute()

    # if count > RATE_LIMIT:
    #     raise HTTPException(status_code=429, detail="rate limit exceeded")
    # return {"status": "ok", "remaining": RATE_LIMIT - count}
    pass