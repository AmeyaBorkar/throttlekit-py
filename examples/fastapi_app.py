"""A FastAPI app rate-limited by ThrottleKit's `contrib.fastapi` adapter.

The idiomatic FastAPI tool is a dependency: `Depends(RateLimit(bind_policy(backend, "api")))` admits (or
rejects with a 429 + `Retry-After` / `RateLimit-*` headers) before the route runs, and stamps `RateLimit-*`
onto the admitted response. The adapter fails OPEN if the backend is unreachable (`on_unavailable="allow"`)
and keys on the raw connecting peer (never a forgeable `X-Forwarded-For`). A sync `ServiceBackend` is fine
in an async app — the adapter runs its blocking `check` in a worker thread, so the event loop never stalls.

    pip install "throttlekit-py[fastapi]" uvicorn
    npx throttlekit-server --config examples/policies.yaml --port 50051
    uvicorn examples.fastapi_app:app --reload
    # then, from another shell, exhaust the burst of 5:
    #   for i in $(seq 1 7); do curl -s -o /dev/null -w "%{http_code}\n" localhost:8000/items; done
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from throttlekit import ServiceBackend, bind_policy
from throttlekit.contrib.fastapi import RateLimit

ADDR = os.environ.get("THROTTLEKIT_ADDR", "localhost:50051")

backend = ServiceBackend(ADDR)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    yield
    backend.close()  # release the gRPC channel on shutdown


app = FastAPI(title="throttlekit-py example", lifespan=lifespan)

# One limit per policy; reuse the dependency across routes (or attach it app-wide via
# FastAPI(dependencies=[...]) or on a router).
limit_api = RateLimit(bind_policy(backend, "api"))


@app.get("/items", dependencies=[Depends(limit_api)])
async def items() -> dict[str, str]:
    return {"status": "ok"}
