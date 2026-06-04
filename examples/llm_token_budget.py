"""The cost axis — meter what an LLM actually SPENDS as it streams (TALE).

Rate limits count requests; an LLM completion's real cost (tokens) isn't known until it streams. A
`tokenBudget` policy meters a windowed token budget, and you `debit` the tokens as they are produced. A
debit is admitted while budget remains; the crossing debit is counted in full and later debits in the
window are refused (`allowed == False`) — so per-token debiting overshoots the budget by zero.

    npx throttlekit-server --config examples/policies.yaml --port 50051
    python examples/llm_token_budget.py
"""

from __future__ import annotations

import os

from throttlekit import ServiceBackend

ADDR = os.environ.get("THROTTLEKIT_ADDR", "localhost:50051")


def fake_stream(chunks: int, tokens_per_chunk: int) -> list[int]:
    """Stand in for a streaming completion: `chunks` chunks, each costing `tokens_per_chunk` tokens."""
    return [tokens_per_chunk] * chunks


def main() -> None:
    with ServiceBackend(ADDR) as rl:
        tenant = "tenant-1"
        # The `completions` policy budgets 100_000 tokens/min. Spend it down a chunk at a time: the first
        # chunks admit, the chunk that crosses the budget is still admitted (counted in full), and the
        # next debit in the window is refused.
        print("== cost axis: debit streaming tokens against a 100k/min budget ==")
        for i, cost in enumerate(fake_stream(chunks=5, tokens_per_chunk=30_000)):
            d = rl.debit("completions", tenant, tokens=cost)
            print(
                f"  chunk #{i + 1}: debit {cost:>6} -> allowed={d.allowed} remaining={d.remaining}"
            )
            if not d.allowed:
                print("  budget for this window is spent — stop the stream and surface a 429.")
                break


if __name__ == "__main__":
    main()
