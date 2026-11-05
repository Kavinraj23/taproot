# Taproot

## Ground rules

- Treat `docs/design.md` as settled. Ask before deviating from it.
- Work on one task at a time, from a plan made in Claude's plan mode (`/plan`). Don't write plan files to `docs/`.
- Write tests from the plan's test cases before implementing.
- Explain what changed after each task.

## Hand-written pieces

Do not write these. Give hints instead (approach, pitfalls, pointers to the design doc), and let the user write the code. See the ownership table in `docs/design.md`.

1. Context propagation in the Python Kafka worker (learning target 1: trace propagation)
2. Dedup keys and commit-after-write in the backend processor (learning target 2: reliable data movement)
3. Root-cause graph walk, shared dependency + timing order (learning target 3: graph-based root cause)

You may write the tests for these and the code around them, but not the pieces themselves.
