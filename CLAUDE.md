# CLAUDE.md

## Pre-commit checks

Run these before committing:

```bash
uv run ruff format .
uv run ruff check .
uv run ty check
uv run pytest
```
