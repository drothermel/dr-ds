# CLAUDE.md

## Documentation Style Guide

`dr-ds` is a shared utility library, not fast-moving research code. Treat
docstrings as part of the public API contract.

### What to document

- Add docstrings to every public module, public function, and public
  constant/type alias that would be imported by another repo.
- Keep private helpers undocumented unless their behavior is subtle enough
  that a short docstring materially improves maintenance.
- Prefer docstrings over inline comments. Add inline comments only for
  invariants, filesystem safety details, or non-obvious implementation
  choices that a caller would not learn from the public docstring.

### Docstring content

- Start with a short imperative summary sentence.
- Document behavior, not implementation history.
- Call out input normalization, output shape, ordering guarantees,
  fallback behavior, and failure modes when they matter.
- Include edge cases when they are part of the contract, such as:
  tuple-to-list conversion, set ordering, large-int coercion, null/NaN
  handling, and string fallback behavior.
- Keep examples in tests and README unless a function is hard to use
  correctly without one.

### Style rules

- Be concise. Most function docstrings should be 1-5 lines.
- Use plain English, not section-heavy API reference formatting.
- Do not restate obvious type information already expressed in the
  signature unless runtime behavior differs from the type hint.
- If behavior is intentionally lossy or opinionated, say so explicitly.

## Pre-commit checks

Run these before committing:

```bash
uv run ruff format .
uv run ruff check .
uv run ty check
uv run pytest
```
