# target-ndjson

This is a [Singer](https://singer.io) target that reads JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

- [target-ndjson](#target-ndjson)
  - [Behaviour](#behaviour)
    - [Performance](#performance)
  - [Testing](#testing)
    - [With Docker](#with-docker)
    - [Without Docker](#without-docker)

---

## Behaviour

This target writes to files in NDJSON (aka "Newline-Delimited JSON" files). This
essentially unpacks each `"RECORD"` log recived, verbatim, and writes to file.

This target is intended for use in systems that implement their own data processing
layers, or that need un-transformed output.

### Performance

In addition to producing "un-transformed" output, there are some additional changes
in this tap to improve overall latency.

- Using `ujson` over standard-lib `json` for JSON serde.
- Keeping output files open, rather than re-opening in append mode per-line

This tap is designed for circumstances in which large amounts of data must be ingested
from a tap and written to file, as fast and efficiently as (reasonably) possible.

## Testing

This repository can be tested using `pytest`, with or with out a docker container

### With Docker

```shell
# This Makefile contains handy helper commands for using docker.
# You can also use the docker commands directly (see Makefile for examples)
make build test
```

### Without Docker

```shell
pip install -r requirements.txt
pytest
```

---
