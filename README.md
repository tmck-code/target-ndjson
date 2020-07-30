# target-ndjson

This is a [Singer](https://singer.io) target that reads JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

---

## Function

This target writes to files in NDJSON (aka "Newline-Delimited JSON" files). This
essentially unpacks each `"RECORD"` log recived, verbatim, and writes to file.

This target is intended for use in systems that implement their own data
processing layers.

---

## TODO

This repository is still a WIP! Next tasks, in no particular order:

- [ ] Type hinting
- [ ] Unit tests
- [ ] Flatten option?
