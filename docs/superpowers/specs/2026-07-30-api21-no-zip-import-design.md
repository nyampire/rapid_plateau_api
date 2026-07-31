# Design: `--no-zip` importer mode + local-conversion wrapper outline (#21)

> **Status: proposal, not implemented (as of 2026-07-30).** Phase 0 of #21 is
> verified (all five go/no-go checks passed in the issue thread). Nothing in
> Deliverables A–C below exists in the code yet. This spec covers the
> migration-independent first steps: a detailed design for the importer
> `--no-zip` mode (Phase 2 building block) plus a hardening fix, and a
> high-level outline of the Phase 1 local-conversion wrapper. Parts that depend
> on the still-undecided server-migration direction are marked **TBD**.

Date: 2026-07-30
Issue: #21 (switch upstream acquisition to local citygml-osm conversion + push)
Related: #31 (cross-city dedup, merged) — same mesh-tile-straddles-boundary
phenomenon; #33 (Lod1Solid duplicate part filter, proposal)

## Problem

Converted OSM XML for PLATEAU CityGML is currently obtained from a third-party
upstream service. That service occasionally goes down (a 502 was observed on
2026-07-29), and concentrating download load upstream is undesirable as V5
(2025 edition) cities are imported over time.

`citygml-osm` (Java 17 / Maven, v3.0.6) is OSS and can convert CityGML to OSM
XML locally. Phase 0 verified that its `1st` output is byte-compatible with
what `plateau_importer2postgis.py` expects (row-count parity confirmed on two
V4 meshes; V5 relation/part structure confirmed). On 2026-07-29 three Kumamoto
cities (43100 / 43213 / 43443) were added to production by running the full
pipeline **by hand**, which established a working blueprint.

Two friction points surfaced from that manual run:

1. **The importer only reads `*.zip`.** `run_complete_import()` globs
   `data_dir/*.zip`, and errors out if none are found. To feed locally
   converted `.osm` files in, the manual runbook had to re-pack each
   `<mesh>.osm` into a `<mesh>_bldg_6697_op.zip` purely so the glob-then-extract
   step would accept it. This is wasted I/O with no benefit.

2. **Node keys are namespaced by basename.** During import, nodes and building
   node-refs are keyed as `f"{osm_file.name}:{id}"` — i.e. by the file's
   *basename*, not its path. Mesh codes are unique **within** one city, so this
   is correct for the one-city-per-import model. But mesh tiles straddle
   municipal boundaries, so the same mesh code (e.g. `53385729.osm`) can appear
   in two neighbouring cities. If two such files ever landed in one import run,
   their nodes would collide in the in-memory dict and silently corrupt
   coordinates. This latent property exists in the zip flow today too; it is
   simply never triggered because each city is imported separately.

## Goals

- Let the importer read locally converted `.osm` directly, with no zip
  re-packing (removes step 4 of the manual runbook).
- Make node keying robust against basename collisions, so a mixed-city
  directory cannot corrupt data even by accident.
- Sketch the Phase 1 wrapper structure so the migration-independent stages can
  be built without waiting on the migration decision.

## Non-goals

- Deciding the server-migration destination. Phase 1 wrapper stages that depend
  on it (transport target, import execution host, coverage refresh) stay
  **TBD**.
- Making coverage-view refresh robust under the production memory limit. The
  existing operational procedure is referenced, not changed.
- Pinning the `citygml-osm` version. That is decided just before Phase 4.

## Deliverable A: importer `--no-zip` mode

### Behaviour

Add a `--no-zip` boolean flag to `plateau_importer2postgis.py`. When set, the
importer skips zip discovery and extraction entirely and collects `.osm` files
directly from the data directory:

```
python plateau_importer2postgis.py --data-dir plateau_data/43100 \
    --no-zip --citycode 43100 --postgres-url "$DATABASE_URL"
```

Discovery uses a **recursive glob** — `sorted(data_dir.rglob("*.osm"))` — which
is a strict superset of both a flat layout and the `extracted/<mesh>/` layout,
so it works regardless of exactly how the converted files are laid out. If no
`.osm` file is found, the run fails cleanly with a message naming `--no-zip`
mode (mirroring the existing "no zip found" error).

### Code changes (`plateau_importer2postgis.py`)

- CLI: add `--no-zip` (`action='store_true'`); thread a `no_zip` flag into the
  importer constructor.
- `run_complete_import()` Phase 2/3: branch on `no_zip`.
  - Default: unchanged — `find_zip_files()` then `extract_zip_files()`.
  - `--no-zip`: skip both; `osm_files = sorted(self.data_dir.rglob("*.osm"))`;
    if empty, log an error naming `--no-zip` and return `False`.
- Phase 4 onward (batch splitting, the `--citycode` pre-delete/re-import, DB
  insertion) is **unchanged**.
- `create_import_report()`: in `--no-zip` mode the `zip_count` argument is `0`
  (report shows zip count as `0` / not-applicable).

### Idempotency

`--no-zip` has no extraction step, so the `existing_osm` skip is unused. Re-run
cleanliness is already guaranteed by the existing `--citycode` pre-delete at the
top of Phase 4 (deletes the city's rows before re-inserting). No change needed.

## Deliverable B: relative-path node key (hardening, both flows)

Change the per-file key namespace from basename to the path **relative to
`data_dir`**, applied uniformly to both the zip and `--no-zip` flows:

```python
# in run_complete_import(), per-file loop (around the node/ref keying)
key_base = str(osm_file.relative_to(self.data_dir))   # e.g. "extracted/53385729/53385729.osm"
file_specific_key = f"{key_base}:{original_id}"
# building node_refs use the same key_base
```

Because keys only need to be **consistent** between the node dict and the
building refs (the format itself is opaque downstream), and because mesh names
are already unique within a correctly-used single-city directory, this is a
**no-op for current correct usage** and strictly safer: two files sharing a
basename in different subdirectories no longer collide.

Implementation note: before changing the format, `grep` for every use of the
`file_specific_key` / node-ref key to confirm nothing parses or depends on the
basename shape. From the current code the keys are opaque (used only as dict
keys and matched refs), but this must be verified during implementation.

## Deliverable C: Phase 1 local-conversion wrapper (high-level)

Per-city pipeline. Stages ①–③ run locally and are **independent** of the
migration decision; stages ④–⑥ have migration-dependent targets marked **TBD**.

| Stage | What | Migration-dependent? |
|---|---|---|
| ① download | Locate the package via the G-Kukan (G空間) CKAN API → download the CityGML zip → extract `udx/bldg/*.gml` | No (local) |
| ② convert | Run `citygml-osm` `1st` (Java 17) over the meshes → `<mesh>.osm` | No structurally; JVM heap for large cities needs measurement |
| ③ package | Place output as `plateau_data/<code>/extracted/<mesh>/<mesh>.osm` (the zip-flow layout, **without** zipping) | No (local) |
| ④ transport | `rsync` the directory to the serving host | **TBD** (destination depends on migration target) |
| ⑤ import | On the server, run the importer with `--no-zip --citycode <code>`, once per city | Partly **TBD** (execution host; integration with the existing re-import wrapper) |
| ⑥ coverage refresh | After all cities are imported, refresh once; under the production memory limit this needs care per existing operational procedure | **TBD** (may change with migration) |

### Design rule: one directory = one city_code

`plateau_data/<citycode>/` is dedicated to a single city. The wrapper never
mixes cities in one directory and imports each city with its own
`--citycode <code>` invocation (as the manual Kumamoto run did). `city_code` is
the isolation boundary; the same mesh tile shared by adjacent cities enters the
DB under each city's own `city_code` and is de-duplicated at API output by #31.

### Structure

The wrapper is a sequence of discrete per-city steps mirroring the manual
runbook, so a failure can be resumed at the failed stage rather than restarting
the whole city. Whether stages ①–③ are one script or a small set of composable
scripts is an implementation detail for the Phase 1 plan; the migration-
dependent stages ④–⑥ are specified only at the interface level here (inputs,
outputs, per-city `--citycode`) and detailed once the migration target is
decided.

## Testing

- **`--no-zip` integration test**: point the importer at a directory of
  `extracted/**/*.osm` (no zips) and assert building / node / part counts match
  an existing fixture imported via the zip flow.
- **Relative-path key test**: two `.osm` files with the *same basename* in
  different subdirectories, containing overlapping node ids, are both imported
  without collision (would fail under the old basename keying).
- Existing importer tests must stay green (the key change is a no-op for the
  single-city fixtures).

## Rollout

Deliverables A and B are self-contained importer changes with no dependency on
the migration decision and can ship first. Deliverable C's local stages (①–③)
can follow. The transport/import/refresh stages (④–⑥) are deferred until the
server-migration direction is settled.
