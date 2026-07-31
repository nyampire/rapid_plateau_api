# api#21 `--no-zip` importer mode + relative-path key hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `plateau_importer2postgis.py` import locally converted `.osm` files directly (no zip re-packing) and make node keying robust against basename collisions.

**Architecture:** Two testable seams are extracted from `run_complete_import()` so the new logic is unit-testable without a database: `_discover_osm_files()` (honors a new `no_zip` flag) and `_file_key()` (path-relative node key). `run_complete_import()` is rewired to call them; everything downstream (batch split, `--citycode` pre-delete, DB insert) is unchanged.

**Tech Stack:** Python 3, pytest, `pathlib`. Tests use the existing `bare_importer` fixture (no DB) and the existing citygml-osm fixtures under `tests/fixtures/citygml-osm/`.

## Global Constraints

- Public repository — no secrets, hostnames, internal paths, or operational
  commands in code, tests, comments, or commit messages.
- The `--no-zip` change must be a **no-op for the existing zip flow**: default
  behavior (no flag) is byte-identical to today.
- The `_file_key()` change is applied to **both** flows and must be a no-op for
  correctly-used single-city directories (mesh names already unique).
- Commit messages on this project omit `Co-Authored-By` trailers (repo convention).
- Existing test suite must stay green: `pytest tests/test_plateau_importer2postgis.py`.

---

### Task 1: `--no-zip` flag and `_discover_osm_files()` seam

**Files:**
- Modify: `plateau_importer2postgis.py` — `__init__` (around 37-65), `run_complete_import` Phase 2/3 (around 1306-1319), `main()` argparse (around 1437-1461), constructor call (1461)
- Test: `tests/test_plateau_importer2postgis.py`

**Interfaces:**
- Consumes: existing `find_zip_files()` / `extract_zip_files()` (unchanged), `bare_importer` fixture.
- Produces:
  - `PlateauImporter2PostGIS(__init__)` gains keyword arg `no_zip: bool = False`, stored as `self.no_zip`.
  - `PlateauImporter2PostGIS._discover_osm_files() -> tuple[list[Path], int]` returning `(osm_files, zip_count)`. When `self.no_zip` is true: `(sorted(self.data_dir.rglob("*.osm")), 0)`. Otherwise: the zip-flow result and the zip count.
  - CLI flag `--no-zip` (`action='store_true'`).

- [ ] **Step 1: Write the failing test for the no-zip discovery seam**

Add to `tests/test_plateau_importer2postgis.py` (new test class near the importer tests):

```python
from pathlib import Path
import shutil


class TestDiscoverOsmFiles:
    """`_discover_osm_files()` picks the .osm source based on the no_zip flag."""

    FIX_DIR = Path(__file__).parent / 'fixtures' / 'citygml-osm'

    def _place(self, data_dir: Path, rel: str) -> Path:
        """Copy the v4 fixture to data_dir/<rel> and return the path."""
        dest = data_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(self.FIX_DIR / 'v4_outline_only.osm', dest)
        return dest

    def test_no_zip_rglob_finds_nested_osm_and_ignores_zip(self, bare_importer):
        importer = bare_importer(citycode='43100')
        importer.no_zip = True
        data_dir = Path(importer.data_dir)
        self._place(data_dir, 'extracted/53385729/53385729.osm')
        self._place(data_dir, 'extracted/53385730/53385730.osm')
        # A stray .zip must be ignored entirely in no-zip mode.
        (data_dir / 'leftover.zip').write_bytes(b'not a real zip')

        osm_files, zip_count = importer._discover_osm_files()

        names = sorted(p.name for p in osm_files)
        assert names == ['53385729.osm', '53385730.osm']
        assert zip_count == 0

    def test_no_zip_empty_dir_returns_empty(self, bare_importer):
        importer = bare_importer(citycode='43100')
        importer.no_zip = True
        osm_files, zip_count = importer._discover_osm_files()
        assert osm_files == []
        assert zip_count == 0
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/test_plateau_importer2postgis.py::TestDiscoverOsmFiles -v`
Expected: FAIL — `AttributeError: 'PlateauImporter2PostGIS' object has no attribute 'no_zip'` (and `_discover_osm_files`).

- [ ] **Step 3: Add the `no_zip` attribute in `__init__`**

In `plateau_importer2postgis.py`, change the constructor signature and body:

```python
    def __init__(self,
                 data_dir="./plateau_data",
                 postgres_url="postgresql://osmfj_user:secure_plateau_password@localhost:5432/osmfj_plateau",
                 coord_bounds=None,
                 citycode=None,
                 no_zip=False):
```

and add, right after `self.coord_bounds = coord_bounds` (line 51):

```python
        self.no_zip = no_zip
```

- [ ] **Step 4: Add the `_discover_osm_files()` method**

Insert this method just above `find_zip_files` (before line 255):

```python
    def _discover_osm_files(self):
        """Return (osm_files, zip_count).

        In --no-zip mode, collect .osm directly (recursive glob) — a strict
        superset of both a flat and an ``extracted/<mesh>/`` layout — and report
        zero zips. Otherwise, run the existing zip discovery + extraction.
        """
        if self.no_zip:
            osm_files = sorted(self.data_dir.rglob("*.osm"))
            logger.info(f"📂 --no-zip: {len(osm_files)}個の.osmを直接検出: {self.data_dir}")
            return osm_files, 0

        zip_files = self.find_zip_files()
        if not zip_files:
            return [], 0
        return self.extract_zip_files(zip_files), len(zip_files)
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `pytest tests/test_plateau_importer2postgis.py::TestDiscoverOsmFiles -v`
Expected: PASS (both tests).

- [ ] **Step 6: Rewire `run_complete_import()` Phase 2/3 to use the seam**

Replace the current Phase 2 + Phase 3 blocks (lines 1306-1319):

```python
            # Phase 2: zipファイル確認
            logger.info("\n📁 Phase 2: zipファイル確認")
            zip_files = self.find_zip_files()
            if not zip_files:
                logger.error("❌ zipファイルが見つかりません")
                logger.info("💡 ヒント: データディレクトリにzipファイルを配置してください")
                return False

            # Phase 3: OSM抽出
            logger.info("\n📂 Phase 3: OSM展開・抽出")
            osm_files = self.extract_zip_files(zip_files)
            if not osm_files:
                logger.error("❌ OSMファイルが見つかりません")
                return False
```

with:

```python
            # Phase 2/3: OSMファイル収集 (zip 経由 or --no-zip 直読み)
            logger.info("\n📁 Phase 2/3: OSMファイル収集")
            osm_files, zip_count = self._discover_osm_files()
            if not osm_files:
                if self.no_zip:
                    logger.error("❌ .osmファイルが見つかりません (--no-zip モード)")
                    logger.info("💡 ヒント: data-dir 配下に変換済み .osm を配置してください")
                else:
                    logger.error("❌ zipファイルが見つかりません")
                    logger.info("💡 ヒント: データディレクトリにzipファイルを配置してください")
                return False
```

Then update the report call (line 1409) — replace `len(zip_files)` with `zip_count`:

```python
            self.create_import_report(
                start_analysis, zip_count, len(osm_files),
                total_buildings_count, total_nodes_count
            )
```

- [ ] **Step 7: Add the `--no-zip` CLI flag and thread it through**

In `main()`, after the `--verbose` argument (line 1447-1448) add:

```python
    parser.add_argument('--no-zip', action='store_true',
                       help='data-dir 配下の .osm を直接読む (zip 展開をスキップ)')
```

and change the constructor call (line 1461):

```python
    importer = PlateauImporter2PostGIS(args.data_dir, args.postgres_url, coord_bounds, args.citycode, args.no_zip)
```

- [ ] **Step 8: Run the importer test module to confirm no regression**

Run: `pytest tests/test_plateau_importer2postgis.py -v`
Expected: PASS (all existing tests + `TestDiscoverOsmFiles`).

- [ ] **Step 9: Commit**

```bash
git add plateau_importer2postgis.py tests/test_plateau_importer2postgis.py
git commit -m "feat(#21): add --no-zip importer mode (read .osm directly, skip zip)"
```

---

### Task 2: relative-path node key (`_file_key`) hardening

**Files:**
- Modify: `plateau_importer2postgis.py` — new `_file_key` method; per-file keying loop in `run_complete_import` (around 1374-1380)
- Test: `tests/test_plateau_importer2postgis.py`

**Interfaces:**
- Consumes: `bare_importer` fixture, `self.data_dir`.
- Produces: `PlateauImporter2PostGIS._file_key(osm_file) -> str` returning the path of `osm_file` relative to `self.data_dir` (falling back to the full path string if the file is outside `data_dir`). Used to namespace node keys and building node-refs.

- [ ] **Step 1: Write the failing test for `_file_key`**

Add to `tests/test_plateau_importer2postgis.py`:

```python
class TestFileKey:
    """`_file_key()` namespaces meshes by path, not basename, so two files
    that share a basename (adjacent cities share a mesh tile) do not collide."""

    def test_same_basename_different_subdir_distinct_keys(self, bare_importer):
        importer = bare_importer(citycode='43100')
        data_dir = Path(importer.data_dir)
        f1 = data_dir / 'extracted' / '53385729_a' / '53385729.osm'
        f2 = data_dir / 'extracted' / '53385729_b' / '53385729.osm'

        k1 = importer._file_key(f1)
        k2 = importer._file_key(f2)

        assert k1 != k2
        assert k1 == 'extracted/53385729_a/53385729.osm'
        assert k2 == 'extracted/53385729_b/53385729.osm'

    def test_file_outside_data_dir_falls_back_to_full_path(self, bare_importer):
        importer = bare_importer(citycode='43100')
        outside = Path('/somewhere/else/mesh.osm')
        assert importer._file_key(outside) == str(outside)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/test_plateau_importer2postgis.py::TestFileKey -v`
Expected: FAIL — `AttributeError: ... has no attribute '_file_key'`.

- [ ] **Step 3: Add the `_file_key()` method**

Insert just above `_discover_osm_files` (added in Task 1):

```python
    def _file_key(self, osm_file):
        """Namespace key for a mesh file, unique within data_dir.

        Uses the path relative to data_dir instead of the basename, so two
        files that share a basename in different subdirectories (adjacent
        cities share a mesh tile) do not collide in the in-memory node dict.
        A no-op for single-city dirs where mesh names are already unique.
        """
        try:
            return str(Path(osm_file).relative_to(self.data_dir))
        except ValueError:
            return str(osm_file)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/test_plateau_importer2postgis.py::TestFileKey -v`
Expected: PASS.

- [ ] **Step 5: Rewire the per-file keying loop to use `_file_key`**

In `run_complete_import`, replace the per-file keying block (lines 1374-1380):

```python
                    for original_id, node_data in nodes.items():
                        file_specific_key = f"{osm_file.name}:{original_id}"
                        all_nodes[file_specific_key] = node_data

                    for building in buildings:
                        building['node_refs'] = [f"{osm_file.name}:{ref}" for ref in building['node_refs']]
                        all_buildings.append(building)
```

with:

```python
                    key_base = self._file_key(osm_file)
                    for original_id, node_data in nodes.items():
                        file_specific_key = f"{key_base}:{original_id}"
                        all_nodes[file_specific_key] = node_data

                    for building in buildings:
                        building['node_refs'] = [f"{key_base}:{ref}" for ref in building['node_refs']]
                        all_buildings.append(building)
```

- [ ] **Step 6: Audit for other uses of the basename key format**

Run: `grep -n "osm_file.name\|file_specific_key\|node_refs" plateau_importer2postgis.py`
Expected: the only `f"...:{...}"` key construction sites are the two lines just edited; `node_refs` are consumed only by `process_buildings_safe` via matching keys (format-opaque). Confirm no code parses the key back into a basename. If any other construction site exists, update it to use `key_base` too.

- [ ] **Step 7: Run the importer test module to confirm no regression**

Run: `pytest tests/test_plateau_importer2postgis.py -v`
Expected: PASS (all tests, incl. `TestFileKey` and `TestDiscoverOsmFiles`).

- [ ] **Step 8: Commit**

```bash
git add plateau_importer2postgis.py tests/test_plateau_importer2postgis.py
git commit -m "fix(#21): key import nodes by data_dir-relative path, not basename"
```

---

### Task 3: end-to-end `--no-zip` verification (manual smoke)

**Files:** none committed. This task validates the wired-up CLI end-to-end. It is
manual because `run_complete_import()` touches the database in Phase 1 (before
discovery), so an automated end-to-end test needs the opt-in integration DB and
PostGIS — out of proportion to this change, and covered at the unit level by
Tasks 1–2.

- [ ] **Step 1: Run the full test suite**

Run: `pytest tests/`
Expected: PASS (integration tests skip without `PLATEAU_TEST_DATABASE_URL`).

- [ ] **Step 2: Build a scratch no-zip data dir from the fixtures**

```bash
SCRATCH=$(mktemp -d)/43100
mkdir -p "$SCRATCH/extracted/53385729" "$SCRATCH/extracted/53385730"
cp tests/fixtures/citygml-osm/v4_outline_only.osm "$SCRATCH/extracted/53385729/53385729.osm"
cp tests/fixtures/citygml-osm/v5_outline_with_part.osm "$SCRATCH/extracted/53385730/53385730.osm"
```

- [ ] **Step 3: Dry-check discovery via a one-liner (no DB)**

```bash
python -c "from plateau_importer2postgis import PlateauImporter2PostGIS as I; imp=I(data_dir='$SCRATCH', postgres_url='fake', citycode='43100', no_zip=True); imp._test_connection=lambda: None; print(sorted(p.name for p in imp._discover_osm_files()[0]))"
```
Expected: prints `['53385729.osm', '53385730.osm']` — confirming the CLI path discovers both meshes with no zips present.

- [ ] **Step 4 (optional, against a throwaway local DB): full import smoke**

With a disposable local PostGIS DB URL in `$DATABASE_URL`, run:

```bash
python plateau_importer2postgis.py --data-dir "$SCRATCH" --no-zip --citycode 43100 --postgres-url "$DATABASE_URL"
```
Expected: completes, log shows `📂 --no-zip: 2個の.osmを直接検出` and a non-zero building insert count. (Skip if no disposable DB is available; Tasks 1–2 cover the new logic.)

- [ ] **Step 5: Clean up**

```bash
rm -rf "$(dirname "$SCRATCH")"
```

---

## Self-Review

- **Spec coverage:** Deliverable A (`--no-zip`) → Task 1. Deliverable B
  (relative-path key) → Task 2. Spec "Testing" unit items → Tasks 1–2 tests;
  the named integration/parity test → Task 3 manual smoke (rationale: Phase-1 DB
  coupling makes an automated E2E disproportionate). Deliverable C (Phase 1
  wrapper) is explicitly out of this plan's scope (migration-dependent stages
  deferred per the spec).
- **Placeholder scan:** none — every code step shows complete code; the only
  "optional" step (Task 3 Step 4) is a genuine environment-gated smoke, not a
  deferred requirement.
- **Type consistency:** `no_zip: bool` / `self.no_zip`, `_discover_osm_files() ->
  (list, int)`, `_file_key() -> str`, and `key_base`/`zip_count` names are used
  identically across tasks.
