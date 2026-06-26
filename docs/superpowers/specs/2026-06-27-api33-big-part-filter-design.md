# Design: Filter clean Lod1Solid-derived duplicate parts at API output (#33)

Date: 2026-06-27
Issue: #33
Related: #29 (parent issue, hold), #30 (ref:MLIT_PLATEAU DB column), #31 (cross-city dedup, merged)

## Problem

When PLATEAU CityGML is converted by `citygml-osm`, the Lod1Solid extrusion
(an "overall extruded box" of the building) is sometimes emitted as a separate
`building:part` way alongside the Lod0 footprint outline. The two ways
represent the same physical structure, and the OSM Simple 3D buildings
convention says 3D volumes that share faces should be avoided. The redundant
part appears in API output and would be uploaded as a duplicate object.

Scale, measured against the production DB (12.66M outlines) on 2026-06-26
using the threshold `area_ratio > 0.5 AND height_diff < 0.5 AND levels match`:

- 235,999 outlines have at least one big-part candidate
- 220,594 of those (93.5%) have exactly one such candidate per relation
- 179,641 of the n=1 cases (81%) have an outline-part centroid distance < 3m
  — the typical Lod1Solid signature

Investigation for #29 (predecessor issue) discovered that the broad
threshold mixes two phenomena: clean Lod1Solid duplicates (close centroid,
similar shape) and wing-style structures (large centroid offset, large
Hausdorff distance). The flagship example referenced in #29
(relation 20994592 in Fuchu City) is the latter:
outline 3221m² vs big-part 2251m², ratio 0.699, but centroid distance 30.71m
and Hausdorff distance 53.9m — not a Lod1Solid signature.

This issue addresses only the clean Lod1Solid case. The wing-style case
needs a different fix and is out of scope (handled in #29).

## Goals

- Drop the Lod1Solid duplicate part when the geometric signature is clear:
  close centroid, similar shape, matching height/levels, and the building's
  only big-part candidate
- Never drop a structurally meaningful part — multi-wing buildings,
  offset wings, and height-mismatched parts must pass through
- Be deterministic and reproducible across requests
- Reuse the threshold definitions from importer-side work that will come
  later (centralize constants so a single change updates both layers)

## Non-goals

- Wing-style big-parts (centroid offset, Hausdorff large) — covered by #29
- Importer-side filter at import time — separate issue; this work
  centralizes the threshold constants so the importer can reuse them
- DB cleanup migration to remove redundant rows — output guard makes it
  non-urgent
- `orphan_parts` (parts without a parent outline in the same query) — no
  outline to compare against; needs a different signal
- Upstream `citygml-osm` patch — separate effort

## Design

### Filter target

The `related_parts` CTE inside `get_buildings_in_bbox`. A row is dropped
when it satisfies all of the predicates below.

### Filter predicates

A part row `b` joined with its parent outline `o` is filtered when:

1. `b.building_part = 'yes'`
2. Both `b.height` and `o.height` are non-null
3. `ABS(b.height - o.height) < 0.5` (meters)
4. `b.building_levels = o.building_levels`
5. `ST_Area(b.geom::geography) > 0.5 * ST_Area(o.geom::geography)`
6. `ST_Distance(b.centroid::geography, o.centroid::geography) < 5` (meters)
7. `ST_HausdorffDistance(b.geom, o.geom) * 111000 < 10` (meters approx).
   The `* 111000` converts degrees-to-meters at the equator; for Japanese
   latitudes (~35°) the effective threshold ranges roughly 7-13m due to
   the longitude scale shrinking with latitude. Coarse on purpose —
   `ST_HausdorffDistance` does not accept geography arguments, and the
   wing-style cases we want to keep have distances >> 10m anyway
8. The big-part count in the same relation is exactly 1
   (where "big-part" satisfies predicates 1-5 — predicates 6-7 are
   evaluated at the filter step only, not in the count)

Predicates 6 and 7 are the discriminators between Lod1Solid duplicates
and wing-style structures. Predicate 8 prevents accidental filtering of
multi-wing buildings.

### Threshold constants

Defined once at module scope so importer-side filter (future issue) can
import the same values:

```python
# #33 thresholds — keep in sync with importer-side filter when added.
BIG_PART_HEIGHT_DIFF_M = 0.5
BIG_PART_AREA_RATIO_MIN = 0.5
BIG_PART_CENTROID_DIST_M = 5.0
BIG_PART_HAUSDORFF_M = 10.0
```

### Implementation: pre-aggregate CTE (option A)

A new `big_part_counts` CTE pre-counts the predicates 1-5 candidates per
outline. The `related_parts` CTE LEFT JOINs the count and checks
predicates 6-8 inline.

```sql
big_part_counts AS (
    SELECT o.id AS outline_id, COUNT(*) AS n_big
    FROM bbox_outlines o
    JOIN plateau_buildings p ON p.parent_building_id = o.id
    WHERE {big_part_match_predicate}    -- predicates 1-5 only
    GROUP BY o.id
),
related_parts AS (
    SELECT b.id, b.osm_id, ..., b.parent_building_id, 0 AS pre_dedup_count
    FROM plateau_buildings b
    JOIN bbox_outlines o ON b.parent_building_id = o.id
    LEFT JOIN big_part_counts bpc ON bpc.outline_id = o.id
    WHERE NOT (
        -- Predicates 1-5 inline (DRY with the count CTE via Python f-string)
        b.building_part = 'yes'
        AND b.height IS NOT NULL AND o.height IS NOT NULL
        AND ABS(b.height - o.height) < {BIG_PART_HEIGHT_DIFF_M}
        AND b.building_levels = o.building_levels
        AND ST_Area(b.geom::geography) > {BIG_PART_AREA_RATIO_MIN} * ST_Area(o.geom::geography)
        -- Predicates 6-7 (Lod1Solid signature discriminators)
        AND ST_Distance(b.centroid::geography, o.centroid::geography) < {BIG_PART_CENTROID_DIST_M}
        AND ST_HausdorffDistance(b.geom, o.geom) * 111000 < {BIG_PART_HAUSDORFF_M}
        -- Predicate 8 (multi-wing guard)
        AND COALESCE(bpc.n_big, 0) = 1
    )
),
```

Rationale for option A over a per-row correlated subquery:

- Pre-aggregation runs once per outline (vs once per related part)
- "Group by then filter" is the textbook SQL idiom for cardinality-based
  filtering; the next person reading the code recognizes the pattern
- The aggregate result can be reused for the observability log without a
  second query
- The same Python predicate string can be lifted into the importer-side
  filter if/when that work happens

### Observability

Extend the existing log line with a new field:

```
検索結果: {len(result)}件 (bbox: ..., outline_candidates: N, deduped: M件, big_parts_filtered: K件)
```

`big_parts_filtered` = the number of outlines whose `n_big = 1` in
`big_part_counts` (these are the outlines where exactly one part was
dropped). The value is carried out of the SQL via an extra column on
`bbox_outlines`:

```sql
bbox_outlines AS (
    SELECT DISTINCT ON (...) ..., COUNT(*) OVER () AS pre_dedup_count,
        (SELECT COUNT(*) FROM big_part_counts WHERE n_big = 1) AS big_parts_filtered_count
    FROM ...
)
```

Python reads `big_parts_filtered_count` from any returned row (same
trick as `pre_dedup_count`).

The LIMIT-hit branch (`raw_count > limit`) reports
`limit_hit: true (limit=L)` as before — `big_parts_filtered` is omitted
in that branch because the underlying outline set is truncated.

## Edge cases

| Case | Behavior |
|---|---|
| outline `area = 0` (data error) | ratio condition false → not filtered |
| `height NULL` on either side | predicate false → not filtered |
| `building_levels` mismatch | not filtered |
| `n_big_parts >= 2` in the relation | not filtered (multi-wing guard) |
| centroid distance >= 5m | not filtered (wing-style suspected) |
| Hausdorff distance >= 10m | not filtered (shape diverges) |
| orphan part (no parent in bbox) | unchanged behavior (handled by #31 dedup only) |
| Big-part survives outside this bbox but is filtered in this bbox | acceptable — the filter is per-request, deterministic for any given bbox |

## Testing

Integration tests added to `tests/test_dedup_city_duplicates.py`. Use the
existing fixtures (`fresh_plateau_full_schema`, `plateau_api_class`) and
helpers (`_seed_building`, `_square_wkt`). The helpers can produce
outline + offset-part pairs by varying `lat` / `lon` / `size_deg`.

| Test | Asserts |
|---|---|
| `test_big_part_filter_drops_clean_lod1solid` | outline + single big-part centered on outline → big-part absent from result |
| `test_big_part_filter_keeps_wing_style` | outline + big-part offset by ~30m → both present |
| `test_big_part_filter_keeps_multi_wing` | outline + two big-parts (both n_big_parts=2) → all three present |
| `test_big_part_filter_keeps_when_height_differs` | outline (h=22) + big-part (h=23) at same position → both present |
| `test_big_part_filter_keeps_when_levels_differ` | outline (lv=5) + big-part (lv=4) → both present |
| `test_big_part_filter_logs_filtered_count` | caplog asserts `big_parts_filtered: 1件` in the info log |

CI continues to skip integration tests unless `--run-integration` is set.

## Rollout

- Single production file change (`osmfj_plateau_api.py`); no migration; no
  API response shape change (returned ways decrease by ~180k system-wide)
- Threshold constants placed at module top so future importer-side filter
  imports them
- Rollback: `git revert` — stateless
- Validation: deploy, query a bbox containing a known clean-Lod1Solid case,
  confirm the redundant part is absent and `big_parts_filtered` shows in
  the log
- Post-deploy observation: monitor for user reports of missing parts. If
  any reports arrive, examine the centroid distance / Hausdorff distance
  of the dropped part and adjust thresholds if needed

## Closing #29

When this issue (#33) closes, the parent issue #29 closes as well — the
parent's broad "filter big-parts" scope is satisfied by this targeted
filter plus the explicit out-of-scope notes for the wing-style cases.
