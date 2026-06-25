# Design: API output dedup for cross-city duplicate buildings (#31)

Date: 2026-06-24
Issue: #31
Related: #29 (large-part filter), #30 (ref:MLIT_PLATEAU DB column)

## Problem

When PLATEAU CityGML data from two adjacent municipalities both include the same building (boundary overlap, mesh-share at administrative borders, etc.), the building is imported into the DB twice with different `city_code` and `osm_id`. A bbox query against `/api/mapwithai/buildings` then returns the same building twice.

Scale (measured 2026-06-23, by `(centroid, height)` equality):
- ~520k distinct buildings affected
- ~1.14M rows total
- ~9% of all outline rows in the DB (12.66M)

Existing mitigation (`city_boundary_filter` CTE, introduced for Rapid #35) only catches buildings whose centroid is outside the source city's N03 administrative boundary. The 9% leak through the gap because:
- `dash_city_master` has no row for some `city_code` (LEFT JOIN passes them)
- `boundary_geom IS NULL` for special datasets (13999 / 27999)
- N03 boundaries near city borders contain centroids of buildings claimed by both adjacent cities

## Goals

- Same building never appears twice in a single bbox response
- Deterministic: same bbox query returns the same `osm_id` across requests (so Rapid frontend conflation does not double-suggest)
- Pick the geographically correct city when possible (the one whose N03 boundary contains the building centroid)
- Defense in depth: do not replace `city_boundary_filter`; layer dedup on top

## Non-goals

- Importer-side dedup (separate medium-term work; will take effect on the next re-import)
- DB cleanup migration (separate medium-term work; the output guard makes this non-urgent)
- Filling in missing `boundary_geom` rows for cities without N03 data (separate issue)

## Design

### Dedup key

Four-tuple identifying "same building":

```
(round(centroid_x, 6), round(centroid_y, 6),
 height, building_levels, building_part)
```

- Coordinates rounded to 6 decimals (~10cm precision) to absorb floating-point noise
- `NULL`s in any field are collapsed by `COALESCE(... ::text, '')` — only matched against other `NULL`s in the same field (safe side: differing NULL/non-NULL stays separate)
- Including `building_part` ensures an outline (`NULL`) and a same-footprint part (`'yes'`) are not collapsed together

### Tiebreaker

When two rows share the dedup key, keep the row chosen by this ordered priority:

1. **N03 boundary priority**: the row whose `city_code` has `boundary_geom` containing the building centroid. Same data source (`dash_city_master`) as the existing input filter — keeps the principle consistent across input and output.
2. **Smallest `city_code`** as deterministic fallback. Covers three edge cases:
   - Both cities' N03 boundaries contain the centroid
   - Neither does
   - Either city has no `dash_city_master` row or `boundary_geom IS NULL`

This yields stable `osm_id` selection across requests.

### Implementation layer: SQL CTE

`bbox_outlines` CTE in `PlateauAPI.get_buildings_in_bbox`: replace the current `DISTINCT ON ({distinct_key})` with `DISTINCT ON (...4-tuple...)`, and extend `ORDER BY` with the N03-contained / `city_code` tiebreaker.

```sql
WITH bbox_outlines AS (
    SELECT DISTINCT ON (
        ROUND(ST_X(b.centroid)::numeric, 6),
        ROUND(ST_Y(b.centroid)::numeric, 6),
        COALESCE(b.height::text, ''),
        COALESCE(b.building_levels::text, ''),
        COALESCE(b.building_part, '')
    )
        b.id, b.osm_id, ...
    FROM plateau_buildings b
    WHERE {spatial_condition}
      AND b.building_part IS NULL
      {city_boundary_filter}
    ORDER BY
        ROUND(ST_X(b.centroid)::numeric, 6),
        ROUND(ST_Y(b.centroid)::numeric, 6),
        COALESCE(b.height::text, ''),
        COALESCE(b.building_levels::text, ''),
        COALESCE(b.building_part, ''),
        (CASE WHEN EXISTS (
            SELECT 1 FROM dash_city_master m
            WHERE m.city_code = b.city_code
              AND m.boundary_geom IS NOT NULL
              AND ST_Contains(m.boundary_geom, b.centroid)
        ) THEN 0 ELSE 1 END),
        b.city_code
    LIMIT %s
)
```

The `orphan_parts` CTE gets the same `DISTINCT ON` and `ORDER BY` treatment. `related_parts` does not need its own dedup: its `WHERE b.parent_building_id IN (SELECT id FROM bbox_outlines)` automatically rides along with the deduped outline survivor.

Rationale for SQL over Python:
- Dedup happens **before** `LIMIT`, so the page is not silently shortened
- Keeps dedup logic next to the existing input filter — single place to read for maintenance
- The same N03 lookup is already cached in `dash_city_master`; no extra round-trip
- Trade-off accepted: tests need real Postgres (`fresh_plateau_schema` fixture from #23)

### Observability

`logger.info` records both the raw outline-candidate count and a deduped
figure per request. The pre-dedup count comes from `COUNT(*) OVER ()` in the
CTE — single-query, no extra round-trip.

Normal path (LIMIT did not fire — `raw_count <= limit`):

```
検索結果: {len(result)}件 (bbox: ..., outline_candidates: {raw_count}, deduped: {N}件)
```

LIMIT path (`raw_count > limit`; post-truncation dedup state is unknowable
without re-querying):

```
検索結果: {len(result)}件 (bbox: ..., outline_candidates: {raw_count}, limit_hit: true (limit={L}))
```

`deduped` is computed against the outline rows in the result (excluding
related/orphan parts), so a mixed-content bbox does not under-count. The
LIMIT-hit branch keeps the operator from chasing a misleading "deduped: 4000"
when 4000 is really LIMIT truncation, not city dedup.

## Edge cases

| Case | Behavior |
|---|---|
| `city_code` not in `dash_city_master` | EXISTS returns false → smallest `city_code` wins |
| `boundary_geom IS NULL` (e.g. 13999, 27999) | EXISTS returns false → smallest `city_code` wins |
| `centroid IS NULL` | Importer maintains non-null; if present, collapses to one group |
| `height` NULL on both sides | Both `''` after COALESCE — same group |
| `height` NULL on one side, set on other | Different groups (safe — never silently drops a distinct-height row) |
| Outline + same-footprint orphan part with same centroid | Different `building_part` value → different groups (preserved) |

## Testing

Integration tests using `PLATEAU_TEST_DATABASE_URL` + `fresh_plateau_schema` fixture introduced in #23 / #26. CI continues to skip integration tests unless `--run-integration` is set; local and manual runs cover the change.

| Test | Asserts |
|---|---|
| `test_dedup_picks_n03_contained_city` | Two cities seeded with the same building; only city A's N03 contains the centroid → A's `osm_id` returned |
| `test_dedup_picks_smallest_city_code_when_neither_n03_contains` | Both cities lack `boundary_geom` → smallest `city_code` returned |
| `test_dedup_picks_smallest_city_code_when_both_n03_contain` | Boundary-overlap edge case → smallest `city_code` returned |
| `test_height_difference_preserves_both` | Same centroid, different `height` → both rows returned (no false collapse) |
| `test_orphan_parts_dedup` | Orphan part cross-city duplicate is also deduped |
| `test_related_parts_follow_surviving_outline` | When outline A wins over outline B, only A's children parts are returned |

## Rollout

- Single file change (`osmfj_plateau_api.py`), no migration, no API response shape change (counts only decrease)
- Rollback: `git revert` — stateless
- Validation: deploy, exercise an affected bbox (e.g. the example from the issue near 35.689363, 139.485579), confirm a single record and check the `deduped:` log

## Out of scope

- Importer-side dedup (medium-term issue; complements this output guard)
- DB cleanup migration (medium-term issue; output guard removes urgency)
- Backfilling `boundary_geom` for cities without N03 data (separate issue)
- Feature flag / URL parameter to disable dedup (not needed; revert is fast)
