"""
plateau_migrate.py のテスト
"""

from unittest.mock import MagicMock

import pytest

import plateau_migrate
from plateau_migrate import (
    ADVISORY_LOCK_ID,
    BATCH_SIZE,
    CITY_CODE_PATTERN,
    MIN_DISK_FREE_GB,
    ROW_SIZE_BYTES,
    Migrator,
    confirm_execute,
)


# ----------------------------------------------------------------------
# 定数
# ----------------------------------------------------------------------

class TestConstants:
    def test_advisory_lock_id_is_distinct_from_purge(self):
        """マイグレーション用ロックIDは purge と別であること（同時実行可能にするため）"""
        import plateau_purge
        assert ADVISORY_LOCK_ID != plateau_purge.ADVISORY_LOCK_ID

    def test_batch_size_positive(self):
        assert BATCH_SIZE > 0

    def test_min_disk_free_gb_positive(self):
        assert MIN_DISK_FREE_GB > 0

    def test_row_size_bytes_realistic(self):
        """1行あたり数百バイト〜数KBの想定"""
        assert 100 <= ROW_SIZE_BYTES <= 10000


# ----------------------------------------------------------------------
# CITY_CODE_PATTERN
# ----------------------------------------------------------------------

class TestCityCodePattern:
    def test_extracts_5_digit_code(self):
        m = CITY_CODE_PATTERN.search('plateau_13112_53393438_bldg_6697_op.osm')
        assert m is not None
        assert m.group(1) == '13112'

    def test_does_not_match_invalid_format(self):
        assert CITY_CODE_PATTERN.search('random_text') is None
        assert CITY_CODE_PATTERN.search('plateau_abc_xyz') is None

    def test_matches_various_real_examples(self):
        cases = [
            ('plateau_03201_59413067_bldg_6697_op.osm', '03201'),  # 盛岡市
            ('plateau_14100_67890_bldg_6697_op.osm', '14100'),     # 横浜市
            ('plateau_40130_99999_bldg_6697_op.osm', '40130'),     # 福岡市
        ]
        for source_dataset, expected in cases:
            assert CITY_CODE_PATTERN.search(source_dataset).group(1) == expected


# ----------------------------------------------------------------------
# Migrator メソッド
# ----------------------------------------------------------------------

class TestMigrator:
    def test_postgres_url_from_env(self, monkeypatch):
        monkeypatch.setenv('DATABASE_URL', 'postgresql://env')
        m = Migrator()
        assert m.postgres_url == 'postgresql://env'

    def test_check_column_exists_true(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = ('city_code',)
        m = Migrator('postgresql://x')
        m.conn = mock_connection
        assert m.check_column_exists() is True

    def test_check_column_exists_false(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = None
        m = Migrator('postgresql://x')
        m.conn = mock_connection
        assert m.check_column_exists() is False

    def test_check_index_exists(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = ('idx_buildings_city_code',)
        m = Migrator('postgresql://x')
        m.conn = mock_connection
        assert m.check_index_exists() is True

    def test_check_not_null_constraint(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = ('NO',)  # not nullable
        m = Migrator('postgresql://x')
        m.conn = mock_connection
        assert m.check_not_null_constraint() is True

    def test_check_not_null_constraint_when_nullable(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = ('YES',)
        m = Migrator('postgresql://x')
        m.conn = mock_connection
        assert m.check_not_null_constraint() is False

    def test_count_total_rows(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = (12761402,)
        m = Migrator('postgresql://x')
        m.conn = mock_connection
        assert m.count_total_rows() == 12761402

    def test_count_unmigrated(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = (1000,)
        m = Migrator('postgresql://x')
        m.conn = mock_connection
        assert m.count_unmigrated() == 1000

    def test_get_id_range(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = (1, 23231099)
        m = Migrator('postgresql://x')
        m.conn = mock_connection
        assert m.get_id_range() == (1, 23231099)

    def test_get_id_range_when_empty(self, mock_connection):
        """空テーブルでは (0, 0) を返す"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = (None, None)
        m = Migrator('postgresql://x')
        m.conn = mock_connection
        assert m.get_id_range() == (0, 0)


# ----------------------------------------------------------------------
# analyze_extraction
# ----------------------------------------------------------------------

class TestAnalyzeExtraction:
    def test_all_extractable(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.side_effect = [
            (12761402,),  # extractable
            (0,),         # null_count
            (0,),         # failed
        ]
        cursor.fetchall.return_value = []  # failed_samples

        m = Migrator('postgresql://x')
        m.conn = mock_connection
        result = m.analyze_extraction()

        assert result['extractable'] == 12761402
        assert result['null_count'] == 0
        assert result['failed_count'] == 0
        assert result['failed_samples'] == []

    def test_with_failures(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.side_effect = [
            (1000000,),   # extractable
            (10,),        # null
            (5,),         # failed
        ]
        cursor.fetchall.return_value = [('bad_format_1.osm', 3), ('bad_format_2.osm', 2)]

        m = Migrator('postgresql://x')
        m.conn = mock_connection
        result = m.analyze_extraction()

        assert result['failed_count'] == 5
        assert result['null_count'] == 10
        assert len(result['failed_samples']) == 2
        assert result['failed_samples'][0]['source_dataset'] == 'bad_format_1.osm'


# ----------------------------------------------------------------------
# city_code_distribution
# ----------------------------------------------------------------------

class TestCityCodeDistribution:
    def test_returns_sorted_list(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchall.return_value = [
            ('14100', 868984),
            ('23100', 678887),
            ('11100', 454522),
        ]

        m = Migrator('postgresql://x')
        m.conn = mock_connection
        result = m.city_code_distribution()

        assert len(result) == 3
        assert result[0]['city_code'] == '14100'
        assert result[0]['row_count'] == 868984


# ----------------------------------------------------------------------
# compare_with_cities_2024
# ----------------------------------------------------------------------

class TestCompareWithCities2024:
    def test_no_unexpected_no_missing(self):
        m = Migrator('postgresql://x')
        # CITIES_2024 + ALREADY_IMPORTED の和集合を完全に再現
        from batch_import_2024 import CITIES_2024, ALREADY_IMPORTED
        expected = sorted(set(CITIES_2024) | set(ALREADY_IMPORTED))

        distribution = [{'city_code': code, 'row_count': 100} for code in expected]
        result = m.compare_with_cities_2024(distribution)

        assert result['unexpected'] == []
        assert result['missing'] == []
        assert result['expected_count'] == len(expected)
        assert result['found_count'] == len(expected)

    def test_detects_unexpected_cities(self):
        m = Migrator('postgresql://x')
        distribution = [{'city_code': '99999', 'row_count': 100}]
        result = m.compare_with_cities_2024(distribution)
        assert '99999' in result['unexpected']

    def test_detects_missing_cities(self):
        """期待都市がDBにない場合、missing に含まれる"""
        m = Migrator('postgresql://x')
        distribution = []  # 何も検出されない
        result = m.compare_with_cities_2024(distribution)
        assert len(result['missing']) > 0  # CITIES_2024 が空でなければ


# ----------------------------------------------------------------------
# estimate_migration
# ----------------------------------------------------------------------

class TestEstimateMigration:
    def test_batch_safe_when_disk_sufficient(self):
        m = Migrator('postgresql://x')
        result = m.estimate_migration(
            total_rows=12_761_402,
            extractable=12_761_402,
            free_gb=15.0,
        )
        # バッチ方式は安全（ピーク ~1.2GB << 15GB）
        assert result['safe_batch'] is True

    def test_full_unsafe_when_disk_tight(self):
        """ディスクが10GB未満なら一括方式は危険"""
        m = Migrator('postgresql://x')
        result = m.estimate_migration(
            total_rows=12_761_402,
            extractable=12_761_402,
            free_gb=10.0,
        )
        assert result['safe_full'] is False
        # バッチ方式はまだ安全（10GB > 1.19GB + 1.0GB マージン）
        assert result['safe_batch'] is True

    def test_batch_count_matches_extractable(self):
        m = Migrator('postgresql://x')
        result = m.estimate_migration(
            total_rows=2_500_000,
            extractable=2_500_000,
            free_gb=20.0,
        )
        # 100万行ずつ → 3バッチ
        assert result['batch_count'] == 3


# ----------------------------------------------------------------------
# confirm_execute
# ----------------------------------------------------------------------

class TestConfirmExecute:
    def test_yes_approves(self, monkeypatch):
        monkeypatch.setattr('builtins.input', lambda prompt='': 'yes')
        assert confirm_execute() is True

    def test_y_approves(self, monkeypatch):
        monkeypatch.setattr('builtins.input', lambda prompt='': 'y')
        assert confirm_execute() is True

    def test_no_rejects(self, monkeypatch):
        monkeypatch.setattr('builtins.input', lambda prompt='': 'no')
        assert confirm_execute() is False

    def test_empty_rejects(self, monkeypatch):
        monkeypatch.setattr('builtins.input', lambda prompt='': '')
        assert confirm_execute() is False
