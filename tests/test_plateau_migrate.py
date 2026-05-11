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
    """
    CITY_CODE_PATTERN は `plateau_(\\d{5})_` で抽出する。
    境界・負例を網羅し、誤抽出を防ぐ。
    """

    @pytest.mark.parametrize('source,expected', [
        # 正例: 実データ各種
        ('plateau_03201_59413067_bldg_6697_op.osm', '03201'),  # 盛岡市
        ('plateau_14100_67890_bldg_6697_op.osm', '14100'),     # 横浜市
        ('plateau_40130_99999_bldg_6697_op.osm', '40130'),     # 福岡市
        ('plateau_13112_53393438_bldg_6697_op.osm', '13112'),  # 世田谷区
        # 各桁の境界値
        ('plateau_00000_x', '00000'),
        ('plateau_99999_x', '99999'),
        # 周辺にゴミ
        ('prefix_plateau_12345_suffix', '12345'),
        ('  plateau_12345_  ', '12345'),
    ])
    def test_positive_matches(self, source, expected):
        m = CITY_CODE_PATTERN.search(source)
        assert m is not None, f'Should match: {source!r}'
        assert m.group(1) == expected

    @pytest.mark.parametrize('source', [
        # 桁数違い
        'plateau_1234_x',          # 4桁
        'plateau_123456_x',        # 6桁
        'plateau_1_x',             # 1桁
        # 数字以外
        'plateau_ABCDE_x',
        'plateau_1234a_x',
        'plateau_a1234_x',
        # 末尾アンダースコアなし
        'plateau_12345',
        'plateau_12345.osm',
        # 接頭辞なし
        '12345_data.osm',
        'PLATEAU_12345_x',         # 大文字違い
        # 空・None系
        '',
        '_____',
        'plateau__12345_x',        # アンダースコア2連で plateau_ の直後
    ])
    def test_negative_matches(self, source):
        """マッチしないべきパターン"""
        m = CITY_CODE_PATTERN.search(source)
        assert m is None, f'Should NOT match: {source!r}'

    def test_extracts_first_occurrence_only(self):
        """複数候補があれば最初の1つを返す"""
        m = CITY_CODE_PATTERN.search('plateau_11111_xxx_plateau_22222_yyy')
        assert m.group(1) == '11111'

    def test_rejects_fullwidth_digits(self):
        """全角数字 (０-９) はマッチしない（re.ASCII フラグで保護）"""
        m = CITY_CODE_PATTERN.search('plateau_１２３４５_x')
        assert m is None

    def test_rejects_arabic_indic_digits(self):
        """アラビア数字以外の数字（例: アラビア・インド数字）もマッチしない"""
        m = CITY_CODE_PATTERN.search('plateau_٠١٢٣٤_x')
        assert m is None


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
    """
    `compare_with_cities_2024` のセマンティクス検証。
    実装と同じ集合演算で answer を作ると tautology になるため、
    既知の小さなケースで挙動を固定する。
    """

    def test_unexpected_contains_only_extras_not_missing(self, monkeypatch):
        """unexpected と missing の方向を明示的に検証（取り違え検知用）"""
        # 期待値を意図的にハードコードして注入
        monkeypatch.setattr(plateau_migrate, 'CITIES_2024', ['11111', '22222', '33333'])
        monkeypatch.setattr(plateau_migrate, 'ALREADY_IMPORTED', {'44444'})

        m = Migrator('postgresql://x')
        # DB には 22222, 33333, 44444, 55555 がある
        # → unexpected: {55555}（リスト外）
        # → missing: {11111}（リストにあるがDBにない）
        distribution = [
            {'city_code': '22222', 'row_count': 1},
            {'city_code': '33333', 'row_count': 1},
            {'city_code': '44444', 'row_count': 1},
            {'city_code': '55555', 'row_count': 1},
        ]
        result = m.compare_with_cities_2024(distribution)

        assert result['unexpected'] == ['55555']
        assert result['missing'] == ['11111']

    def test_empty_distribution_makes_all_expected_missing(self, monkeypatch):
        """distribution が空 → 全期待都市が missing、unexpected は空"""
        monkeypatch.setattr(plateau_migrate, 'CITIES_2024', ['11111', '22222'])
        monkeypatch.setattr(plateau_migrate, 'ALREADY_IMPORTED', set())

        m = Migrator('postgresql://x')
        result = m.compare_with_cities_2024([])
        assert sorted(result['missing']) == ['11111', '22222']
        assert result['unexpected'] == []

    def test_returns_sorted_lists(self, monkeypatch):
        """unexpected と missing はソートされた順序で返る（出力安定性）"""
        monkeypatch.setattr(plateau_migrate, 'CITIES_2024', ['33333', '11111', '22222'])
        monkeypatch.setattr(plateau_migrate, 'ALREADY_IMPORTED', set())

        m = Migrator('postgresql://x')
        distribution = [
            {'city_code': '99999', 'row_count': 1},
            {'city_code': '88888', 'row_count': 1},
        ]
        result = m.compare_with_cities_2024(distribution)
        assert result['unexpected'] == sorted(result['unexpected'])
        assert result['missing'] == sorted(result['missing'])

    def test_count_fields_are_accurate(self, monkeypatch):
        monkeypatch.setattr(plateau_migrate, 'CITIES_2024', ['11111', '22222'])
        monkeypatch.setattr(plateau_migrate, 'ALREADY_IMPORTED', {'33333'})

        m = Migrator('postgresql://x')
        distribution = [
            {'city_code': '11111', 'row_count': 1},
            {'city_code': '99999', 'row_count': 1},
        ]
        result = m.compare_with_cities_2024(distribution)
        assert result['expected_count'] == 3  # 11111, 22222, 33333
        assert result['found_count'] == 2     # 11111, 99999

    def test_skipped_when_cities_2024_unavailable(self, monkeypatch):
        """CITIES_2024 が空（import失敗想定）なら skipped を返す"""
        monkeypatch.setattr(plateau_migrate, 'CITIES_2024', [])
        m = Migrator('postgresql://x')
        result = m.compare_with_cities_2024([{'city_code': '12345', 'row_count': 1}])
        assert result.get('skipped') is True


# ----------------------------------------------------------------------
# estimate_migration
# ----------------------------------------------------------------------

class TestEstimateMigration:
    """
    安全性判定の境界値を狙う。
    ミューテーション `>` vs `>=` などを検知できるように、
    判定境界の前後 ±0.01GB をテストする。
    """

    def test_batch_safe_when_disk_sufficient(self):
        m = Migrator('postgresql://x')
        result = m.estimate_migration(
            total_rows=12_761_402,
            extractable=12_761_402,
            free_gb=15.0,
        )
        assert result['safe_batch'] is True

    def test_full_unsafe_when_disk_tight(self):
        m = Migrator('postgresql://x')
        result = m.estimate_migration(
            total_rows=12_761_402,
            extractable=12_761_402,
            free_gb=10.0,
        )
        assert result['safe_full'] is False
        assert result['safe_batch'] is True

    @pytest.mark.parametrize('extractable,expected_batches', [
        # 100万行ずつのバッチサイズ
        (1, 1),                  # 最小: 1行 → 1バッチ
        (1_000_000, 1),          # ちょうど1バッチ
        (1_000_001, 2),          # +1 で2バッチ
        (2_000_000, 2),          # ちょうど2バッチ
        (2_000_001, 3),          # +1 で3バッチ
        (12_500_000, 13),        # 13バッチ
        (13_000_000, 13),        # ちょうど13バッチ
        (13_000_001, 14),        # +1 で14バッチ
    ])
    def test_batch_count_boundary(self, extractable, expected_batches):
        """バッチ数計算の境界値（ceil 計算ミス検知）"""
        m = Migrator('postgresql://x')
        result = m.estimate_migration(
            total_rows=extractable,
            extractable=extractable,
            free_gb=100.0,
        )
        assert result['batch_count'] == expected_batches

    def test_safe_batch_boundary_around_threshold(self):
        """バッチ方式の安全判定: 「peak_bloat_gb + 1.0GB」が境界"""
        m = Migrator('postgresql://x')
        peak = m.estimate_migration(0, 0, 100.0)['peak_bloat_gb_batch']
        threshold = peak + 1.0  # 実装が要求する最小空き

        # 境界の少し下は危険判定
        result_below = m.estimate_migration(12_000_000, 12_000_000, threshold - 0.01)
        assert result_below['safe_batch'] is False

        # 境界の少し上は安全判定
        result_above = m.estimate_migration(12_000_000, 12_000_000, threshold + 0.01)
        assert result_above['safe_batch'] is True

    def test_safe_full_scales_with_extractable(self):
        """一括方式は extractable に比例して大きな空きが必要"""
        m = Migrator('postgresql://x')
        small = m.estimate_migration(1_000_000, 1_000_000, 5.0)
        large = m.estimate_migration(12_761_402, 12_761_402, 5.0)
        # 100万行なら safe、1270万行なら unsafe
        assert small['safe_full'] is True
        assert large['safe_full'] is False

    def test_peak_bloat_gb_full_proportional_to_extractable(self):
        """peak_bloat_gb_full は extractable × ROW_SIZE_BYTES × 2"""
        m = Migrator('postgresql://x')
        result = m.estimate_migration(1_000_000, 1_000_000, 100.0)
        expected_gb = (1_000_000 * ROW_SIZE_BYTES * 2) / (1024 ** 3)
        assert abs(result['peak_bloat_gb_full'] - round(expected_gb, 2)) < 0.01


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
