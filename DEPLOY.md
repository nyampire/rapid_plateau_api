# VPS デプロイ手順書

Plateau Building Data Pipeline と RapiD エディタを VPS にデプロイする手順です。

## 前提

- VPS: Ubuntu 22.04 LTS 以上
- ドメイン: 1つ（例: `plateau.example.com`）
- DNS: ドメインが VPS の IP を指していること

## 構成図

```
ブラウザ
  |
  |  https://plateau.example.com/          → RapiD エディタ (静的ファイル)
  |  https://plateau.example.com/api/...   → Plateau API (リバースプロキシ)
  v
nginx
  |
  |-- /               → /var/www/rapid/        (静的ファイル配信)
  |-- /api/           → proxy_pass :8000/api/  (FastAPI)
  |-- /health         → proxy_pass :8000       (FastAPI)
  v
uvicorn (FastAPI, port 8000)
  |
  v
PostgreSQL + PostGIS
```

---

# 第 1 部 新規構築

サーバをまっさらな状態から立ち上げる手順。
0 から 7 まで順番に進める。

## 0. SSH 接続設定

### 初回接続とユーザー作成

VPS プロバイダから提供された IP アドレスと root パスワードで初回接続します。

```bash
# 初回接続 (root)
ssh root@<VPS_IP_ADDRESS>

# 作業用ユーザーを作成
adduser plateau
usermod -aG sudo plateau
```

### SSH 鍵認証の設定

パスワード認証を無効化し、鍵認証のみにします。

```bash
# ローカルマシンで SSH 鍵を生成（未作成の場合）
ssh-keygen -t ed25519 -C "vps"

# 公開鍵を VPS に転送
ssh-copy-id -i ~/.ssh/id_ed25519.pub plateau@<VPS_IP_ADDRESS>

# 鍵認証で接続できることを確認
ssh plateau@<VPS_IP_ADDRESS>
```

### SSH セキュリティ強化 (VPS 側)

```bash
sudo cp /etc/ssh/sshd_config /etc/ssh/sshd_config.bak

sudo sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
sudo sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
sudo sed -i 's/^#\?PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config

# SSHポートを変更する場合（任意）
# sudo sed -i 's/^#\?Port.*/Port 2222/' /etc/ssh/sshd_config
# sudo ufw allow 2222/tcp

sudo systemctl restart sshd
```

> **注意**: `PasswordAuthentication no` に変更する前に、鍵認証での接続が成功することを必ず確認してください。

### ローカルの SSH config 設定（任意）

`~/.ssh/config` に以下を追加すると、接続が簡単になります。

```
Host vps
    HostName <VPS_IP_ADDRESS>
    User plateau
    IdentityFile ~/.ssh/id_ed25519
    # Port 2222  # ポートを変更した場合
```

以降は `ssh vps` で接続できます。

---

## 1. VPS 初期設定

```bash
# パッケージ更新
sudo apt update && sudo apt upgrade -y

# 必要パッケージ
sudo apt install -y \
  nginx certbot python3-certbot-nginx \
  postgresql postgresql-contrib postgis \
  python3 python3-pip python3-venv \
  git curl unzip

# ファイアウォール
sudo ufw allow OpenSSH
sudo ufw allow 'Nginx Full'
sudo ufw enable
```

---

## 2. PostgreSQL + PostGIS セットアップ

```bash
# PostGIS 拡張インストール
sudo apt install -y postgresql-14-postgis-3
# ※ PostgreSQL のバージョンに合わせて変更 (例: postgresql-16-postgis-3)

# DB ユーザーとデータベース作成
sudo -u postgres psql <<'SQL'
CREATE USER osmfj_user WITH PASSWORD 'ここにパスワードを設定';
CREATE DATABASE osmfj_plateau OWNER osmfj_user;
\c osmfj_plateau
CREATE EXTENSION postgis;
GRANT ALL PRIVILEGES ON DATABASE osmfj_plateau TO osmfj_user;
SQL
```

### テーブル作成

```bash
sudo -u postgres psql -d osmfj_plateau <<'SQL'
CREATE TABLE plateau_buildings (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT,
    building TEXT DEFAULT 'yes',
    height DOUBLE PRECISION,
    ele DOUBLE PRECISION,
    building_levels INTEGER,
    building_levels_underground INTEGER,
    source_dataset TEXT,
    plateau_id TEXT,
    geometry_wkt TEXT,
    name TEXT,
    addr_full TEXT,
    addr_housenumber TEXT,
    addr_street TEXT,
    start_date TEXT,
    building_material TEXT,
    roof_material TEXT,
    roof_shape TEXT,
    amenity TEXT,
    shop TEXT,
    tourism TEXT,
    leisure TEXT,
    landuse TEXT,
    geom GEOMETRY(Polygon, 4326),
    centroid GEOMETRY(Point, 4326),
    city_code VARCHAR(5) NOT NULL,
    building_part TEXT,
    parent_building_id INTEGER REFERENCES plateau_buildings(id) ON DELETE CASCADE,
    ref_mlit_plateau TEXT
);

CREATE TABLE plateau_building_nodes (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT,
    building_id INTEGER REFERENCES plateau_buildings(id) ON DELETE CASCADE,
    sequence_id INTEGER,
    ring_id INTEGER NOT NULL DEFAULT 0,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    geom GEOMETRY(Point, 4326)
);

-- インデックス
CREATE INDEX idx_buildings_geom ON plateau_buildings USING GIST (geom);
CREATE INDEX idx_buildings_centroid ON plateau_buildings USING GIST (centroid);
CREATE INDEX idx_buildings_osm_id ON plateau_buildings (osm_id);
CREATE INDEX idx_buildings_city_code ON plateau_buildings (city_code);
CREATE INDEX idx_buildings_parent_building_id ON plateau_buildings (parent_building_id)
    WHERE parent_building_id IS NOT NULL;
CREATE INDEX idx_nodes_building_id ON plateau_building_nodes (building_id);
CREATE INDEX idx_nodes_osm_id ON plateau_building_nodes (osm_id);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO osmfj_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO osmfj_user;
SQL
```

> **`plateau_building_nodes.building_id` の `ON DELETE CASCADE` を外さないこと。**
> 行政界フィルタは `DELETE FROM plateau_buildings` の 1 文だけを実行し、ノード側の削除を外部キーに任せている。
> CASCADE が無いと、市境の外に落ちる建物を持つ最初の都市で外部キー違反が起き、その都市の取り込みが失敗する。
> `plateau_migrate_fk_cascade.py` は既存データベースをこの状態に揃えるための移行用で、新規構築の手順には要らない。

> **`city_code` は必ず上の DDL で作ること。**
> インポーターは初回実行時に `building_part` / `ref_mlit_plateau` / `parent_building_id` / `ring_id` を
> 自分で追加するが (`_ensure_schema`)、**`city_code` はその対象外**である。
> この列が無いと最初の取り込みが `column "city_code" does not exist` で失敗する。
> `plateau_migrate.py` は既存データベースにこの列を足すための移行用で、新規構築の手順には要らない。
>
> `NOT NULL` にしてあるのは本番に合わせたもの。インポーターは `--citycode` が未指定なら
> `--data-dir` のディレクトリ名から市区町村コードを推定するので、`plateau_data/31202` のように
> コードを名前に含めておくか、`--citycode` を明示する。どちらも無いと値が NULL になり INSERT が落ちる。

> **注意**: `ring_id` カラムは上の `CREATE TABLE` に含まれているので新規構築では問題にならない。
> 既存データベースをこの手順のバージョンに追従させる場合は、`ALTER TABLE plateau_building_nodes ADD COLUMN ring_id INTEGER NOT NULL DEFAULT 0;` をこの時点、つまり API のデプロイより前に必ず実行する。
> API のノード取得クエリは `n.ring_id` を毎リクエスト参照するが、このカラムを作るのはインポーターの初回実行だけなので、ALTER 前に API を新しいバージョンへ更新すると、インポート未実行のデータベースに対するリクエストが全て 500 エラーになる。
> このカラムはデフォルト値が volatile でないため、PostgreSQL の ADD COLUMN はメタデータのみの変更で済み、テーブル全体の書き換えは発生しない。
> ただし ACCESS EXCLUSIVE ロックを取得するので、長時間実行中のクエリが無いタイミングで流すこと。

> **`dash_city_master` について**: このテーブルはダッシュボードが作るもので、上の DDL には含まれない。
> インポーターは行政界 N03 フィルタ (source city の境界外に落ちる建物 = 隣接市との重複を削除する) でこれを参照するが、**存在しなくてよい**。
> 無い場合はフィルタだけを警告付きでスキップし、取り込み自体は通常どおり完走する (重複が残るだけで、API 側の同等フィルタが効く)。
> ダッシュボードを併設しない構成でも、この表を作る必要は無い。
>
> 148 都市を一括で入れる場合は、流し始める前に `deploy/README.md` の開始前確認を読む。
> ダッシュボードを併設する構成では、そこで挙げている「148 都市すべてが行を持つこと」の確認が必須になる。

---

## 3. Plateau API デプロイ

### アプリケーション配置

```bash
# ディレクトリ作成
sudo mkdir -p /opt/plateau-api
sudo chown $USER:$USER /opt/plateau-api

# ファイルをコピー (ローカルから scp)
scp osmfj_plateau_api.py plateau_downloader.py plateau_importer2postgis.py \
    plateau_coverage.py plateau_purge.py \
    user@vps:/opt/plateau-api/
scp -r deploy user@vps:/opt/plateau-api/

# または git で取得
cd /opt/plateau-api
git clone <リポジトリURL> .
```

### Python 環境構築

```bash
cd /opt/plateau-api
python3 -m venv venv
source venv/bin/activate

pip install fastapi uvicorn psycopg2-binary requests beautifulsoup4
```

### 環境変数の設定

```bash
cat > /opt/plateau-api/.env <<'EOF'
DATABASE_URL=postgresql://osmfj_user:ここにパスワードを設定@localhost:5432/osmfj_plateau
EOF
chmod 600 /opt/plateau-api/.env
```

### データの取り込み

`plateau_downloader.py` が読みに行っていた配信元は停止している。
現在は手元 (Mac) で CityGML を変換し、サーバへ送って取り込む経路を使う。

第 1 段 (手元での変換と転送) の手順は `scripts/reimport/README.md` にまとめてある。
第 2 段 (サーバでの取り込み) の手順、必要な環境変数、開始前に確かめる項目は `deploy/README.md` にまとめてある。
ここでは重複させないので、両方を参照して全都市の取り込みを終わらせる。

取り込みは 20 時間以上かかる。
節 4 から節 6 (RapiD エディタのビルドとデプロイ、nginx 設定、SSL) はこの取り込みに依存しないので、完了を待たずに進めてよい。
対応エリアのビューの作成だけは、取り込みが全都市終わってからにする。

取り込みが終わったら、対応エリアのビューを作る。

```bash
cd /opt/plateau-api
source venv/bin/activate
set -a
source .env
set +a
python3 plateau_coverage.py --init --postgres-url "$DATABASE_URL"
```

`$DATABASE_URL` は `.env` を読み込まないと空文字のままで、`--postgres-url ""` が
環境変数 `DATABASE_URL` へのフォールバックより優先されてしまう。
システムの `python3` には `psycopg2` が無いので、venv の有効化も必ず先に行う。

`--init` はビューを作ったあとリフレッシュまで済ませる。
このビューが無いと、RapiD エディタの対応エリア表示が出ない。
以降に都市を足したときの再計算は第 2 部の 8-4 を参照する。

### systemd サービス登録

```bash
sudo cat > /etc/systemd/system/plateau-api.service <<'EOF'
[Unit]
Description=Plateau Building Data API
After=network.target postgresql.service

[Service]
Type=simple
User=www-data
Group=www-data
WorkingDirectory=/opt/plateau-api
EnvironmentFile=/opt/plateau-api/.env
ExecStart=/opt/plateau-api/venv/bin/uvicorn osmfj_plateau_api:app \
    --host 127.0.0.1 \
    --port 8000 \
    --workers 2 \
    --log-level info
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable plateau-api
sudo systemctl start plateau-api

# 動作確認
curl http://127.0.0.1:8000/health
```

---

## 4. RapiD エディタのビルドとデプロイ

### ローカルでビルド

RapiD のソースツリーで作業します。

#### 4-1. API URL を本番用に変更

`modules/services/MapWithAIService.js` を編集:

```javascript
// 変更前 (line 429)
return `http://localhost:8000/api/mapwithai/buildings?${params.toString()}`;

// 変更後
return `https://plateau.example.com/api/mapwithai/buildings?${params.toString()}`;
```

`data/osmf_datasets.json` を編集:

```json
{
  "url": "https://plateau.example.com/api/mapwithai/buildings"
}
```

#### 4-2. プロダクションビルド

```bash
cd /path/to/Rapid
npm install
npm run build
npm run dist
```

`dist` の前に `build` を通す。
`dist/data/l10n/*.min.json` は `build` 側が作るので、`dist` だけ流すと
JavaScript は新しいのに文言だけ古いままのビルドができる (2026-07-19 に実際に起きた)。

`dist/` ディレクトリに以下が生成されます:
- `index.html`
- `rapid.min.js` (プロダクションバンドル)
- `rapid.legacy.min.js` (レガシーブラウザ用)
- `rapid.css`
- `img/`, `data/` 等のアセット

#### 4-3. VPS にアップロード

```bash
# VPS 側のディレクトリ作成
ssh user@vps "sudo mkdir -p /var/www/rapid && sudo chown www-data:www-data /var/www/rapid"

# ドライランで削除対象を確認する (Deleting が 1 件も出ないことを確かめる)
rsync -avzn --delete --exclude '/dashboard/' dist/ user@vps:/var/www/rapid/

# dist の中身をアップロード
rsync -avz --delete --exclude '/dashboard/' dist/ user@vps:/var/www/rapid/
```

`--exclude '/dashboard/'` を必ず付ける。
ダッシュボードは web root の中に置かれるが `dist/` の生成物には含まれないため、
このオプションが無いと更新のたびにダッシュボードごと削除される。
新規構築の時点ではダッシュボードがまだ存在しないので実害は出ないが、
次に Rapid を更新するときから効いてくる。

---

## 5. nginx 設定

1つのドメインで RapiD（静的ファイル）と Plateau API（リバースプロキシ）を同居させます。

```bash
sudo cat > /etc/nginx/sites-available/plateau <<'NGINX'
server {
    listen 80;
    server_name plateau.example.com;

    root /var/www/rapid;
    index index.html;

    # --- Plateau API (リバースプロキシ) ---
    location /api/ {
        proxy_pass http://127.0.0.1:8000/api/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # タイムアウト (大きなbboxクエリ対応)
        proxy_read_timeout 60s;
    }

    location /health {
        proxy_pass http://127.0.0.1:8000/health;
        proxy_set_header Host $host;
    }

    # --- RapiD エディタ (静的ファイル) ---
    location / {
        try_files $uri $uri/ /index.html;
    }

    # 静的アセットのキャッシュ
    location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg|woff|woff2)$ {
        expires 7d;
        add_header Cache-Control "public, immutable";
    }

    # gzip 圧縮
    gzip on;
    gzip_types text/plain text/css application/javascript application/json application/xml image/svg+xml;
    gzip_min_length 1000;
}
NGINX
```

### 有効化と反映

```bash
sudo ln -s /etc/nginx/sites-available/plateau /etc/nginx/sites-enabled/

# デフォルト設定を無効化（必要に応じて）
sudo rm -f /etc/nginx/sites-enabled/default

sudo nginx -t
sudo systemctl reload nginx
```

---

## 6. SSL/TLS (Let's Encrypt)

```bash
sudo certbot --nginx -d plateau.example.com
```

certbot が nginx 設定を自動で HTTPS 対応に書き換えます。自動更新の確認:

```bash
sudo certbot renew --dry-run
```

---

## 7. 動作確認

### API

```bash
# ヘルスチェック
curl https://plateau.example.com/health

# 統計情報
curl https://plateau.example.com/api/stats

# 建物データ取得テスト (米子市中心部)
curl "https://plateau.example.com/api/mapwithai/buildings?bbox=133.33,35.42,133.34,35.43"
```

### RapiD エディタ

ブラウザで `https://plateau.example.com` にアクセスし、Plateau データレイヤーを有効にして建物データが表示されることを確認します。

---

# 第 2 部 更新

構築済みのサーバに対して繰り返す作業をまとめる。
API のコード更新、Rapid の更新、都市の追加や取り込み直し、対応エリアの再計算の 4 つがある。

## 8. 運用

### ログ確認

```bash
# API ログ
sudo journalctl -u plateau-api -f

# nginx ログ
sudo tail -f /var/log/nginx/access.log
sudo tail -f /var/log/nginx/error.log
```

### 8-1. API のコード更新

```bash
cd /opt/plateau-api
git pull

# スキーマ変更を伴う更新のときだけ、対応するマイグレーションをこの時点で流す
# 例: source venv/bin/activate && python plateau_migrate.py --postgres-url "$DATABASE_URL"

sudo systemctl restart plateau-api
```

`git pull` だけでは動いているプロセスには反映されない。
uvicorn は起動時に読み込んだコードのまま動き続けるので、再起動するまで古いコードで応答する。
順番が大事で、スキーマ移行が要る変更では、再起動より先にマイグレーションを終わらせる。
先に再起動すると、新しいコードが移行前のスキーマにアクセスして落ちる。

### 8-2. Rapid の更新

```bash
cd /path/to/Rapid
npm run build
npm run dist
```

`dist` の前に `build` を通す。
`dist/data/l10n/*.min.json` は `build` 側が作るので、`dist` だけ流すと
JavaScript は新しいのに文言だけ古いままのビルドになる。

送る前にドライランで削除対象を確かめる。

```bash
rsync -avzn --delete --exclude '/dashboard/' dist/ user@vps:/var/www/rapid/
```

`Deleting` として出てくるものが無いことを確認してから、`-n` を外して送る。

```bash
rsync -avz --delete --exclude '/dashboard/' dist/ user@vps:/var/www/rapid/
```

`--exclude '/dashboard/'` を落とすと、ダッシュボードごと削除される。
ダッシュボードは web root の中に置かれるが `dist/` の生成物には含まれない。

### 8-3. 都市を足す、取り込み直す

新しい都市の追加も、既存都市の取り込み直しも同じ手順を使う。

1. 手元 (Mac) で `scripts/reimport/ship_city.sh <citycode>` を実行する。抽出、変換、転送までを 1 本で行う
2. サーバで `~/reimport_one.sh <citycode>` を実行する。取り込みがその都市の既存データを消してから入れ直すので、別途 purge を呼ぶ必要は無い

`ship.env` の用意など手元側の詳細は `scripts/reimport/README.md` を参照する。
複数都市をまとめて流す場合や、サーバ側の環境変数、開始前の確認事項は `deploy/README.md` を参照する。

API の再起動は不要。DB を直接参照しているため。

### 8-4. 対応エリアの再計算

都市を足したあと、対応エリアのビュー (coverage) を作り直す。
都市ごとには行わず、一連の追加が終わったところで 1 回にまとめる。

```bash
cd /opt/plateau-api
source venv/bin/activate
set -a
source .env
set +a
python3 plateau_coverage.py --refresh --no-concurrent --postgres-url "$DATABASE_URL"
```

`.env` を読み込む前に実行すると、`$DATABASE_URL` が空文字のまま渡ってしまう。
システムの `python3` には `psycopg2` が無いので、venv の有効化も必ず先に行う。

`--no-concurrent` を必ず付ける。
省略すると CONCURRENTLY を選び、メモリの小さいサーバでは postgres が OOM で落ちる。

メモリが足りない場合は、実行のあいだだけ一時的に swap を足す。
swap の増設には管理者権限が要る。

### サービス管理

```bash
sudo systemctl status plateau-api    # 状態確認
sudo systemctl restart plateau-api   # 再起動
sudo systemctl stop plateau-api      # 停止
```

---

## トラブルシューティング

| 症状 | 確認事項 |
|------|---------|
| API が応答しない | `sudo systemctl status plateau-api` でサービス状態を確認 |
| DB 接続エラー | `.env` の DATABASE_URL、PostgreSQL の pg_hba.conf を確認 |
| RapiD で建物が表示されない | ブラウザの開発者ツール Network タブで API リクエストを確認。API URL が正しいか、CORS エラーがないか |
| 502 Bad Gateway | uvicorn が起動しているか、ポート 8000 でリッスンしているか確認 |
| SSL 証明書エラー | `sudo certbot renew` で更新。nginx 設定で証明書パスを確認 |
