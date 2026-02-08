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
ssh-keygen -t ed25519 -C "plateau-vps"

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
Host plateau-vps
    HostName <VPS_IP_ADDRESS>
    User plateau
    IdentityFile ~/.ssh/id_ed25519
    # Port 2222  # ポートを変更した場合
```

以降は `ssh plateau-vps` で接続できます。

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
    ref_mlit_plateau TEXT,
    name TEXT,
    addr_full TEXT,
    addr_housenumber TEXT,
    addr_street TEXT,
    start_date TEXT,
    survey_date TEXT,
    building_class TEXT,
    building_usage TEXT,
    building_material TEXT,
    roof_material TEXT,
    roof_shape TEXT,
    amenity TEXT,
    shop TEXT,
    tourism TEXT,
    leisure TEXT,
    landuse TEXT,
    geom GEOMETRY(Polygon, 4326),
    centroid GEOMETRY(Point, 4326)
);

CREATE TABLE plateau_building_nodes (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT,
    building_id INTEGER REFERENCES plateau_buildings(id),
    sequence_id INTEGER,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    geom GEOMETRY(Point, 4326)
);

-- インデックス
CREATE INDEX idx_buildings_geom ON plateau_buildings USING GIST (geom);
CREATE INDEX idx_buildings_centroid ON plateau_buildings USING GIST (centroid);
CREATE INDEX idx_buildings_osm_id ON plateau_buildings (osm_id);
CREATE INDEX idx_nodes_building_id ON plateau_building_nodes (building_id);
CREATE INDEX idx_nodes_osm_id ON plateau_building_nodes (osm_id);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO osmfj_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO osmfj_user;
SQL
```

---

## 3. Plateau API デプロイ

### アプリケーション配置

```bash
# ディレクトリ作成
sudo mkdir -p /opt/plateau-api
sudo chown $USER:$USER /opt/plateau-api

# ファイルをコピー (ローカルから scp)
scp osmfj_plateau_api.py plateau_downloader.py plateau_importer2postgis.py \
    user@vps:/opt/plateau-api/

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

### データのダウンロードとインポート

```bash
cd /opt/plateau-api
source venv/bin/activate

# 対象市区町村のデータをダウンロード (例: 米子市)
python plateau_downloader.py --citycode 31202

# PostGIS にインポート
python plateau_importer2postgis.py \
  --data-dir ./plateau_data/31202 \
  --citycode 31202 \
  --postgres-url "postgresql://osmfj_user:パスワード@localhost:5432/osmfj_plateau"

# 全市区町村を一括で行う場合
# python plateau_downloader.py --all --city-interval 30
```

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

RapiD のソースツリー（例: `~/Desktop/Rapid/`）で作業します。

#### 4-1. OSM OAuth2 認証の設定

OSM でカスタム OAuth2 アプリケーションを登録し、`dist/index.html` の `apiConnections` に自身の `client_id` / `client_secret` を設定します。

1. https://www.openstreetmap.org/oauth2/applications で OAuth2 アプリを登録
2. リダイレクト URI に `https://<YOUR_DOMAIN>/land.html` を設定
3. `dist/index.html` の `apiConnections` を編集:

```javascript
context.apiConnections = [
  {
    url: 'https://www.openstreetmap.org',
    apiUrl: 'https://api.openstreetmap.org',
    client_id: '<YOUR_CLIENT_ID>',
    client_secret: '<YOUR_CLIENT_SECRET>'
  }
];
```

> **重要**: `dist/index.html` はビルド時に上書きされないため、ビルド後に手動で設定するか、ルートの `index.html` も同様に変更しておいてください。

#### 4-2. osm-auth と land.html

osm-auth v3.0.0 以降が必要です（2025年7月の OSM サーバー COOP ヘッダー対応）。

```bash
# osm-auth のバージョン確認・更新
npm install osm-auth@latest
```

`dist/land.html` は BroadcastChannel API を使用する形式である必要があります:

```html
<!DOCTYPE html>
<html>
  <head></head>
  <body>
    <script>
      new BroadcastChannel('osm-api-auth-complete').postMessage(window.location.href);
      window.close();
    </script>
  </body>
</html>
```

> **背景**: 2025年7月に OSM サーバーが Cross-Origin Opener Policy (COOP) ヘッダーを追加したため、従来の `window.opener.authComplete()` 方式は動作しません。osm-auth v3 では BroadcastChannel を使用して認証コードを親ウィンドウに受け渡します。

#### 4-3. API URL を本番用に変更

`modules/services/MapWithAIService.js` を編集:

```javascript
// 変更前 (line 429)
return `http://localhost:8000/api/mapwithai/buildings?${params.toString()}`;

// 変更後
return `https://<YOUR_DOMAIN>/api/mapwithai/buildings?${params.toString()}`;
```

`data/osmf_datasets.json` を編集:

```json
{
  "url": "https://<YOUR_DOMAIN>/api/mapwithai/buildings"
}
```

#### 4-4. プロダクションビルド

```bash
cd /path/to/Rapid
npm install
npm run all
```

`dist/` ディレクトリに以下が生成されます:
- `index.html` — エントリポイント（OAuth2 設定含む）
- `land.html` — OAuth2 コールバックページ（BroadcastChannel 対応）
- `rapid.min.js` — プロダクションバンドル（osm-auth v3 含む）
- `rapid.legacy.min.js` — レガシーブラウザ用
- `rapid.css`
- `img/`, `data/` 等のアセット

> **注意**: ビルド後に `dist/index.html` の `client_id` / `client_secret` が正しいか確認してください。ルートの `index.html` を変更していない場合、ビルドでデフォルト値に戻る可能性があります。

#### 4-5. VPS にアップロード

```bash
# VPS 側のディレクトリ作成（初回のみ）
ssh plateau-vps "sudo mkdir -p /var/www/rapid && sudo chown www-data:www-data /var/www/rapid"

# dist の中身をアップロード
rsync -avz --delete dist/ plateau-vps:/var/www/rapid/
```

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

## 8. 運用

### ログ確認

```bash
# API ログ
sudo journalctl -u plateau-api -f

# nginx ログ
sudo tail -f /var/log/nginx/access.log
sudo tail -f /var/log/nginx/error.log
```

### データの追加

新しい市区町村のデータを追加する場合:

```bash
cd /opt/plateau-api
source venv/bin/activate

# ダウンロード
python plateau_downloader.py --citycode 13101

# インポート
python plateau_importer2postgis.py \
  --data-dir ./plateau_data/13101 \
  --citycode 13101
```

API の再起動は不要です（DB を直接参照しているため）。

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
| OSM ログインで白い画面 | `opener is null` エラーの場合、osm-auth v3.0.0 以降 + BroadcastChannel 対応の `land.html` が必要。セクション 4-2 を参照 |
| OSM ログインで redirect_uri エラー | OSM OAuth2 アプリ設定のリダイレクト URI が `https://<YOUR_DOMAIN>/land.html` と一致しているか確認 |
| OSM ログインで client_id エラー | `dist/index.html` の `apiConnections` に正しい `client_id` / `client_secret` が設定されているか確認 |
