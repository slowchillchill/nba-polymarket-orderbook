#!/usr/bin/env bash
set -euo pipefail
# 在部署主机运行，参数为已验收的准确 commit。安装后不启用定时器。
commit=${1:?需要已审完整 commit}
test "$(git rev-parse HEAD)" = "$commit"
install -d /opt/nba-orderbook/releases /etc/nba-orderbook /var/lib/nba-orderbook
release=/opt/nba-orderbook/releases/$commit
install -d "$release"
git archive "$commit" | tar -x -C "$release"
python3.12 -m venv /opt/nba-orderbook/venv
/opt/nba-orderbook/venv/bin/pip install -r "$release/requirements.txt"
/opt/nba-orderbook/venv/bin/pip install --no-deps "$release"
ln -sfn "$release" /opt/nba-orderbook/current
install -m 644 "$release/deploy/nba-orderbook.service" "$release/deploy/nba-orderbook.timer" /etc/systemd/system/
systemctl daemon-reload
