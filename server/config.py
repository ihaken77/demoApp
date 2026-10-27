"""接続先の設定を外部ファイルから読む（AWS フロント側）"""

from pathlib import Path

import yaml


def load_backend_url(name: str, path: Path) -> str:
    """config/targets.yaml から、指定したバックエンドの制御エンドポイントの URL を返す"""
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    backends = data.get("backends", {})
    if name not in backends:
        raise KeyError(f"接続先 '{name}' が {path} に定義されていません")
    return str(backends[name]["control_url"])
