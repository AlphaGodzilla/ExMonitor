import json
import re
from pathlib import Path

import requests

metadata_file_dir = "output/metadata/"
metadata_file_dir_path = Path(metadata_file_dir)
icon_file_dir = "output/icon/"
Path(icon_file_dir).mkdir(parents=True, exist_ok=True)

file_count = sum(1 for entry in metadata_file_dir_path.iterdir() if entry.is_file())
crt_count = 0
for entry in metadata_file_dir_path.iterdir():
    crt_count += 1
    if not entry.is_file():
        continue
    with open(str(entry.resolve())) as file:
        raw = file.read()
    metadata = json.loads(raw)
    logo = metadata['logo']
    if logo is None or len(logo) <= 0:
        continue
    logo_path = Path(logo)
    icon_file = Path(icon_file_dir + entry.stem + logo_path.suffix)
    if icon_file.exists():
        print(f"图标已存在, {str(icon_file.resolve())}")
        continue
    resp = requests.get(logo, stream=True)
    icon_file_abs = str(icon_file.resolve())
    if 200 <= resp.status_code < 400:
        with open(icon_file_abs, 'wb') as file:
            for chunk in resp.iter_content(1024):
                file.write(chunk)
            print(f"图标成功下载并保存到: {icon_file_abs}")
    else:
        print(f"图标下载失败: {entry.name}")
    print(f"同步进度: {crt_count/file_count*100}%")

# 将图片文件名转为大写
icon_file_dir_path = Path(icon_file_dir)
lower_pattern = r'[a-z]'
for entry in icon_file_dir_path.iterdir():
    icon_name = entry.stem
    is_lower = bool(re.match(lower_pattern, icon_name))
    if not is_lower:
        continue
    parent_dir = str(entry.parent.resolve())
    icon_name = icon_name.upper()
    print(parent_dir + "/" + icon_name + entry.suffix)
    entry.rename(Path(parent_dir + "/" + icon_name + entry.suffix))