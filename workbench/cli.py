"""查看本地资料文件及其字节指纹。"""
import argparse
import hashlib
import json
from pathlib import Path
def main():
    p=argparse.ArgumentParser();p.add_argument('command',choices=['inspect']);p.add_argument('directory');a=p.parse_args()
    root=Path(a.directory)
    if not root.is_dir():p.error('资料目录不存在')
    rows=[]
    for path in sorted(root.rglob('*')):
        if path.is_file():
            data=path.read_bytes();rows.append({'path':path.relative_to(root).as_posix(),'size':len(data),'sha256':hashlib.sha256(data).hexdigest()})
    print(json.dumps(rows,ensure_ascii=False,indent=2))
if __name__=='__main__':main()
