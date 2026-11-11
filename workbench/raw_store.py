"""按摘要保存原始字节，读取时重新核对内容。"""
import hashlib
import os
import tempfile
from pathlib import Path

def save_bytes(root, content):
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    key=hashlib.sha256(content).hexdigest();target=root/key
    if target.exists():
        read_bytes(root,key);return key
    fd,name=tempfile.mkstemp(prefix='.part-',dir=root)
    try:
        with os.fdopen(fd,'wb') as f:
            f.write(content);f.flush();os.fsync(f.fileno())
        os.replace(name,target)
    finally:
        if os.path.exists(name):os.unlink(name)
    return key

def read_bytes(root,key):
    if len(key)!=64 or any(c not in '0123456789abcdef' for c in key):raise ValueError('摘要格式不合法')
    data=(Path(root)/key).read_bytes()
    if hashlib.sha256(data).hexdigest()!=key:raise ValueError('原始资料摘要不匹配')
    return data
