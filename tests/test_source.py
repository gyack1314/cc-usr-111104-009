"""核对资料来源与原始字节存储。"""
import json
import tempfile
import unittest
import urllib.request
from pathlib import Path
from source_lab.http import start
from workbench.raw_store import save_bytes, read_bytes

class SourceTests(unittest.TestCase):
    def test_raw_roundtrip(self):
        with tempfile.TemporaryDirectory() as d:
            k=save_bytes(d,b'\x00raw\xff');self.assertEqual(k,save_bytes(d,b'\x00raw\xff'))
            self.assertEqual(read_bytes(d,k),b'\x00raw\xff')
            (Path(d)/k).write_bytes(b'bad')
            with self.assertRaises(ValueError):read_bytes(d,k)
    def test_source_routes(self):
        for config in sorted(Path('sources').glob('*.json')):
            value=json.loads(config.read_text(encoding='utf8'))
            if not isinstance(value,dict) or 'routes' not in value:continue
            s=start(config)
            try:
                for rule in value['routes']:
                    for expected in rule['responses']:
                        payload=json.dumps(rule.get('body',{})).encode() if rule.get('method')=='POST' else None
                        req=urllib.request.Request(f'http://127.0.0.1:{s.server_port}'+rule['path'],data=payload,method=rule.get('method','GET'))
                        try:response=urllib.request.urlopen(req,timeout=3)
                        except urllib.error.HTTPError as e:response=e
                        with response:
                            self.assertEqual(response.status,expected.get('status',200))
                            data=response.read()
                            wanted=(config.parent/expected['file']).read_bytes() if 'file' in expected else expected.get('text','').encode()
                            self.assertEqual(data,wanted)
            finally:s.shutdown();s.server_close()
    def test_reject_bad_key(self):
        with self.assertRaises(ValueError):read_bytes('.', '../x')
if __name__=='__main__':unittest.main()
