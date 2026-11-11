"""可重复的本地资料HTTP来源，响应规则只用于合成业务资料。"""
import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

def subset(actual, wanted):
    return isinstance(actual,dict) and all(k in actual and (subset(actual[k],v) if isinstance(v,dict) else actual[k]==v) for k,v in wanted.items())

def start(scenario, host='127.0.0.1', port=0):
    path=Path(scenario).resolve()
    config=json.loads(path.read_text(encoding='utf8'))
    counts={}
    events=[]
    lock=threading.Lock()
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def log_message(self,*args): pass
        def do_GET(self): self.reply()
        def do_POST(self): self.reply()
        def reply(self):
            raw=self.rfile.read(int(self.headers.get('Content-Length','0')))
            try: body=json.loads(raw) if raw else {}
            except ValueError: body={}
            with lock:
                events.append({'method':self.command,'path':self.path,'body':body,'headers':dict(self.headers)})
                found=None
                for i,rule in enumerate(config['routes']):
                    if rule.get('method','GET')==self.command and rule['path']==self.path and subset(body,rule.get('body',{})):
                        seq=rule['responses']; n=counts.get(i,0); counts[i]=n+1
                        found=seq[min(n,len(seq)-1)];break
            if found is None:
                self.send_error(404);return
            data=(path.parent/found['file']).read_bytes() if 'file' in found else found.get('text','').encode('utf8')
            self.send_response(found.get('status',200))
            for k,v in found.get('headers',[]):self.send_header(k,v)
            self.send_header('Content-Length',str(len(data)))
            self.end_headers()
            try:
                step=found.get('write_size',len(data) or 1)
                for offset in range(0,len(data),step):
                    self.wfile.write(data[offset:offset+step]);self.wfile.flush()
            except (BrokenPipeError,ConnectionResetError):pass
    server=ThreadingHTTPServer((host,port),Handler)
    server.daemon_threads=True
    server.events=events
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    return server

def main():
    p=argparse.ArgumentParser();p.add_argument('scenario');p.add_argument('--port',type=int,default=8765);a=p.parse_args()
    server=start(a.scenario,port=a.port)
    print(json.dumps({'host':'127.0.0.1','port':server.server_port}),flush=True)
    try:threading.Event().wait()
    except KeyboardInterrupt:pass
    finally:server.shutdown();server.server_close()
if __name__=='__main__':main()
