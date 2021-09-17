#%%
import json
import random
import socketserver
import struct
from subprocess import check_output

from tqdm import tqdm

def run(get_work):
    work_iter = iter(tqdm(get_work()))

    def shutdown():
        print("JOB IDX SERVER DONE!", flush=True)
        exit(0)

    class MyTCPHandler(socketserver.BaseRequestHandler):
        def handle(self):
            try:
                job_spec = next(work_iter)            
                payload = json.dumps(job_spec).encode("utf-8")
                payload_size = len(payload)
                self.request.sendall(struct.pack("i", payload_size))
                self.request.sendall(payload)
            except StopIteration:
                shutdown()

    HOST = "0.0.0.0"
    PORT = 9999
    success = False
    while not success:
        try:
            IP = [ip for ip in check_output("hostname -I".split()).decode('utf-8').split() if ip[:2] == "10"][0] # 132.198.x.y hostnames are for external traffic therefore avoid them.
            with open("IDX_SERVER_PORT.txt", "w") as f:
                f.write(f"{IP}:{PORT}")
                
            with socketserver.TCPServer((HOST, PORT), MyTCPHandler) as server:
                server.serve_forever()
            
        except (OSError, PermissionError) as e:
            new_port = random.randint(0, 1<<16)
            print(f"Failed to start server on PORT: {PORT}. Trying: {new_port}")
            PORT = new_port
