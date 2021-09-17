#%%
import json
import random
import socketserver
import socket
import struct
from subprocess import check_output
import time

from tqdm import tqdm

### SERVER
def vacc_get_ip():
    return [ip for ip in check_output("hostname -I".split()).decode('utf-8').split() if ip[:2] == "10"][0] # 132.198.x.y hostnames are for external traffic therefore avoid them.

def default_get_ip():
    return socket.gethostbyname(socket.gethostname())

def run(get_work, ip=None, get_ip=None):
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
            IP = None
            if (ip is not None):
                IP = ip
            elif (get_ip is not None):
                IP = get_ip()
            else:
                default_get_ip()

            with open("IDX_SERVER_PORT.txt", "w") as f:
                f.write(f"{IP}:{PORT}")
                
            with socketserver.TCPServer((HOST, PORT), MyTCPHandler) as server:
                server.serve_forever()
            
        except (OSError, PermissionError) as e:
            new_port = random.randint(0, 1<<16)
            print(f"Failed to start server on PORT: {PORT}. Trying: {new_port}")
            PORT = new_port


### CLIENT
def get_server_by_file(file_str):
    server, port_str = open(file_str, "r").read().split()
    port = int(port_str)
    return (server, port)
    
def get_next_job(host, port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            payload_size = struct.unpack("i", sock.recv(4))[0]
            payload = sock.recv(payload_size)
        return json.loads(payload.decode("utf-8"))
    except Exception as e:
        print(e)
        return None


def time_remaining(start_time, single_work_time=0.0, run_seconds=-1.0):
    elapse_time =   (time.time() + single_work_time) - start_time
    return elapse_time < 0.95 * run_seconds or run_seconds < 0
    
