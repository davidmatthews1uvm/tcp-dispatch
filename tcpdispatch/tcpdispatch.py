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

def _get_ip(ip=None, get_ip=None):
    IP = default_get_ip()
    if (ip is not None):
        IP = ip
    elif (get_ip is not None):
        IP = get_ip()
    return IP

def run_dispatch(get_work, ip=None, get_ip=None):
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
            IP = _get_ip(ip=ip, get_ip=get_ip)
            with open("IDX_SERVER_PORT.txt", "w") as f:
                f.write(f"{IP}:{PORT}")
            with socketserver.TCPServer((HOST, PORT), MyTCPHandler) as server:
                server.serve_forever()
        except (OSError, PermissionError) as e:
            new_port = random.randint(0, 1<<16)
            print(f"Failed to start server on PORT: {PORT}. Trying: {new_port}")
            PORT = new_port


def run_collect(ip=None, get_ip=None):
    class MyTCPHandler(socketserver.BaseRequestHandler):
        def handle(self):
            
            payload_size = struct.unpack("i", self.request.recv(4))[0]
            payload = b''
            while len(payload) < payload_size:
                payload += self.request.recv(min(1<<16, payload_size - len(payload)))
            payload_str = payload.decode("utf-8")

            payload_dict = json.loads(payload_str)
            
            # append to json file
            with open(f"{payload_dict['dest_file']}", "a") as f:
                f.write(payload_str + "\n")
    HOST = "0.0.0.0"
    PORT = 9999
    success = False
    tq = tqdm(unit_scale=True)
    while not success:
        try:
            IP = _get_ip(ip=ip, get_ip=get_ip)
            with open("RESULTS_SERVER_PORT.txt", "w") as f:
                f.write(f"{IP}:{PORT}")
                
            with socketserver.TCPServer((HOST, PORT), MyTCPHandler) as server:
                while True:
                    server.handle_request()
                    tq.update()
            
        except (OSError, PermissionError) as e:
            new_port = random.randint(0, 1<<16)
            print(f"Failed to start server on PORT: {PORT}. Trying: {new_port}")
            PORT = new_port

### CLIENT
def get_server_by_file(file_str):
    server, port_str = open(file_str, "r").read().split(":")
    port = int(port_str)
    return (server, port)
    
def get_next_job(host, port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            payload_size = struct.unpack("i", sock.recv(4))[0]
            payload = b''
            while len(payload) < payload_size:
                payload += sock.recv(min(1<<16, payload_size - len(payload)))
            # payload = sock.recv(payload_size)
            assert len(payload) == payload_size
        payload_str = payload.decode("utf-8")
        return json.loads(payload_str)
    except Exception as e:
        print(e)
        # raise e


def submit_results(host, port, payload):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        payload_size = len(payload)
        sock.sendall(struct.pack("i", payload_size))
        sock.sendall(payload)

def time_remaining(start_time, single_work_time=0.0, run_seconds=-1.0):
    elapse_time =   (time.time() + single_work_time) - start_time
    return elapse_time < 0.95 * run_seconds or run_seconds < 0
    
