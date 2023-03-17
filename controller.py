import subprocess
import applescript
import os
import time
import socket
import config
import threading
import sys
import shutil
from utils import Colors as c

subprocess.call(['chmod', '+x', 'startup.sh'])

pwd = os.getcwd()

if os.path.exists(config.FILES_PATH):
    # os.rmdir(config.FILES_PATH)
    shutil.rmtree(config.FILES_PATH)
os.mkdir(config.FILES_PATH)

print(f"================= {c.SELECTED}STARTING RAFT{c.ENDC} =================")

for client in config.CLIENT_PORTS.keys():
    print(f'Starting {client}...')
    applescript.tell.app("Terminal",f'do script "{pwd}/startup.sh {client}"')
    time.sleep(0.5)

client_name = "CLI"

connections = {}

def receive(app):
    while True:
        try:
            message = app.recv(config.BUFF_SIZE).decode()
            if not message:
                app.close()
                break
        except:
            app.close()
            break

def execute_command(seg_cmd):
    op_type = seg_cmd[0]

    if op_type == '#':
        return
    
    elif op_type == "wait":
        input(f"Press {c.BLINK}ENTER{c.ENDC} to continue simulation...")

    # create <leader_client> <member 1> <member 2> ...
    elif op_type == "create":
        client = seg_cmd[1]
        cmd = "CREATE"
        for member in seg_cmd[2:]:
            cmd = cmd + " " + member
        connections[client].sendall(bytes(cmd, "utf-8"))
    
    # put <leader_client> <key> <value>
    elif op_type == "put":
        client = seg_cmd[1]
        cmd = f'PUT {seg_cmd[2]} {seg_cmd[3]} {seg_cmd[4]}'
        connections[client].sendall(bytes(cmd, "utf-8"))
    
    # get <leader_client> <key>
    elif op_type == "get":
        client = seg_cmd[1]
        cmd = f'GET {seg_cmd[2]} {seg_cmd[3]}'
        connections[client].sendall(bytes(cmd, "utf-8"))
    
    # printdict <leader_client> <dict_id>
    elif op_type == "printdict":
        client = seg_cmd[1]
        cmd = f'PRINTDICT {seg_cmd[2]}'
        connections[client].sendall(bytes(cmd, "utf-8"))
    
    # printall <leader_client>
    elif op_type == "printall":
        client = seg_cmd[1]
        cmd = f'PRINTALL'
        connections[client].sendall(bytes(cmd, "utf-8"))

    # faillink <client1> <client2>
    elif op_type == "faillink":
        client = seg_cmd[1]
        cmd = f'FAILLINK {seg_cmd[2]}'
        connections[client].sendall(bytes(cmd, "utf-8"))

    # fixlink <client1> <client2>
    elif op_type == "fixlink":
        client = seg_cmd[1]
        cmd = f'FIXLINK {seg_cmd[2]}'
        connections[client].sendall(bytes(cmd, "utf-8"))

    # fail <client>
    elif op_type == "fail":
        client = seg_cmd[1]
        cmd = f'FAILPROCESS'
        connections[client].sendall(bytes(cmd, "utf-8"))
    
    # fix <client>
    elif op_type == "fix":
        client = seg_cmd[1]
        cmd = f'FIXPROCESS'
        connections[client].sendall(bytes(cmd, "utf-8"))

    elif op_type == "start":
        for _, connection in connections.items():
            connection.sendall(bytes("START", "utf-8"))
    
    elif op_type == "delay":
        t = float(seg_cmd[1])
        time.sleep(t)
    
    else:
        print(f'{c.ERROR}Invalid command!{c.ENDC}')

def send():
    while True:
        command = input(">>> ").strip()
        if command != "":
            seg_cmd = command.split()
            op_type = seg_cmd[0]

            if op_type == "simulate":
                print('========== STARTING SIMULATION ==========')
                with open('simulate.txt') as f:
                    start_time = time.time()
                    for line in f.readlines():
                        if line.strip() != "":
                            if line.startswith('#'):
                                print(f'{c.VIOLET}{line}{c.ENDC}')
                            else:
                                print(f'{line}')
                            seg = line.strip().split()
                            execute_command(seg)
                    end_time = time.time()
                    elapsed_time = end_time - start_time
                    print(f'{c.BLUE}Execution time: {elapsed_time} seconds{c.ENDC}')
                print('========== SIMULATION COMPLETE ==========')
            
            elif op_type == "exit":
                for connection in connections.values():
                    # connection.sendall(bytes("EXIT", "utf-8"))
                    connection.close()
                sys.exit(0)

            else:
                execute_command(seg_cmd)

def connect_to(name, port):
    print(f'startup# Connecting to {name}...')
    connections[name] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connections[name].setblocking(True)
    connections[name].connect((config.HOST, port))
    connections[name].sendall(bytes(client_name, "utf-8"))
    print(f"startup# {connections[name].recv(config.BUFF_SIZE).decode()}")
    thread = threading.Thread(target=receive, args=(connections[name],))
    thread.start()

if __name__ == "__main__":
    time.sleep(1)
    for client, port in config.CLIENT_PORTS.items():
        connect_to(client, port)

    print(f"================= {c.SELECTED}SETUP COMPLETE{c.ENDC} =================")
    send()