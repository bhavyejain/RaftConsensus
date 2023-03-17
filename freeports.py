import subprocess
import re
import os

def get_pids(port):
	command = "sudo lsof -i :%s | awk '{print $2}'" % port
	pids = subprocess.check_output(command, shell=True)
	pids = pids.strip()
	if pids:
		pids = re.sub(' +', ' ', pids)
		for pid in pids.split('\n'):
			try:
				yield int(pid)
			except:
				pass

def free_ports(ports):
    for port in ports:
        pids = set(get_pids(port))
        print(f'pids on port {port}: {pids}')
        for pid in pids:
            print(f'Killing process {pid} on port {port}')
            command = f'sudo kill -9 {str(pid)}'
            os.system(command)