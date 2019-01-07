import subprocess
import multiprocessing
import os, time

def submit(*cmd):
    subprocess.Popen(cmd, bufsize=1000)

if __name__ == '__main__':
    count = multiprocessing.cpu_count()
    print(f"Available cpu's: {count}")

    print("Creating process pools")
    pool_s1 = multiprocessing.Pool(processes=1)
    pool_s2 = multiprocessing.Pool(processes=8)
    pool_s3 = multiprocessing.Pool(processes=1)
    pool_s4 = multiprocessing.Pool(processes=1)
    pool_s5 = multiprocessing.Pool(processes=1)

    print("Initiating workers")
    pool_s1.starmap(submit, [('python', 'p4_worker_s1.py')])
    pool_s2.starmap(submit, [('python', 'p4_worker_s2.py')] * 8)
    pool_s3.starmap(submit, [('python', 'p4_worker_s3.py')])
    pool_s4.starmap(submit, [('python', 'p4_worker_s4.py')])

    while(True):
        time.sleep(1)