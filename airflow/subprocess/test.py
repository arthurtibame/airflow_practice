import os
import subprocess

process1 = subprocess.run(["docker", "ps", "-a"], universal_newlines=False, stdout=subprocess.PIPE)

process_output = process1.stdout.decode("utf-8")

a = process_output.split("\n")
print(a[1])
#print(len(a))
