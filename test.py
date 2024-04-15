#!/usr/bin/env python
import tarfile
import subprocess
import pathlib
import sys

compress_prog = [
    "./compress.py"
]
decompress_prog = [
    "./compress.py", "-d"
]

with open("/tmp/example_tar.tarlp", 'wb') as outfile:
    with subprocess.Popen(compress_prog, stdin=subprocess.PIPE, stdout=outfile, shell=False, stderr=sys.stderr) as proc:
        with tarfile.open(mode="w|", fileobj=proc.stdin) as t:
            for root, dirs, files in pathlib.Path("./example_data/").walk(top_down=True):
                for file in files:
                    file_path = root/file
                    with open(file_path, 'rb') as reader:
                        print(">", file_path, file=sys.stderr)
                        t.add(str(file_path))

print("finished writing\n\n")

with open("/tmp/example_tar.tarlp", "rb") as infile:
    with subprocess.Popen(decompress_prog, stdin=infile, stdout=subprocess.PIPE, shell=False, stderr=sys.stderr) as proc:
        with tarfile.open(mode="r|", fileobj=proc.stdout) as t:
            for i in t:
                if i.isfile():
                    print("<", i.name, i.size)
                    pass
    
