#!/usr/bin/env python
import tarfile
import subprocess
import pathlib
import sys
import os

compress_prog = [
    "./compress.py"
]
decompress_prog = [
    "./compress.py", "-d"
]

valid = {}

with open("/tmp/example_tar.tarlp", 'wb') as outfile:
    with subprocess.Popen(compress_prog, stdin=subprocess.PIPE, stdout=outfile, shell=False, stderr=sys.stderr) as proc:
        with tarfile.open(mode="w|", fileobj=proc.stdin) as t:
            for root, dirs, files in os.walk(pathlib.Path("./example_data/"), topdown=True):
                for file in files:
                    root = pathlib.Path(root)
                    file_path = root/file
                    with open(file_path, 'rb') as reader:
                        print(">", file_path, file=sys.stderr)
                        t.addfile(t.gettarinfo(name=str(file_path)), fileobj=reader)

                        reader.seek(0, os.SEEK_SET)
                        valid[str(file_path)] = reader.read()

print("finished writing\n\n")

with open("/tmp/example_tar.tarlp", "rb") as infile:
    with subprocess.Popen(decompress_prog, stdin=infile, stdout=subprocess.PIPE, shell=False, stderr=sys.stderr) as proc:
        with tarfile.open(mode="r|", fileobj=proc.stdout) as t:
            for i in t:
                if i.isfile():
                    print("<", i.name, i.size)
                    recovered_reader = t.extractfile(i)
                    assert valid[i.name] == recovered_reader.read()
    
