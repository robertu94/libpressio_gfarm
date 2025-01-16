#!/usr/bin/env python
import argparse
import sys
import tarfile
import json
import os
import math
import copy
import numpy as np
import logging
import libpressio as lp

logger = logging.getLogger()
logging.basicConfig(
        format="%(levelname)s:%(filename)s:%(lineno)d: %(message)s",
        level=int(os.environ.get("LIBPRESSIO_COMPRESS_LOGLEVEL", f"{logging.ERROR}")))

DEFAULT_SCHEMA = {
    "default": {
        "compressor": {
            "compressor_id": "noop",
            "compressor_config": {},
            "early_config": {}
        },
        "data_schema": {
            "dtype": "float",
            "dims": None
        }
    },
    "override": {
        # TODO remove this after testing
        "example_data/FSUTOA_1_1800_3600.dat": {
            "data_schema": {
                "dims": [1800,3600]
            }
        },
        "example_data/CLOUDf48.bin.f32": {
            "data_schema": {
                "dims": [100,500,500]
            }
        },

    }
}

io_index = 0

def write_output(buf, id=""):
    global io_index
    ret =  sys.stdout.buffer.write(buf)
    logger.debug("%s wrote id=%s a=%s, r=%s", io_index, id, len(buf), ret)
    io_index += 1
    return ret

def read_input(N, id=""):
    global io_index
    ret = sys.stdin.buffer.read(N)
    logger.debug("%s read id=%s a=%s r=%s", io_index, id, N, len(ret))
    io_index += 1
    return ret

def round_up(v, round_to):
    return math.ceil(v/round_to)*round_to

def to_fill_block(v, blocksize):
    r = v % blocksize
    if r != 0:
        r = blocksize - r
    logger.debug("to_fill_block r=%s, v=%s bs=%s", r, v, blocksize)
    return r

def to_dtype(s: str):
    if s == "float": return np.float32
    elif s == "double": return np.float64

schema_file = os.getenv("LIBPRESSIO_COMPRESS_SCHEMAFILE")
if schema_file is not None:
    with open(schema_file) as schema_file:
        schema = json.load(schema_file)
else:
    schema = DEFAULT_SCHEMA

default_schema = schema['default']
if "data_schema" not in default_schema:
    default_schema['data_schema'] = DEFAULT_SCHEMA['default']['data_schema']
if "compressor" not in default_schema:
    default_schema['compressor'] = DEFAULT_SCHEMA['default']['compressor']

def decompress():
    while True:
        header_bytes = read_input(tarfile.BLOCKSIZE, id="header")
        try:
            header = tarfile.TarInfo.frombuf(header_bytes, tarfile.ENCODING, "surrogateescape")
        except tarfile.HeaderError:
            break
        logger.debug("decoded %s %s", io_index, header.get_info())
        if header.isfile():
            # determine decompression configuration
            if header.name in schema['override']:
                file_schema = schema['override'][header.name]
                dtype = file_schema.get('data_schema',
                                        default_schema['data_schema']).get("dtype",
                                                                           default_schema['data_schema']['dtype'])
                dims = file_schema.get('data_schema',
                                        default_schema['data_schema']).get("dims",
                                                                           default_schema['data_schema']['dims'])
                config = file_schema.get("compressor", default_schema['compressor'])
            else:
                dtype = default_schema['data_schema']['dtype']
                dims = default_schema['data_schema']['dims']
                config = default_schema['compressor']

            #read raw data from archive
            remaining = round_up(header.size, tarfile.BLOCKSIZE)
            data_parts = []
            while remaining > 0:
                logger.info("READING %s reamaining=%s", header.name, remaining)
                data = read_input(remaining, id="file_data")
                remaining -= len(data)
                data_parts.append(data)

            #create an input object for compression
            full_data = b"".join(data_parts)
            full_data = full_data[:header.size]
            full_data = np.frombuffer(full_data, dtype=np.byte)
            #create an output object for decompression
            output = np.zeros(dims, dtype=to_dtype(dtype))

            #run decompression
            logger.debug("%s compressed=%s %s %s %s", header.name, full_data.shape, dims, dtype, config)
            compressor = lp.PressioCompressor.from_config(config)
            output = compressor.decode(full_data, output)
            logger.debug("finished_decompression")
            #write_output the output output
            if isinstance(output, bytes):
                l = len(output)
                decomp_header = copy.copy(header)
                decomp_header.size = l
                write_output(decomp_header.tobuf(), id="bytes_decompressed_header")
                write_output(output, id="bytes_decompressed_data")
                write_output(b" " * to_fill_block(l, tarfile.BLOCKSIZE), id="bytes_decompressed_padding")
            elif isinstance(output, np.ndarray):
                output_bytes = output.tobytes()
                l = len(output_bytes)
                decomp_header = copy.copy(header)
                decomp_header.size = l
                logger.debug("returning %s %s %s", decomp_header.get_info(), output.shape, output.dtype)
                write_output(decomp_header.tobuf(), id="ndarray_decompressed_header")
                write_output(output_bytes, id="ndarray_decompressed_data")
                write_output(b" " * to_fill_block(l, tarfile.BLOCKSIZE), id="ndarray_decompressed_padding")
            else:
                logger.debug("%s %s compression did not produce bytes falling back to full data")
                write_output(full_data, id="failed_data")

        else:
            write_output(header_bytes, id="nonfile_header")
            to_read = round_up(header.size, tarfile.BLOCKSIZE)
            while to_read > 0:
                read = read_input(to_read, id="nonfile_data")
                to_read -= len(read)
                write_output(read)
            # no need to pad output here because we round up before writing

def compress():
    while True:
        header_bytes = read_input(tarfile.BLOCKSIZE, id="header")
        try:
            header = tarfile.TarInfo.frombuf(header_bytes, tarfile.ENCODING, "surrogateescape")
        except tarfile.HeaderError:
            # this should be the eof header entry
            write_output(header_bytes, id="eof header")
            break
        logger.debug("decoded %s %s", io_index, header.get_info())
        if header.isfile():
            # determine configuration
            if header.name in schema['override']:
                file_schema = schema['override'][header.name]
                dtype = file_schema.get('data_schema',
                                        default_schema['data_schema']).get("dtype",
                                                                           default_schema['data_schema']['dtype'])
                dims = file_schema.get('data_schema',
                                        default_schema['data_schema']).get("dims",
                                                                           default_schema['data_schema']['dims'])
                config = file_schema.get("compressor", default_schema['compressor'])
            else:
                dtype = default_schema['data_schema']['dtype']
                dims = default_schema['data_schema']['dims']
                config = default_schema['compressor']
                
            #read raw data from archive
            remaining = round_up(header.size, tarfile.BLOCKSIZE)
            data_parts = []
            while remaining > 0:
                logger.info("READING %s remaining=%s", header.name, remaining)
                data = read_input(remaining, id="file_data")
                remaining -= len(data)
                data_parts.append(data)

            #create an input object for compression
            full_data = b"".join(data_parts)
            full_data = full_data[:header.size]
            full_data_arr = np.frombuffer(full_data, dtype=to_dtype(dtype))
            if dims is not None:
                full_data_arr = full_data_arr.reshape(dims)

            #run the compressor
            logger.debug("%s %s %s %s %s %s", header.name, dims, dtype, full_data_arr.shape, full_data_arr.dtype, config)
            compressor = lp.PressioCompressor.from_config(config)
            compressed = compressor.encode(full_data_arr)

            #write_output the output output
            if isinstance(compressed, bytes):
                # TODO roundout to a full blocksize
                l = len(compressed)
                comp_header = copy.copy(header)
                comp_header.size = l
                logger.debug("returning %s", comp_header.get_info())
                write_output(comp_header.tobuf(), id="bytes_header")
                write_output(compressed, id="bytes_compressed_data")
                write_output(b" " * to_fill_block(l, tarfile.BLOCKSIZE), id="bytes_data_padding")
            elif isinstance(compressed, np.ndarray):
                # TODO roundout to a full blocksize
                compressed = compressed.tobytes()
                l = len(compressed)
                comp_header = copy.copy(header)
                comp_header.size = l
                write_output(comp_header.tobuf(), id="ndarray_compressed_header")
                write_output(compressed, id="ndarray_compressed_data")
                write_output(b" " * to_fill_block(l, tarfile.BLOCKSIZE), id="ndarray_data_padding")
            else:
                logger.warning("%s %s compression did not produce bytes falling back to full data", header.name, dims)
                write_output(full_data, id="failed_compression")

        else:
            write_output(header_bytes, id="nonfile_header_bytes")
            to_read = round_up(header.size, tarfile.BLOCKSIZE)
            while to_read > 0:
                read = read_input(to_read, id="nonfile_data")
                to_read -= len(read)
                write_output(read, id="nonfile_data")
            # no need to pad here because we rounded up


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--decompress", help="run decompression on the tarfile", action="store_false", dest="compress")
    args = parser.parse_args()
    logger.debug("%s", args)
    if args.compress:
        compress()
    else:
        decompress()

    
