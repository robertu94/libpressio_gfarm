# Example Data filter for LibPressio and gfptar and gfarm

## Installation

This code requires the following spack packages

```
spack install libpressio+python+blosc ^ python@3.12
```

## Configuration

GFarm doesn't have a way to natively pass metadata including compression parameters or data descriptors to the underlying compressor.
So we need to create our own method.   For this, we utilize the envionmental variable `"LIBPRESSIO_COMPRESS_SCHEMAFILE`.  This variable
has the path to a file containing this information.  In order to decompress the data later, you must communicate this file via an alternative method.

Here is the schema for this file:


+ `default.compressor` a libpressio configuration for the compressor to use when the file is unknown.
+ `default.data_schmea.dtype` the name for the data type in libpressio for the data
+ `default.data_schmea.dims` the dimensions for the data in C order (this is consistent with python, but not the default for LibPressio)
+ `override.$filepath.compressor` a libpressio configuration for the compressor to use for the file `$filepath`
+ `override.$filepath.data_schmea.dtype` the name for the data type in libpressio for the data for the file `$filepath`
+ `override.$filepath.data_schmea.dims` the dimensions for the data in C order (this is consistent with python, but not the default for LibPressio) for the file `$filepath`

If a corresponding override is not provided, configuration falls back to the 


## Known limitations

+ the python 3.12 requirement is for pathlib.Path.walk,
+ the blosc requirment is derived from the DEFAULT_SCHEMA
+ other formats such as images, HDF5 files, netcdf files are not supported for now.
+ there is a bug with the "noop" compressor resulting in an error message because it returns a non-byte type output, but lossless compressors such as blosc are supported
+ GPU based compressors are not supported for now
