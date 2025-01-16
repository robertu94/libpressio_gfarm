# Example Data filter for LibPressio and gfptar and gfarm

## Installation

This code requires the following spack packages

```
spack install libpressio+python+blosc ^ python@3.12
```

## Configuration

GFarm and TAR doesn't have a way to natively pass metadata including compression parameters or data descriptors to the underlying compressor.
So we need to create our own method.   For this, we utilize the environmental variable `"LIBPRESSIO_COMPRESS_SCHEMAFILE`.  This variable
has the path to a file containing this information.  In order to decompress the data later, you must communicate this file via an alternative method.

Here is the schema for this file:


+ `default.compressor` a libpressio configuration for the compressor to use when the file is unknown.
+ `default.data_schmea.dtype` the name for the data type in libpressio for the data
+ `default.data_schmea.dims` the dimensions for the data in C order (this is consistent with python, but not the default for LibPressio)
+ `override.$filepath.compressor` a libpressio configuration for the compressor to use for the file `$filepath`
+ `override.$filepath.data_schmea.dtype` the name for the data type in libpressio for the data for the file `$filepath`
+ `override.$filepath.data_schmea.dims` the dimensions for the data in C order (this is consistent with python, but not the default for LibPressio) for the file `$filepath`

If a corresponding override is not provided, configuration falls back to the  default.

Compressors should look like this:

```json
{
"compressor_id": "noop",
"compressor_config": {},
"early_config": {}
}
```

## Known limitations

+ only files in raw binary formats are supported for now other formats such as images, HDF5 files, netcdf files are not supported.
