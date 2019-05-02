# Introduction

FIONA stands for Fortran IO Netcdf Assembly, which encapsulates netCDF library for easy use.

# Dependencies

- NetCDF

  You need to install netCDF by yourself and by any means (e.g. [STARMAN](https://github.com/dongli/starman))

- [fortran-container](https://github.com/dongli/fortran-container)

  It can be added to your project as another submodule.
  
```
$ git submodule add https://github.com/dongli/fortran-container lib/container
```

In your `CMakeLists.txt`:
```cmake
add_subdirectory(lib/container)
include_directories(${CMAKE_BINARY_DIR}/fortran_container) # Not sure why we need this.
...
target_link_libraries(... fortran_container)
```

# Usage

## Step 1

Add FIONA as a submodule in you git project:

```
$ git submodule add https://github.com/dongli/fiona lib/fiona
```

## Step 2

Add the following line to your `CMakeLists.txt`:
```cmake
add_subdirectory(lib/fiona)
```

Then link your executable or library with `fiona`:

```cmake
target_link_libraries(... fiona)
```

You are good to build your project.

## Step 3

For input, use the following paradigm:

```fortran
use io_mod

call io_init()
call io_create_dataset(name='grids', file_path=grids_file_path, mode='input')
call io_get_dim('location_nv', dataset_name='grids', size=nx)
...
call io_start_input('grids')
call io_input('vtx_p', x, dataset_name='grids')
call io_end_input('grids')
...
```

For output:

```fortran
call io_create_dataset(name='mpas_mesh', file_path='mpas_mesh.' // trim(to_string(mesh%nCells)) // '.nc')
call io_add_att('on_a_sphere',   'YES',              dataset_name='mpas_mesh')
call io_add_att('sphere_radius', 1.0d0,              dataset_name='mpas_mesh')
...
call io_add_dim('nCells',       size=mesh%nCells,    dataset_name='mpas_mesh')
call io_add_dim('nEdges',       size=mesh%nEdges,    dataset_name='mpas_mesh')
call io_add_dim('nVertices',    size=mesh%nVertices, dataset_name='mpas_mesh')
...
call io_add_var('latCell',      long_name='Latitude of all cell centers',        units='radian', dim_names=['nCells'], data_type='real(8)', dataset_name='mpas_mesh')
call io_add_var('lonCell',      long_name='Longitude of all cell centers',       units='radian', dim_names=['nCells'], data_type='real(8)', dataset_name='mpas_mesh')
call io_add_var('xCell',        long_name='x axis position of all cell centers', units='m',      dim_names=['nCells'], data_type='real(8)', dataset_name='mpas_mesh')
call io_add_var('yCell',        long_name='y axis position of all cell centers', units='m',      dim_names=['nCells'], data_type='real(8)', dataset_name='mpas_mesh')
call io_add_var('zCell',        long_name='z axis position of all cell centers', units='m',      dim_names=['nCells'], data_type='real(8)', dataset_name='mpas_mesh')
call io_output('latCell',       mesh%latCell,        dataset_name='mpas_mesh')
call io_output('lonCell',       mesh%lonCell,        dataset_name='mpas_mesh')
call io_output('xCell',         mesh%xCell,          dataset_name='mpas_mesh')
call io_output('yCell',         mesh%yCell,          dataset_name='mpas_mesh')
call io_output('zCell',         mesh%zCell,          dataset_name='mpas_mesh')
...
call io_end_output(dataset_name='mpas_mesh')
```

It is your turn to simplify your callings on netCDF!
