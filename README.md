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
call io_create_dataset('grids', file_path=grids_file_path, mode='input')
call io_get_dim('grids', 'location_nv', size=nx) ! Get dimension size in one line!
...
call io_start_input('grids')
call io_input('grids', 'vtx_p', x)
call io_end_input('grids')
...
```

For output:

```fortran
call io_create_dataset('mpas_mesh', file_path='mpas_mesh.' // trim(to_string(mesh%nCells)) // '.nc')
call io_add_att('mpas_mesh', 'on_a_sphere',   'YES')
call io_add_att('mpas_mesh', 'sphere_radius', 1.0d0)
...
call io_add_dim('mpas_mesh', 'nCells',    size=mesh%nCells)
call io_add_dim('mpas_mesh', 'nEdges',    size=mesh%nEdges)
call io_add_dim('mpas_mesh', 'nVertices', size=mesh%nVertices)
...
call io_add_var('mpas_mesh', 'latCell',   long_name='Latitude of all cell centers',        units='radian', dim_names=['nCells'], data_type='real(8)')
call io_add_var('mpas_mesh', 'lonCell',   long_name='Longitude of all cell centers',       units='radian', dim_names=['nCells'], data_type='real(8)')
call io_add_var('mpas_mesh', 'xCell',     long_name='x axis position of all cell centers', units='m',      dim_names=['nCells'], data_type='real(8)')
call io_add_var('mpas_mesh', 'yCell',     long_name='y axis position of all cell centers', units='m',      dim_names=['nCells'], data_type='real(8)')
call io_add_var('mpas_mesh', 'zCell',     long_name='z axis position of all cell centers', units='m',      dim_names=['nCells'], data_type='real(8)')
...
call io_output('mpas_mesh', 'latCell',    mesh%latCell)
call io_output('mpas_mesh', 'lonCell',    mesh%lonCell)
call io_output('mpas_mesh', 'xCell',      mesh%xCell)
call io_output('mpas_mesh', 'yCell',      mesh%yCell)
call io_output('mpas_mesh', 'zCell',      mesh%zCell)
...
call io_end_output(dataset_name='mpas_mesh')
```

It is your turn to simplify your callings on netCDF!
