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
use fiona

call fiona_init()
call fiona_create_dataset('grids', file_path=grids_file_path, mode='r')
call fiona_get_dim('grids', 'location_nv', size=nx) ! Get dimension size in one line!
...
call fiona_start_input('grids')
call fiona_input('grids', 'vtx_p', x)
call fiona_end_input('grids')
...
```

For output:

```fortran
use fiona

call fiona_create_dataset('mpas_mesh', file_path='mpas_mesh.' // trim(to_string(mesh%nCells)) // '.nc')
call fiona_add_att('mpas_mesh', 'on_a_sphere',   'YES')
call fiona_add_att('mpas_mesh', 'sphere_radius', 1.0d0)
...
call fiona_add_dim('mpas_mesh', 'nCells',    size=mesh%nCells)
call fiona_add_dim('mpas_mesh', 'nEdges',    size=mesh%nEdges)
call fiona_add_dim('mpas_mesh', 'nVertices', size=mesh%nVertices)
...
call fiona_add_var('mpas_mesh', 'latCell',   long_name='Latitude of all cell centers',        units='radian', dim_names=['nCells'], data_type='real(8)')
call fiona_add_var('mpas_mesh', 'lonCell',   long_name='Longitude of all cell centers',       units='radian', dim_names=['nCells'], data_type='real(8)')
call fiona_add_var('mpas_mesh', 'xCell',     long_name='x axis position of all cell centers', units='m',      dim_names=['nCells'], data_type='real(8)')
call fiona_add_var('mpas_mesh', 'yCell',     long_name='y axis position of all cell centers', units='m',      dim_names=['nCells'], data_type='real(8)')
call fiona_add_var('mpas_mesh', 'zCell',     long_name='z axis position of all cell centers', units='m',      dim_names=['nCells'], data_type='real(8)')
...
call fiona_start_output('mpas_mesh')
call fiona_output('mpas_mesh', 'latCell',    mesh%latCell)
call fiona_output('mpas_mesh', 'lonCell',    mesh%lonCell)
call fiona_output('mpas_mesh', 'xCell',      mesh%xCell)
call fiona_output('mpas_mesh', 'yCell',      mesh%yCell)
call fiona_output('mpas_mesh', 'zCell',      mesh%zCell)
...
call fiona_end_output('mpas_mesh')
```

For parallel input:

```fortran
use fiona

character(...) paths(...)
integer num_x, num_y
integer time_start_idx, time_end_idx, time_idx
real(8), allocatable :: f(:,:,:) ! with dimensions x, y, time (the variant dimension of the dataset bundle is usually time.)

! Set sorted paths.

call fiona_init(mpi_comm)

call fiona_create_dataset('h0', file_paths=paths, mode='r', parallel=.true.)
call fiona_get_dim('h0', 'x', size=num_x)
call fiona_get_dim('h0', 'y', size=num_y)
call fiona_get_dim('h0', 'time', start=time_start_idx, end=time_end_idx)
allocate(f(num_x,num_y,time_start_idx:time_end_idx))
call fiona_start_input('h0')
! Read time series (maybe partial of the file), each process reads its assigned partition.
do time_idx = time_start_idx, time_end_idx
  call fiona_input('h0', 'f', f(:,:,time_idx), variant_dim_idx=time_idx, ...)
end do
call fiona_end_input('h0')
! By now, each process should already get data for its partition.
! Do some calculation, and reduce the results to root process or all processes.
```

It is your turn to simplify your callings on netCDF!
