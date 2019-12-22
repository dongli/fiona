program fiona_test

  use fiona
  use unit_test
#ifdef HAS_MPI
  use mpi
#endif

  implicit none

  type(test_suite_type) test_suite

  real(8) xi(10)
  real(8) yi(10)
  real(8) fi(10,10)
  real(8) xo(10)
  real(8) yo(10)
  real(8) fo(10,10)

  integer ierr
  integer :: proc_id = 0

#ifdef HAS_MPI
  call MPI_INIT(ierr)
  
  call MPI_COMM_RANK(MPI_COMM_WORLD, proc_id, ierr)
#endif

  call fiona_init()

  if (proc_id == 0) then
    call test_suite_init('Test IO', test_suite)

    call test_output()
    call test_input()
    call test_quick_output()
  end if

  ! call test_parallel_input()

  if (proc_id == 0) then
    call test_suite_report(test_suite)
    call test_suite_final(test_suite)
  end if

#ifdef HAS_MPI
  call MPI_FINALIZE(ierr)
#endif

contains

  subroutine test_output()

    integer i, j

    call test_case_create('Output', test_suite)

    xi = [(i * 0.1, i = 1, size(xi))]
    yi = [(j * 0.1, j = 1, size(yi))]

    do j = 1, size(yi)
      do i = 1, size(xi)
        fi(i,j) = sin(xi(i) * yi(i))
      end do
    end do

    call fiona_create_dataset('t0', file_path='test.nc', mode='w', mute=.true.)
    call fiona_add_dim('t0', 'x', long_name='x', units='m', size=size(xi), add_var=.true.)
    call fiona_add_dim('t0', 'y', long_name='y', units='m', size=size(yi), add_var=.true.)
    call fiona_add_var('t0', 'f', long_name='f', units='1', dim_names=['x', 'y'], data_type='r8')
    call fiona_start_output('t0')
    call fiona_output('t0', 'x', xi)
    call fiona_output('t0', 'y', yi)
    call fiona_output('t0', 'f', fi)
    call fiona_end_output('t0')

  end subroutine test_output

  subroutine test_input()

    integer nx, ny

    call test_case_create('Input', test_suite)

    call fiona_create_dataset('t0', file_path='test.nc', mode='r', mute=.true.)
    call fiona_get_dim('t0', 'x', size=nx)
    call assert_equal(nx, size(xi), __FILE__, __LINE__, test_suite)
    call fiona_get_dim('t0', 'y', size=ny)
    call assert_equal(ny, size(yi), __FILE__, __LINE__, test_suite)
    call fiona_start_input('t0')
    call fiona_input('t0', 'x', xo)
    call assert_equal(xo, xi, __FILE__, __LINE__, test_suite)
    call fiona_input('t0', 'y', yo)
    call assert_equal(yo, yi, __FILE__, __LINE__, test_suite)
    call fiona_input('t0', 'f', fo)
    call assert_equal(fo, fi, __FILE__, __LINE__, test_suite)
    call fiona_end_input('t0')

  end subroutine test_input

  subroutine test_quick_output()

    integer nx, ny

    call test_case_create('Quick Output', test_suite)

    call fiona_quick_output('t1', 'x', ['x'], xi, file_prefix='test_quick')
    call fiona_quick_output('t1', 'y', ['y'], yi, file_prefix='test_quick')
    call fiona_quick_output('t1', 'f', ['x', 'y'], fi, file_prefix='test_quick')
    call fiona_quick_output('t1', 'g', ['x', 'y'], fi, file_prefix='test_quick')

    call fiona_create_dataset('t1', file_path='test_quick.t1.nc', mode='r', mute=.true.)
    call fiona_get_dim('t1', 'x', size=nx)
    call assert_equal(nx, size(xi), __FILE__, __LINE__, test_suite)
    call fiona_get_dim('t1', 'y', size=ny)
    call assert_equal(ny, size(yi), __FILE__, __LINE__, test_suite)
    call fiona_start_input('t1')
    call fiona_input('t1', 'x', xo)
    call assert_equal(xo, xi, __FILE__, __LINE__, test_suite)
    call fiona_input('t1', 'y', yo)
    call assert_equal(yo, yi, __FILE__, __LINE__, test_suite)
    call fiona_input('t1', 'f', fo)
    call assert_equal(fo, fi, __FILE__, __LINE__, test_suite)
    call fiona_input('t1', 'g', fo)
    call assert_equal(fo, fi, __FILE__, __LINE__, test_suite)
    call fiona_end_input('t1')

  end subroutine test_quick_output

  subroutine test_parallel_input()

#ifdef HAS_MPI
    character(256) file_paths(8016)
    integer num_x, num_y, i, start(2), count(2)
    integer time_idx, time_start_idx, time_end_idx
    real, allocatable :: u(:,:,:), v(:,:,:)

    open(10, file='filelist', status='old')
    do i = 1, size(file_paths)
      read(10, '(A)') file_paths(i)
    end do
    close(10)
    call fiona_open_dataset('p0', file_paths, parallel=.true., mpi_comm=MPI_COMM_WORLD)
    call fiona_get_dim('p0', 'x', size=num_x)
    call fiona_get_dim('p0', 'y', size=num_y)
    call fiona_get_dim('p0', 'time', start_idx=time_start_idx, end_idx=time_end_idx)
    allocate(u(2,2,time_start_idx:time_end_idx))
    allocate(v(2,2,time_start_idx:time_end_idx))
    start = [200,200]
    count = [2,2]
    do time_idx = time_start_idx, time_end_idx
      call fiona_start_input('p0', file_idx=time_idx)
      call fiona_input('p0', 'u', u(:,:,time_idx), start=start, count=count)
      call fiona_input('p0', 'v', v(:,:,time_idx), start=start, count=count)
      call fiona_end_input('p0')
    end do
    deallocate(u)
    deallocate(v)
#endif

  end subroutine test_parallel_input

end program fiona_test
