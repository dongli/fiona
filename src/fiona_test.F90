program fiona_test

  use fiona
  use unit_test

  implicit none

  type(test_suite_type) test_suite

  real(8) xi(10)
  real(8) yi(10)
  real(8) fi(10,10)
  real(8) xo(10)
  real(8) yo(10)
  real(8) fo(10,10)

  call test_suite_init('Test IO', test_suite)

  call test_output()
  call test_input()

  call test_suite_report(test_suite)
  call test_suite_final(test_suite)

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

    call fiona_init()

    call fiona_create_dataset('test', file_path='test.nc', mode='w')
    call fiona_add_dim('test', 'x', long_name='x', units='m', size=size(xi), add_var=.true.)
    call fiona_add_dim('test', 'y', long_name='y', units='m', size=size(yi), add_var=.true.)
    call fiona_add_var('test', 'f', long_name='f', units='1', dim_names=['x', 'y'], data_type='real(8)')
    call fiona_start_output('test')
    call fiona_output('test', 'x', xi)
    call fiona_output('test', 'y', yi)
    call fiona_output('test', 'f', fi)
    call fiona_end_output('test')

  end subroutine test_output

  subroutine test_input()

    integer nx, ny

    call test_case_create('Input', test_suite)

    call fiona_create_dataset('test', file_path='test.nc', mode='r')
    call fiona_get_dim('test', 'x', size=nx)
    call assert_equal(nx, size(xi), __FILE__, __LINE__, test_suite)
    call fiona_get_dim('test', 'y', size=ny)
    call assert_equal(ny, size(yi), __FILE__, __LINE__, test_suite)
    call fiona_start_input('test')
    call fiona_input('test', 'x', xo)
    call assert_equal(xo, xi, __FILE__, __LINE__, test_suite)
    call fiona_input('test', 'y', yo)
    call assert_equal(yo, yi, __FILE__, __LINE__, test_suite)
    call fiona_input('test', 'f', fo)
    call assert_equal(fo, fi, __FILE__, __LINE__, test_suite)
    call fiona_end_input('test')

  end subroutine test_input

end program fiona_test
