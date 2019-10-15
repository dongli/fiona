program fiona_test

  use fiona

  implicit none
    
  real(8) f(100,100)

  call fiona_init()

  call fiona_create_dataset('test', file_path='test.nc', mode='output')
  call fiona_add_dim('test', 'x', long_name='x', units='m', size=100, add_var=.true.)
  call fiona_add_dim('test', 'y', long_name='y', units='m', size=100, add_var=.true.)
  call fiona_add_var('test', 'f', long_name='f', units='1', dim_names=['x', 'y'], data_type='real(8)')
  call fiona_start_output('test')
  call fiona_output('test', 'f', f)
  call fiona_end_output('test')

end program fiona_test
