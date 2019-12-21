module fiona_mod

  use netcdf
  use flogger
  use string
  use hash_table_mod

  implicit none

  private

  public fiona_init
  public fiona_create_dataset
  public fiona_add_att
  public fiona_add_dim
  public fiona_add_var
  public fiona_has_var
  public fiona_start_output
  public fiona_output
  public fiona_end_output
  public fiona_start_input
  public fiona_get_dim
  public fiona_get_att
  public fiona_input
  public fiona_end_input

  type dataset_type
    integer :: id = -1
    character(30) name
    character(256) :: desc = 'N/A'
    character(256) :: author = 'N/A'
    character(256) :: file_path = 'N/A'
    character(256) :: file_prefix = 'N/A'
    character(10) mode
    character(256) last_file_path
    type(var_type), pointer :: time_var => null()
    type(hash_table_type) atts
    type(hash_table_type) dims
    type(hash_table_type) vars
    integer :: time_step = 0
  contains
    procedure :: get_dim => get_dim_from_dataset
    procedure :: get_var => get_var_from_dataset
  end type dataset_type

  type dim_type
    integer id
    character(30) name
    character(256) long_name
    character(60) units
    integer size
  end type dim_type

  type var_dim_type
    type(dim_type), pointer :: ptr => null()
  end type var_dim_type

  type var_type
    integer id
    character(30) name
    character(256) long_name
    character(60) units
    integer data_type
    class(*), pointer :: missing_value => null()
    type(var_dim_type), allocatable :: dims(:)
  contains
    final :: var_final
  end type var_type

  type(hash_table_type) datasets
  real(8) time_units_in_seconds

  interface fiona_output
    module procedure fiona_output_0d
    module procedure fiona_output_1d
    module procedure fiona_output_2d
    module procedure fiona_output_3d
    module procedure fiona_output_4d
    module procedure fiona_output_5d
  end interface fiona_output

  interface fiona_get_att
    module procedure fiona_get_att_str
    module procedure fiona_get_att_i4
    module procedure fiona_get_att_i8
    module procedure fiona_get_att_r4
    module procedure fiona_get_att_r8
    module procedure fiona_get_var_att_str
  end interface fiona_get_att

  interface fiona_input
    module procedure fiona_input_0d
    module procedure fiona_input_1d
    module procedure fiona_input_2d
    module procedure fiona_input_3d
    module procedure fiona_input_4d
    module procedure fiona_input_5d
  end interface fiona_input

  character(30) time_units_str
  character(30) start_time_str

contains

  subroutine fiona_init(time_units, start_time)

    character(*), intent(in), optional :: time_units
    character(*), intent(in), optional :: start_time

    logical, save :: called = .false.

    if (called) return
    called = .true.

    if (present(time_units)) then
      time_units_str = time_units
      select case (time_units)
      case ('days')
        time_units_in_seconds = 86400.0
      case ('hours')
        time_units_in_seconds = 3600.0
      case ('minutes')
        time_units_in_seconds = 60.0
      case ('seconds')
        time_units_in_seconds = 1.0
      case default
        call log_error('Invalid time_units ' // trim(time_units) // '!')
      end select
    end if

    if (present(start_time)) then
      start_time_str = start_time
    end if

    datasets = hash_table()

    call log_notice('IO module is initialized.')

  end subroutine fiona_init

  subroutine fiona_create_dataset(dataset_name, desc, file_prefix, file_path, mode, mute)

    character(*), intent(in) :: dataset_name
    character(*), intent(in), optional :: desc
    character(*), intent(in), optional :: file_prefix
    character(*), intent(in), optional :: file_path
    character(*), intent(in), optional :: mode
    logical, intent(in), optional :: mute

    character(10) mode_
    character(256) desc_, file_prefix_, file_path_
    type(dataset_type) dataset
    logical is_exist
    integer i

    if (present(desc)) then
      desc_ = desc
    else
      desc_ = 'N/A'
    end if
    if (present(file_prefix)) then
      file_prefix_ = file_prefix
    else
      file_prefix_ = ''
    end if
    if (present(file_path)) then
      file_path_ = file_path
    else
      file_path_ = ''
    end if
    if (present(mode)) then
      mode_ = mode
    else
      mode_ = 'output'
    end if
    if (mode_ == 'r') mode_ = 'input'
    if (mode_ == 'w') mode_ = 'output'

    if (mode_ == 'input') then
      inquire(file=file_path_, exist=is_exist)
      if (.not. is_exist) then
        call log_error('fiona_create_dataset: Input file "' // trim(file_path_) // '" does not exist!')
      end if
    end if

    if (datasets%hashed(dataset_name)) then
      call log_error('Already created dataset ' // trim(dataset_name) // '!')
    end if

    dataset%name = dataset_name
    dataset%desc = desc_
    dataset%atts = hash_table()
    dataset%dims = hash_table()
    dataset%vars = hash_table()
    if (file_prefix_ /= '' .and. file_path_ == '') then
      dataset%file_prefix = trim(file_prefix_) // '.' // trim(dataset_name)
    else if (file_prefix_ == '' .and. file_path_ /= '') then
      dataset%file_path = file_path_
    end if
    dataset%mode = mode_

    call datasets%insert(trim(dataset%name) // '.' // trim(dataset%mode), dataset)

    if (present(mute)) then
      if (mute) return
    end if
    if (dataset%file_path /= 'N/A') then
      call log_notice('Create ' // trim(dataset%mode) // ' dataset ' // trim(dataset%file_path) // '.')
    else if (dataset%file_prefix /= 'N/A') then
      call log_notice('Create ' // trim(dataset%mode) // ' dataset ' // trim(dataset%file_prefix) // '.')
    end if

  end subroutine fiona_create_dataset

  subroutine fiona_add_att(dataset_name, name, value)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*), intent(in) :: value

    type(dataset_type), pointer :: dataset

    dataset => get_dataset(dataset_name, mode='output')

    call dataset%atts%insert(name, value)

  end subroutine fiona_add_att

  subroutine fiona_add_dim(dataset_name, name, long_name, units, size, add_var)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    character(*), intent(in), optional :: long_name
    character(*), intent(in), optional :: units
    integer, intent(in), optional :: size
    logical, intent(in), optional :: add_var

    type(dataset_type), pointer :: dataset
    type(dim_type) dim

    dataset => get_dataset(dataset_name, mode='output')

    if (dataset%dims%hashed(name)) then
      call log_error('Already added dimension ' // trim(name) // ' in dataset ' // trim(dataset%name) // '!')
    end if

    dim%name = name
    if (.not. present(size)) then
      dim%size = NF90_UNLIMITED
    else
      dim%size = size
    end if

    call dataset%dims%insert(name, dim)

    if (present(add_var)) then
      if (add_var) then
        ! Add corresponding dimension variable.
        if (.not. present(long_name)) then
          select case (name)
          case ('lon', 'ilon')
            dim%long_name = 'Longitude'
          case ('lat', 'ilat')
            dim%long_name = 'Latitude'
          case ('time', 'Time')
            dim%long_name = 'Time'
          end select
        else
          dim%long_name = long_name
        end if
        if (.not. present(units)) then
          select case (name)
          case ('lon', 'ilon')
            dim%units = 'degrees_east'
          case ('lat', 'ilat')
            dim%units = 'degrees_north'
          case ('time', 'Time')
            write(dim%units, '(A, " since ", A)') trim(time_units_str), trim(start_time_str)
          end select
        else
          dim%units = units
        end if
        call fiona_add_var(dataset_name, name, long_name=dim%long_name, units=dim%units, dim_names=[name], data_type='real(8)')
      end if
    end if

  end subroutine fiona_add_dim

  subroutine fiona_add_var(dataset_name, name, long_name, units, dim_names, data_type, missing_value)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    character(*), intent(in) :: long_name
    character(*), intent(in) :: units
    character(*), intent(in) :: dim_names(:)
    character(*), intent(in), optional :: data_type
    class(*), intent(in), optional :: missing_value

    type(dataset_type), pointer :: dataset
    type(var_type) :: var
    type(hash_table_iterator_type) iter
    type(dim_type), pointer :: dim
    integer i
    logical found
    real real

    dataset => get_dataset(dataset_name, mode='output')

    if (dataset%vars%hashed(name)) then
      call log_error('Already added variable ' // trim(name) // ' in dataset ' // trim(dataset%name) // '!')
    end if

    var%name = name
    var%long_name = long_name
    var%units = units

    if (.not. present(data_type)) then
      select case (sizeof(real))
      case (4)
        var%data_type = NF90_FLOAT
      case (8)
        var%data_type = NF90_DOUBLE
      end select
    else
      select case (data_type)
      case ('real', 'real(4)')
        var%data_type = NF90_FLOAT
      case ('real(8)')
        var%data_type = NF90_DOUBLE
      case ('integer', 'integer(4)')
        var%data_type = NF90_INT
      case ('integer(8)')
        var%data_type = NF90_INT64
      case default
        call log_error('Unknown data type ' // trim(data_type) // ' for variable ' // trim(name) // '!')
      end select
    end if

    if (present(missing_value)) then
      allocate(var%missing_value, source=missing_value)
    end if

    allocate(var%dims(size(dim_names)))
    do i = 1, size(dim_names)
      found = .false.
      iter = hash_table_iterator(dataset%dims)
      do while (.not. iter%ended())
        dim => dataset%get_dim(iter%key)
        if (dim%name == trim(dim_names(i))) then
          var%dims(i)%ptr => dim
          found = .true.
          exit
        end if
        call iter%next()
      end do
      if (.not. found) then
        call log_error('Unknown dimension ' // trim(dim_names(i)) // ' for variable ' // trim(name) // '!')
      end if
    end do

    call dataset%vars%insert(name, var)

    if (name == 'Time' .or. name == 'time') dataset%time_var => dataset%get_var(name)

  end subroutine fiona_add_var

  logical function fiona_has_var(dataset_name, var_name) result(res)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: var_name

    type(dataset_type), pointer :: dataset
    integer ierr, varid

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_INQ_VARID(dataset%id, var_name, varid)
    res = ierr == NF90_NOERR

  end function fiona_has_var

  subroutine fiona_start_output(dataset_name, time_in_seconds, new_file, tag)

    character(*), intent(in) :: dataset_name
    real(8), intent(in), optional :: time_in_seconds
    logical, intent(in), optional :: new_file
    character(*), intent(in), optional :: tag

    character(256) file_path
    type(dataset_type), pointer :: dataset
    type(hash_table_iterator_type) iter
    type(dim_type), pointer :: dim
    type(var_type), pointer :: var
    logical new_file_
    integer i, ierr, dimids(10)

    dataset => get_dataset(dataset_name, mode='output')

    if (present(new_file)) then
      new_file_ = new_file
    else
      new_file_ = .true.
    end if

    if (present(tag)) then
      if (dataset%file_path /= 'N/A') then
        file_path = trim(delete_string(dataset%file_path, '.nc')) // '.' // trim(tag) // '.nc'
      else
        write(file_path, "(A, '.', A, '.nc')") trim(dataset%file_prefix), trim(tag)
      end if
    else
      if (dataset%file_path /= 'N/A') then
        file_path = dataset%file_path
      else
        write(file_path, "(A, '.nc')") trim(dataset%file_prefix)
      end if
    end if

    call apply_dataset_to_netcdf(file_path, dataset, dataset%time_step == 0 .or. new_file_)
    
    ! Write time dimension variable.
    if (associated(dataset%time_var)) then
      if (.not. present(time_in_seconds)) then
        call log_error('Time in seconds is needed!', __FILE__, __LINE__)
      end if
      dataset%time_step = dataset%time_step + 1
      ! Update time units because restart may change it.
      write(dataset%time_var%units, '(A, " since ", A)') trim(time_units_str), trim(start_time_str)
      ierr = NF90_PUT_ATT(dataset%id, dataset%time_var%id, 'units', trim(dataset%time_var%units))
      call handle_error(ierr, 'Failed to add attribute to variable time!', __FILE__, __LINE__)
      ierr = NF90_PUT_VAR(dataset%id, dataset%time_var%id, [time_in_seconds / time_units_in_seconds], [dataset%time_step], [1])
      call handle_error(ierr, 'Failed to write variable time!', __FILE__, __LINE__)
    end if

  end subroutine fiona_start_output

  subroutine apply_dataset_to_netcdf(file_path, dataset, new_file)

    character(*), intent(in) :: file_path
    type(dataset_type), intent(inout) :: dataset
    logical, intent(in) :: new_file

    type(hash_table_iterator_type) iter
    type(dim_type), pointer :: dim
    type(var_type), pointer :: var
    integer i, ierr, dimids(10)

    if (new_file) then
      ierr = NF90_CREATE(file_path, NF90_CLOBBER + NF90_64BIT_OFFSET, dataset%id)
      call handle_error(ierr, 'Failed to create NetCDF file to output!', __FILE__, __LINE__)
    else
      ierr = NF90_OPEN(dataset%last_file_path, NF90_WRITE + NF90_64BIT_OFFSET, dataset%id)
      call handle_error(ierr, 'Failed to open NetCDF file to output! ' // trim(NF90_STRERROR(ierr)), __FILE__, __LINE__)
      ierr = NF90_REDEF(dataset%id)
      call handle_error(ierr, 'Failed to enter definition mode!', __FILE__, __LINE__)
    end if
    ierr = NF90_PUT_ATT(dataset%id, NF90_GLOBAL, 'dataset', dataset%name)
    ierr = NF90_PUT_ATT(dataset%id, NF90_GLOBAL, 'desc', dataset%desc)
    ierr = NF90_PUT_ATT(dataset%id, NF90_GLOBAL, 'author', dataset%author)

    iter = hash_table_iterator(dataset%atts)
    do while (.not. iter%ended())
      select type (value => iter%value)
      type is (integer)
        ierr = NF90_PUT_ATT(dataset%id, NF90_GLOBAL, iter%key, value)
      type is (real(4))
        ierr = NF90_PUT_ATT(dataset%id, NF90_GLOBAL, iter%key, value)
      type is (real(8))
        ierr = NF90_PUT_ATT(dataset%id, NF90_GLOBAL, iter%key, value)
      type is (character(*))
        ierr = NF90_PUT_ATT(dataset%id, NF90_GLOBAL, iter%key, value)
      type is (logical)
        ierr = NF90_PUT_ATT(dataset%id, NF90_GLOBAL, iter%key, to_string(value))
      end select
      call iter%next()
    end do

    iter = hash_table_iterator(dataset%dims)
    do while (.not. iter%ended())
      dim => dataset%get_dim(iter%key)
      ierr = NF90_DEF_DIM(dataset%id, dim%name, dim%size, dim%id)
      call handle_error(ierr, 'Failed to define dimension ' // trim(dim%name) // '!', __FILE__, __LINE__)
      call iter%next()
    end do

    iter = hash_table_iterator(dataset%vars)
    do while (.not. iter%ended())
      var => dataset%get_var(iter%key)
      do i = 1, size(var%dims)
        dimids(i) = var%dims(i)%ptr%id
      end do
      ierr = NF90_DEF_VAR(dataset%id, var%name, var%data_type, dimids(1:size(var%dims)), var%id)
      call handle_error(ierr, 'Failed to define variable ' // trim(var%name) // '!', __FILE__, __LINE__)
      ierr = NF90_PUT_ATT(dataset%id, var%id, 'long_name', trim(var%long_name))
      ierr = NF90_PUT_ATT(dataset%id, var%id, 'units', trim(var%units))
      if (associated(var%missing_value)) then
        select type (missing_value => var%missing_value)
        type is (integer(4))
          ierr = NF90_PUT_ATT(dataset%id, var%id, '_FillValue', missing_value)
        type is (integer(8))
          ierr = NF90_PUT_ATT(dataset%id, var%id, '_FillValue', missing_value)
        type is (real(4))
          ierr = NF90_PUT_ATT(dataset%id, var%id, '_FillValue', missing_value)
        type is (real(8))
          ierr = NF90_PUT_ATT(dataset%id, var%id, '_FillValue', missing_value)
        end select
      end if
      call iter%next()
    end do

    ierr = NF90_ENDDEF(dataset%id)
    call handle_error(ierr, 'Failed to end definition!', __FILE__, __LINE__)

    if (new_file) then
      dataset%time_step = 0 ! Reset to zero!
      dataset%last_file_path = file_path
    end if

  end subroutine apply_dataset_to_netcdf

  subroutine fiona_output_0d(dataset_name, name, value)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*), intent(in) :: value

    type(dataset_type), pointer :: dataset
    type(var_type), pointer :: var
    integer ierr
    integer start(1)

    dataset => get_dataset(dataset_name, mode='output')
    var => dataset%get_var(name)

    if (var%dims(1)%ptr%size == NF90_UNLIMITED) then
      start(1) = dataset%time_step
    else
      start(1) = 1
    end if
    select type (value)
    type is (integer)
      ierr = NF90_PUT_VAR(dataset%id, var%id, value, start)
    type is (real(4))
      ierr = NF90_PUT_VAR(dataset%id, var%id, value, start)
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, value, start)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' in dataset ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine fiona_output_0d

  subroutine fiona_output_1d(dataset_name, name, array)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*), intent(in) :: array(:)

    type(dataset_type), pointer :: dataset
    type(var_type), pointer :: var
    integer lb, ub
    integer i, ierr
    integer start(2), count(2)

    dataset => get_dataset(dataset_name, mode='output')
    var => dataset%get_var(name)

    if (size(var%dims) == 1) then
      lb = 1
      ub = var%dims(1)%ptr%size
      start(1) = 1
      count(1) = var%dims(1)%ptr%size
    else
      do i = 1, 2
        if (var%dims(i)%ptr%size == NF90_UNLIMITED) then
          start(i) = dataset%time_step
          count(i) = 1
        else
          lb = lbound(array, i)
          ub = ubound(array, i)
          start(i) = 1
          count(i) = var%dims(i)%ptr%size
        end if
      end do
    end if

    select type (array)
    type is (integer)
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb:ub), start, count)
    type is (real(4))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb:ub), start, count)
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb:ub), start, count)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' in dataset ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine fiona_output_1d

  subroutine fiona_output_2d(dataset_name, name, array)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*), intent(in) :: array(:,:)

    type(dataset_type), pointer :: dataset
    type(var_type), pointer :: var
    integer lb1, ub1, lb2, ub2
    integer i, ierr
    integer start(3), count(3)

    dataset => get_dataset(dataset_name, mode='output')
    var => dataset%get_var(name)

    lb1 = lbound(array, 1)
    ub1 = ubound(array, 1)
    lb2 = lbound(array, 2)
    ub2 = ubound(array, 2)

    do i = 1, size(var%dims)
      if (var%dims(i)%ptr%size == NF90_UNLIMITED) then
        start(i) = dataset%time_step
        count(i) = 1
      else
        start(i) = 1
        count(i) = var%dims(i)%ptr%size
      end if
    end do

    select type (array)
    type is (integer)
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2), start, count)
    type is (real(4))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2), start, count)
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2), start, count)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' to ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine fiona_output_2d

  subroutine fiona_output_3d(dataset_name, name, array)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*), intent(in) :: array(:,:,:)

    type(dataset_type), pointer :: dataset
    type(var_type    ), pointer :: var
    integer lb1, ub1, lb2, ub2, lb3, ub3
    integer i, ierr
    integer start(3), count(3)

    dataset => get_dataset(dataset_name, mode='output')
    var => dataset%get_var(name)

    lb1 = lbound(array, 1)
    ub1 = ubound(array, 1)
    lb2 = lbound(array, 2)
    ub2 = ubound(array, 2)
    lb3 = lbound(array, 3)
    ub3 = ubound(array, 3)

    do i = 1, size(var%dims)
      if (var%dims(i)%ptr%size == NF90_UNLIMITED) then
        start(i) = dataset%time_step
        count(i) = 1
      else
        start(i) = 1
        count(i) = var%dims(i)%ptr%size
      end if
    end do

    select type (array)
    type is (integer)
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2,lb3:ub3), start, count)
    type is (real(4))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2,lb3:ub3), start, count)
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2,lb3:ub3), start, count)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' to ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine fiona_output_3d

  subroutine fiona_output_4d(dataset_name, name, array)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*)    , intent(in) :: array(:,:,:,:)

    type(dataset_type), pointer :: dataset
    type(var_type    ), pointer :: var
    integer lb1, ub1, lb2, ub2, lb3, ub3, lb4, ub4
    integer i, ierr
    integer start(4), count(4)

    dataset => get_dataset(dataset_name, mode='output')
    var => dataset%get_var(name)

    lb1 = lbound(array, 1)
    ub1 = ubound(array, 1)
    lb2 = lbound(array, 2)
    ub2 = ubound(array, 2)
    lb3 = lbound(array, 3)
    ub3 = ubound(array, 3)
    lb4 = lbound(array, 4)
    ub4 = ubound(array, 4)

    do i = 1, size(var%dims)
      if (var%dims(i)%ptr%size == NF90_UNLIMITED) then
        start(i) = dataset%time_step
        count(i) = 1
      else
        start(i) = 1
        count(i) = var%dims(i)%ptr%size
      end if
    end do

    select type (array)
    type is (integer)
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2,lb3:ub3,lb4:ub4), start, count)
    type is (real(4))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2,lb3:ub3,lb4:ub4), start, count)
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2,lb3:ub3,lb4:ub4), start, count)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' to ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine fiona_output_4d
  
  subroutine fiona_output_5d(dataset_name, name, array)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*)    , intent(in) :: array(:,:,:,:,:)

    type(dataset_type), pointer :: dataset
    type(var_type    ), pointer :: var
    integer lb1, ub1, lb2, ub2, lb3, ub3, lb4, ub4, lb5, ub5
    integer i, ierr
    integer start(5), count(5)

    dataset => get_dataset(dataset_name, mode='output')
    var => dataset%get_var(name)

    lb1 = lbound(array, 1)
    ub1 = ubound(array, 1)
    lb2 = lbound(array, 2)
    ub2 = ubound(array, 2)
    lb3 = lbound(array, 3)
    ub3 = ubound(array, 3)
    lb4 = lbound(array, 4)
    ub4 = ubound(array, 4)
    lb5 = lbound(array, 5)
    ub5 = ubound(array, 5)

    do i = 1, size(var%dims)
      if (var%dims(i)%ptr%size == NF90_UNLIMITED) then
        start(i) = dataset%time_step
        count(i) = 1
      else
        start(i) = 1
        count(i) = var%dims(i)%ptr%size
      end if
    end do

    select type (array)
    type is (integer)
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2,lb3:ub3,lb4:ub4,lb5:ub5), start, count)
    type is (real(4))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2,lb3:ub3,lb4:ub4,lb5:ub5), start, count)
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2,lb3:ub3,lb4:ub4,lb5:ub5), start, count)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' to ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine fiona_output_5d

  subroutine fiona_end_output(dataset_name)

    character(*), intent(in) :: dataset_name

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='output')

    ierr = NF90_CLOSE(dataset%id)
    call handle_error(ierr, 'Failed to close dataset ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine fiona_end_output

  subroutine fiona_start_input(dataset_name)

    character(*), intent(in) :: dataset_name

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_OPEN(dataset%file_path, NF90_NOWRITE + NF90_64BIT_OFFSET, dataset%id)
    call handle_error(ierr, 'Failed to open NetCDF file ' // trim(dataset%file_path) // ' to input!', __FILE__, __LINE__)

  end subroutine fiona_start_input

  subroutine fiona_get_dim(dataset_name, name, size)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    integer, intent(out), optional :: size

    type(dataset_type), pointer :: dataset
    type(dim_type), pointer :: dim
    integer ierr, dimid
    integer temp_id

    ! TODO: Try to refactor mode usage in dataset key.
    dataset => get_dataset(dataset_name, mode='input')

    if (dataset%dims%hashed(name)) then
      dim => dataset%get_dim(name)
    else
      allocate(dim)
      dim%name = name
      call dataset%dims%insert(name, dim)
      deallocate(dim)
      dim => dataset%get_dim(name)

      ! Temporally open the data file.
      ierr = NF90_OPEN(trim(dataset%file_path), NF90_NOWRITE + NF90_64BIT_OFFSET, temp_id)
      call handle_error(ierr, 'Failed to open NetCDF file "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

      ierr = NF90_INQ_DIMID(temp_id, name, dimid)
      call handle_error(ierr, 'Failed to inquire dimension ' // trim(name) // ' in NetCDF file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

      ierr = NF90_INQUIRE_DIMENSION(temp_id, dimid, len=dim%size)
      call handle_error(ierr, 'Failed to inquire size of dimension ' // trim(name) // ' in NetCDF file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

      ierr = NF90_CLOSE(temp_id)
      call handle_error(ierr, 'Failed to close file "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    end if
    if (present(size)) size = dim%size

  end subroutine fiona_get_dim

  subroutine fiona_get_att_str(dataset_name, att_name, value)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: att_name
    character(*), intent(out) :: value

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_GET_ATT(dataset%id, NF90_GLOBAL, att_name, value)
    call handle_error(ierr, 'Failed to get attribute "' // trim(att_name) // '" from file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

  end subroutine fiona_get_att_str

  subroutine fiona_get_var_att_str(dataset_name, var_name, att_name, value)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: var_name
    character(*), intent(in ) :: att_name
    character(*), intent(out) :: value

    type(dataset_type), pointer :: dataset
    type(var_type    ), pointer :: var
    integer ierr

    character(30) name
    integer num_att, i

    dataset => get_dataset(dataset_name, mode='input')
    if (dataset%vars%hashed(var_name)) then
      var => dataset%get_var(var_name)
    else
      allocate(var)
      var%name = var_name
      call dataset%vars%insert(var_name, var)
      deallocate(var)
      var => dataset%get_var(var_name)

      ierr = NF90_INQ_VARID(dataset%id, var_name, var%id)
      call handle_error(ierr, 'Failed to inquire id of variable "' // trim(var_name) // '"!')
    end if

    ierr = NF90_GET_ATT(dataset%id, var%id, att_name, value)
    if (ierr /= NF90_NOERR) then
      ierr = NF90_INQUIRE_VARIABLE(dataset%id, var%id, natts=num_att)
      call handle_error(ierr, 'Failed to inquire attribute number of variable "' // trim(var_name) // '"!', __FILE__, __LINE__)
      do i = 1, num_att
        ierr = NF90_INQ_ATTNAME(dataset%id, var%id, i, name)
        call handle_error(ierr, 'Failed to inquire attribute name of variable "' // trim(var_name) // '"!', __FILE__, __LINE__)
        print *, '- ', trim(name)
      end do
    end if
    call handle_error(ierr, 'Failed to get attribute "' // trim(att_name) // '" of variable "' // trim(var_name) // '" from file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

  end subroutine fiona_get_var_att_str

  subroutine fiona_get_att_i4(dataset_name, att_name, value)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: att_name
    integer  (4), intent(out) :: value

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_GET_ATT(dataset%id, NF90_GLOBAL, att_name, value)
    call handle_error(ierr, 'Failed to get global attribute "' // trim(att_name) // '" from file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

  end subroutine fiona_get_att_i4

  subroutine fiona_get_att_i8(dataset_name, att_name, value)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: att_name
    integer  (8), intent(out) :: value

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_GET_ATT(dataset%id, NF90_GLOBAL, att_name, value)
    call handle_error(ierr, 'Failed to get global attribute "' // trim(att_name) // '" from file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

  end subroutine fiona_get_att_i8

  subroutine fiona_get_att_r4(dataset_name, att_name, value)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: att_name
    real     (4), intent(out) :: value

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_GET_ATT(dataset%id, NF90_GLOBAL, att_name, value)
    call handle_error(ierr, 'Failed to get global attribute "' // trim(att_name) // '" from file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

  end subroutine fiona_get_att_r4

  subroutine fiona_get_att_r8(dataset_name, att_name, value)

    character(*), intent(in)  :: dataset_name
    character(*), intent(in)  :: att_name
    real     (8), intent(out) :: value

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_GET_ATT(dataset%id, NF90_GLOBAL, att_name, value)
    call handle_error(ierr, 'Failed to get global attribute "' // trim(att_name) // '" from file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

  end subroutine fiona_get_att_r8

  subroutine fiona_input_0d(dataset_name, var_name, value, time_step)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: var_name
    class(*), intent(out) :: value
    integer, intent(in), optional :: time_step

    type(dataset_type), pointer :: dataset
    integer ierr, varid

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_INQ_VARID(dataset%id, var_name, varid)
    call handle_error(ierr, 'No variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    select type (value)
    type is (integer)
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, value, start=[time_step])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, value)
      end if
    type is (real(4))
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, value, start=[time_step])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, value)
      end if
    type is (real(8))
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, value, start=[time_step])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, value)
      end if
    class default
      call log_error('Unsupported data type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine fiona_input_0d
  
  subroutine fiona_input_1d(dataset_name, var_name, array, time_step)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: var_name
    class(*), intent(out) :: array(:)
    integer, intent(in), optional :: time_step

    type(dataset_type), pointer :: dataset
    integer ierr, varid

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_INQ_VARID(dataset%id, var_name, varid)
    call handle_error(ierr, 'No variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    select type (array)
    type is (integer)
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,time_step], count=[size(array),1])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, array)
      end if
    type is (real(4))
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,time_step], count=[size(array),1])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, array)
      end if
    type is (real(8))
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,time_step], count=[size(array),1])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, array)
      end if
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine fiona_input_1d

  subroutine fiona_input_2d(dataset_name, var_name, array, time_step)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: var_name
    class(*), intent(out) :: array(:,:)
    integer, intent(in), optional :: time_step

    type(dataset_type), pointer :: dataset
    integer ierr, varid

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_INQ_VARID(dataset%id, var_name, varid)
    call handle_error(ierr, 'No variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    select type (array)
    type is (integer)
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,1,time_step], count=[size(array, 1),size(array, 2),1])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, array)
      end if
    type is (real(4))
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,1,time_step], count=[size(array, 1),size(array, 2),1])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, array)
      end if
    type is (real(8))
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,1,time_step], count=[size(array, 1),size(array, 2),1])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, array)
      end if
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine fiona_input_2d

  subroutine fiona_input_3d(dataset_name, var_name, array, time_step)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: var_name
    class    (*), intent(out) :: array(:,:,:)
    integer     , intent(in ), optional :: time_step

    type(dataset_type), pointer :: dataset
    integer ierr, varid

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_INQ_VARID(dataset%id, var_name, varid)
    call handle_error(ierr, 'No variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    select type (array)
    type is (integer)
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,1,1,time_step], count=[size(array, 1),size(array, 2),size(array, 3),1])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, array)
      end if
    type is (real(4))
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,1,1,time_step], count=[size(array, 1),size(array, 2),size(array, 3),1])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, array)
      end if
    type is (real(8))
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,1,1,time_step], count=[size(array, 1),size(array, 2),size(array, 3),1])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, array)
      end if
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine fiona_input_3d

  subroutine fiona_input_4d(dataset_name, var_name, array, time_step)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: var_name
    class    (*), intent(out) :: array(:,:,:,:)
    integer     , intent(in ), optional :: time_step

    type(dataset_type), pointer :: dataset
    integer ierr, varid

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_INQ_VARID(dataset%id, var_name, varid)
    call handle_error(ierr, 'No variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    select type (array)
    type is (integer)
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,1,1,1,time_step], count=[size(array, 1),size(array, 2),size(array, 3),size(array, 4),1])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, array)
      end if
    type is (real(4))
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,1,1,1,time_step], count=[size(array, 1),size(array, 2),size(array, 3),size(array, 4),1])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, array)
      end if
    type is (real(8))
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,1,1,1,time_step], count=[size(array, 1),size(array, 2),size(array, 3),size(array, 4),1])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, array)
      end if
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine fiona_input_4d
  
  subroutine fiona_input_5d(dataset_name, var_name, array, time_step)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: var_name
    class    (*), intent(out) :: array(:,:,:,:,:)
    integer     , intent(in ), optional :: time_step

    type(dataset_type), pointer :: dataset
    integer ierr, varid

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_INQ_VARID(dataset%id, var_name, varid)
    call handle_error(ierr, 'No variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    select type (array)
    type is (integer)
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,1,1,1,1,time_step], count=[size(array, 1),size(array, 2),size(array, 3),size(array, 4),size(array, 5),1])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, array)
      end if
    type is (real(4))
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,1,1,1,1,time_step], count=[size(array, 1),size(array, 2),size(array, 3),size(array, 4),size(array, 5),1])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, array)
      end if
    type is (real(8))
      if (present(time_step)) then
        ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,1,1,1,1,time_step], count=[size(array, 1),size(array, 2),size(array, 3),size(array, 4),size(array, 5),1])
      else
        ierr = NF90_GET_VAR(dataset%id, varid, array)
      end if
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine fiona_input_5d

  subroutine fiona_end_input(dataset_name)

    character(*), intent(in) :: dataset_name

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_CLOSE(dataset%id)
    call handle_error(ierr, 'Failed to end input!', __FILE__, __LINE__)

  end subroutine fiona_end_input

  function get_dataset(dataset_name, mode) result(res)

    character(*), intent(in) :: dataset_name
    character(*), intent(in), optional :: mode
    type(dataset_type), pointer :: res

    character(30) mode_

    if (present(mode)) then
      mode_ = mode
    else
      mode_ = 'output'
    end if
    select type (value => datasets%value(trim(dataset_name) // '.' // trim(mode_)))
    type is (dataset_type)
      res => value
    class default
      call log_error('Failed to get dataset ' // trim(dataset_name) // '.' // trim(mode_) // '!')
    end select

  end function get_dataset

  function get_dim_from_dataset(this, dim_name) result(res)

    class(dataset_type), intent(in) :: this
    character(*), intent(in) :: dim_name
    type(dim_type), pointer :: res

    res => null()
    select type (value => this%dims%value(dim_name))
    type is (dim_type)
      res => value
    end select

  end function get_dim_from_dataset

  function get_var_from_dataset(this, var_name) result(res)

    class(dataset_type), intent(in) :: this
    character(*), intent(in) :: var_name
    type(var_type), pointer :: res

    res => null()
    select type (value => this%vars%value(var_name))
    type is (var_type)
      res => value
    end select

  end function get_var_from_dataset

  subroutine var_final(this)

    type(var_type), intent(inout) :: this

    if (associated(this%missing_value)) then
      deallocate(this%missing_value)
    end if

  end subroutine var_final

  subroutine handle_error(ierr, msg, file, line)

    integer, intent(in) :: ierr
    character(*), intent(in) :: msg
    character(*), intent(in), optional :: file
    integer, intent(in), optional :: line

    if (ierr /= NF90_NOERR) then
      call log_error(trim(msg) // ' ' // trim(NF90_STRERROR(ierr)), file, line)
    end if

  end subroutine handle_error

end module fiona_mod
