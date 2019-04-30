module io_mod

  use netcdf
  use log_mod
  use string_mod
  use hash_table_mod

  implicit none

  private

  public io_init
  public io_create_dataset
  public io_add_att
  public io_add_dim
  public io_add_var
  public io_start_output
  public io_output
  public io_end_output
  public io_start_input
  public io_get_dim
  public io_get_att
  public io_input
  public io_end_input

  type dataset_type
    integer :: id = -1
    character(30) name
    character(256) :: desc = 'N/A'
    character(256) :: author = 'N/A'
    character(256) :: file_path = 'N/A'
    character(256) :: file_prefix = 'N/A'
    character(10) mode
    character(10) :: new_file_alert = 'N/A'
    character(256) last_file_path
    type(var_type), pointer :: time_var => null()
    type(hash_table_type) atts
    type(hash_table_type) dims
    type(hash_table_type) vars
    real period
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
    type(var_dim_type), allocatable :: dims(:)
  end type var_type

  type(hash_table_type) datasets
  real(8) time_units_in_seconds

  interface io_output
    module procedure io_output_0d
    module procedure io_output_1d
    module procedure io_output_2d
    module procedure io_output_3d
  end interface io_output

  interface io_get_att
    module procedure io_get_att_str
  end interface io_get_att

  interface io_input
    module procedure io_input_1d
    module procedure io_input_2d
  end interface io_input

  interface
    subroutine add_alert_interface(name, months, days, hours, minutes, seconds)
      character(*), intent(in) :: name
      real, intent(in), optional :: months
      real, intent(in), optional :: days
      real, intent(in), optional :: hours
      real, intent(in), optional :: minutes
      real, intent(in), optional :: seconds
    end subroutine add_alert_interface

    function is_alerted_interface(name) result(res)
      character(*), intent(in) :: name
      logical res
    end function is_alerted_interface
  end interface

  character(30) time_units_str
  character(30) start_time_str
  procedure(add_alert_interface), pointer :: add_alert => null()
  procedure(is_alerted_interface), pointer :: is_alerted => null()

contains

  subroutine io_init(time_units, start_time, register_add_alert, register_is_alerted)

    character(*), intent(in), optional :: time_units
    character(*), intent(in), optional :: start_time
    procedure(add_alert_interface), optional :: register_add_alert
    procedure(is_alerted_interface), optional :: register_is_alerted

    if (present(time_units)) then
      time_units_str = time_units
      select case (time_units)
      case ('days')
        time_units_in_seconds = 86400.0
      case ('hours')
        time_units_in_seconds = 3600.0
      case ('seconds')
        time_units_in_seconds = 60.0
      case default
        call log_error('Invalid time_units ' // trim(time_units) // '!')
      end select
    end if

    if (present(start_time)) then
      start_time_str = start_time
    end if

    if (present(register_add_alert)) then
      add_alert => register_add_alert
    end if
    if (present(register_is_alerted)) then
      is_alerted => register_is_alerted
    end if

    datasets = hash_table()

    call log_notice('IO module is initialized.')

  end subroutine io_init

  subroutine io_create_dataset(name, desc, file_prefix, file_path, mode, period, time_step_size, frames_per_file)

    character(*), intent(in), optional :: name
    character(*), intent(in), optional :: desc
    character(*), intent(in), optional :: file_prefix
    character(*), intent(in), optional :: file_path
    character(*), intent(in), optional :: mode
    character(*), intent(in), optional :: period
    real, intent(in), optional :: time_step_size
    character(*), intent(in), optional :: frames_per_file

    character(30) name_, period_, time_value, time_units
    character(10) mode_
    character(256) desc_, file_prefix_, file_path_
    type(dataset_type) dataset
    logical is_exist
    integer i
    real value

    if (present(name)) then
      name_ = name
    else
      name_ = 'hist0'
    end if
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
    if (present(period)) then
      period_ = period
    else
      period_ = 'once'
    end if
    if (present(file_path) .and. period_ /= 'once') then
      call log_warning('io_create_dataset: Set file_path to "' // trim(file_path_) // &
        '", but period is not once! Reset period.')
      period_ = 'once'
    end if

    if (mode_ == 'input') then
      inquire(file=file_path_, exist=is_exist)
      if (.not. is_exist) then
        call log_error('io_create_dataset: Input file "' // trim(file_path_) // '" does not exist!')
      end if
    end if

    if (datasets%hashed(name_)) then
      call log_error('Already created dataset ' // trim(name_) // '!')
    end if

    dataset%name = name_
    dataset%desc = desc_
    dataset%atts = hash_table()
    dataset%dims = hash_table()
    dataset%vars = hash_table()
    if (file_prefix_ /= '' .and. file_path_ == '') then
      dataset%file_prefix = file_prefix_
    else if (file_prefix_ == '' .and. file_path_ /= '') then
      dataset%file_path = file_path_
    end if
    if (name_(1:4) == 'hist') then
      dataset%file_prefix = trim(dataset%file_prefix) // '.' // trim(string_delete(name_, 'ist'))
    end if
    dataset%mode = mode_

    ! Add alert for IO action.
    if (period_ /= 'once') then
      time_value = string_split(period_, 1)
      time_units = string_split(period_, 2)
      read(time_value, *) dataset%period 
    else
      time_units = 'once'
    end if
    select case (time_units)
    case ('days')
      dataset%period = dataset%period * 86400
    case ('hours')
      dataset%period = dataset%period * 3600
    case ('minutes')
      dataset%period = dataset%period * 60
    case ('seconds')
      dataset%period = dataset%period
    case ('steps')
      dataset%period = dataset%period * time_step_size
    case ('once')
      dataset%period = 0
    case default
      call log_error('Invalid IO period ' // trim(period_) // '!')
    end select

    if (dataset%period /= 0.0 .and. associated(add_alert)) then
      call add_alert(trim(dataset%name) // '.' // trim(dataset%mode), seconds=dataset%period)
    end if

    ! Add alert for create new file.
    if (present(frames_per_file) .and. frames_per_file /= 'N/A') then
      dataset%new_file_alert = trim(dataset%name) // '.new_file'
      time_value = string_split(frames_per_file, 1)
      time_units = string_split(frames_per_file, 2)
      read(time_value, *) value
        select case (time_units)
        case ('months')
          call add_alert(dataset%new_file_alert, months=value)
        case ('days')
          call add_alert(dataset%new_file_alert, days=value)
        case default
          call log_error('Invalid IO period ' // trim(period_) // '!')
        end select
    end if

    call datasets%insert(trim(dataset%name) // '.' // trim(dataset%mode), dataset)

    if (dataset%file_path /= 'N/A') then
      call log_notice('Create ' // trim(dataset%mode) // ' dataset ' // trim(dataset%file_path) // '.')
    else if (dataset%file_prefix /= 'N/A') then
      call log_notice('Create ' // trim(dataset%mode) // ' dataset ' // trim(dataset%file_prefix) // '.')
    end if

  end subroutine io_create_dataset

  subroutine io_add_att(name, value, dataset_name)

    character(*), intent(in) :: name
    class(*), intent(in) :: value
    character(*), intent(in), optional :: dataset_name

    type(dataset_type), pointer :: dataset

    if (present(dataset_name)) then
      dataset => get_dataset(name=dataset_name, mode='output')
    else
      dataset => get_dataset(mode='output')
    end if

    call dataset%atts%insert(name, value)

  end subroutine io_add_att

  subroutine io_add_dim(name, dataset_name, long_name, units, size, add_var)

    character(*), intent(in) :: name
    character(*), intent(in), optional :: dataset_name
    character(*), intent(in), optional :: long_name
    character(*), intent(in), optional :: units
    integer, intent(in), optional :: size
    logical, intent(in), optional :: add_var

    type(dataset_type), pointer :: dataset
    type(dim_type) dim

    if (present(dataset_name)) then
      dataset => get_dataset(name=dataset_name, mode='output')
    else
      dataset => get_dataset(mode='output')
    end if

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

    if (present(add_var) .and. add_var) then
      ! Add corresponding dimension variable.
      if (.not. present(long_name)) then
        select case (name)
        case ('lon', 'ilon')
          dim%long_name = 'Longitude'
        case ('lat', 'ilat')
          dim%long_name = 'Latitude'
        case ('time', 'Time')
          dim%long_name = 'Time'
        case default
          dim%long_name = long_name
        end select
      end if
      if (.not. present(units)) then
        select case (name)
        case ('lon', 'ilon')
          dim%units = 'degrees_east'
        case ('lat', 'ilat')
          dim%units = 'degrees_north'
        case ('time', 'Time')
          write(dim%units, '(A, " since ", A)') trim(time_units_str), trim(start_time_str)
        case default
          dim%units = units
        end select
      end if
      call io_add_var(name, dataset_name, long_name=dim%long_name, units=dim%units, dim_names=[name], data_type='real(8)')
    end if

  end subroutine io_add_dim

  subroutine io_add_var(name, dataset_name, long_name, units, data_type, dim_names)

    character(*), intent(in) :: name
    character(*), intent(in), optional :: dataset_name
    character(*), intent(in) :: long_name
    character(*), intent(in) :: units
    character(*), intent(in), optional :: data_type
    character(*), intent(in) :: dim_names(:)

    type(dataset_type), pointer :: dataset
    type(var_type) :: var
    type(hash_table_iterator_type) iter
    type(dim_type), pointer :: dim
    integer i
    logical found
    real real

    if (present(dataset_name)) then
      dataset => get_dataset(name=dataset_name, mode='output')
    else
      dataset => get_dataset(mode='output')
    end if

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

    if (name == 'Time') dataset%time_var => dataset%get_var(name)

  end subroutine io_add_var

  subroutine io_start_output(time_in_seconds, dataset_name, tag)

    real(8), intent(in), optional :: time_in_seconds
    character(*), intent(in), optional :: dataset_name
    character(*), intent(in), optional :: tag

    character(256) file_path
    type(dataset_type), pointer :: dataset
    type(hash_table_iterator_type) iter
    type(dim_type), pointer :: dim
    type(var_type), pointer :: var
    integer i, ierr, dimids(10)

    if (present(dataset_name)) then
      dataset => get_dataset(name=dataset_name, mode='output')
    else
      dataset => get_dataset(mode='output')
    end if

    if (present(tag)) then
      if (dataset%file_path /= 'N/A') then
        file_path = trim(string_delete(dataset%file_path, '.nc')) // '.' // trim(tag) // '.nc'
      else
        write(file_path, "(A, '.', A, '.nc')") trim(dataset%file_prefix), tag
      end if
    else
      if (dataset%file_path /= 'N/A') then
        file_path = dataset%file_path
      else
        write(file_path, "(A, '.nc')") trim(dataset%file_prefix)
      end if
    end if

    if (dataset%new_file_alert == 'N/A' .or. (associated(is_alerted) .and. is_alerted(dataset%new_file_alert)) .or. dataset%time_step == 0) then
      ierr = NF90_CREATE(file_path, NF90_CLOBBER, dataset%id)
      if (ierr /= NF90_NOERR) then
        call log_error('Failed to create NetCDF file to output!')
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
        if (ierr /= NF90_NOERR) then
          call log_error('Failed to define dimension ' // trim(dim%name) // '!')
        end if
        call iter%next()
      end do

      iter = hash_table_iterator(dataset%vars)
      do while (.not. iter%ended())
        var => dataset%get_var(iter%key)
        do i = 1, size(var%dims)
          dimids(i) = var%dims(i)%ptr%id
        end do
        ierr = NF90_DEF_VAR(dataset%id, var%name, var%data_type, dimids(1:size(var%dims)), var%id)
        if (ierr /= NF90_NOERR) then
          call log_error('Failed to define variable ' // trim(var%name) // '!')
        end if
        ierr = NF90_PUT_ATT(dataset%id, var%id, 'long_name', trim(var%long_name))
        ierr = NF90_PUT_ATT(dataset%id, var%id, 'units', trim(var%units))
        call iter%next()
      end do

      ierr = NF90_ENDDEF(dataset%id)

      dataset%time_step = 0 ! Reset to zero!
      dataset%last_file_path = file_path
    else
      ierr = NF90_OPEN(dataset%last_file_path, NF90_WRITE, dataset%id)
      if (ierr /= NF90_NOERR) then
        call log_error('Failed to open NetCDF file to output! ' // trim(NF90_STRERROR(ierr)), __FILE__, __LINE__)
      end if
    end if
    
    ! Write time dimension variable.
    if (associated(dataset%time_var)) then
      dataset%time_step = dataset%time_step + 1
      ! Update time units because restart may change it.
      write(dataset%time_var%units, '(A, " since ", A)') trim(time_units_str), trim(start_time_str)
      ierr = NF90_PUT_ATT(dataset%id, dataset%time_var%id, 'units', trim(dataset%time_var%units))
      ierr = NF90_PUT_VAR(dataset%id, dataset%time_var%id, [time_in_seconds / time_units_in_seconds], [dataset%time_step], [1])
      if (ierr /= NF90_NOERR) then
        call log_error('Failed to write variable time!')
      end if
    end if

  end subroutine io_start_output

  subroutine io_output_0d(name, value, dataset_name)

    character(*), intent(in) :: name
    class(*), intent(in) :: value
    character(*), intent(in), optional :: dataset_name

    type(dataset_type), pointer :: dataset
    type(var_type), pointer :: var
    integer ierr

    if (present(dataset_name)) then
      dataset => get_dataset(name=dataset_name, mode='output')
    else
      dataset => get_dataset(mode='output')
    end if
    var => dataset%get_var(name)

    select type (value)
    type is (integer)
      ierr = NF90_PUT_VAR(dataset%id, var%id, value)
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, value)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    if (ierr /= NF90_NOERR) then
      call log_error('Failed to write variable ' // trim(name) // ' in dataset ' // trim(dataset%name) // '! ' // trim(NF90_STRERROR(ierr)))
    end if

  end subroutine io_output_0d

  subroutine io_output_1d(name, array, dataset_name)

    character(*), intent(in) :: name
    class(*), intent(in) :: array(:)
    character(*), intent(in), optional :: dataset_name

    type(dataset_type), pointer :: dataset
    type(var_type), pointer :: var
    integer lb, ub
    integer i, ierr
    integer start(2), count(2)

    if (present(dataset_name)) then
      dataset => get_dataset(name=dataset_name, mode='output')
    else
      dataset => get_dataset(mode='output')
    end if
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
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb:ub), start, count)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    if (ierr /= NF90_NOERR) then
      call log_error('Failed to write variable ' // trim(name) // ' in dataset ' // trim(dataset%name) // '! ' // trim(NF90_STRERROR(ierr)))
    end if

  end subroutine io_output_1d

  subroutine io_output_2d(name, array, dataset_name)

    character(*), intent(in) :: name
    class(*), intent(in) :: array(:,:)
    character(*), intent(in), optional :: dataset_name

    type(dataset_type), pointer :: dataset
    type(var_type), pointer :: var
    integer lb1, ub1, lb2, ub2
    integer i, ierr
    integer start(3), count(3)

    if (present(dataset_name)) then
      dataset => get_dataset(name=dataset_name, mode='output')
    else
      dataset => get_dataset(mode='output')
    end if
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
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2), start, count)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    if (ierr /= NF90_NOERR) then
      call log_error('Failed to write variable ' // trim(name) // ' to ' // trim(dataset%name) // '!' // NF90_STRERROR(ierr))
    end if

  end subroutine io_output_2d

  subroutine io_output_3d(name, array, dataset_name)

    character(*), intent(in)           :: name
    class(*)    , intent(in)           :: array(:,:,:)
    character(*), intent(in), optional :: dataset_name

    type(dataset_type), pointer :: dataset
    type(var_type    ), pointer :: var
    integer lb1, ub1, lb2, ub2, lb3, ub3
    integer i, ierr
    integer start(3), count(3)

    if (present(dataset_name)) then
      dataset => get_dataset(name=dataset_name, mode='output')
    else
      dataset => get_dataset(mode='output')
    end if
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
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2,lb3:ub3), start, count)
    end select
    if (ierr /= NF90_NOERR) then
      call log_error('Failed to write variable ' // trim(name) // ' to ' // trim(dataset%name) // '!' // NF90_STRERROR(ierr))
    end if

  end subroutine io_output_3d

  subroutine io_end_output(dataset_name)

    character(*), intent(in), optional :: dataset_name

    type(dataset_type), pointer :: dataset
    integer ierr

    if (present(dataset_name)) then
      dataset => get_dataset(name=dataset_name, mode='output')
    else
      dataset => get_dataset(mode='output')
    end if

    ierr = NF90_CLOSE(dataset%id)
    if (ierr /= NF90_NOERR) then
      call log_error('Failed to close dataset ' // trim(dataset%name) // '!')
    end if

  end subroutine io_end_output

  subroutine io_start_input(dataset_name)

    character(*), intent(in), optional :: dataset_name

    type(dataset_type), pointer :: dataset
    integer ierr

    if (present(dataset_name)) then
      dataset => get_dataset(name=dataset_name, mode='input')
    else
      dataset => get_dataset(mode='input')
    end if

    ierr = NF90_OPEN(dataset%file_path, NF90_NOWRITE, dataset%id)
    if (ierr /= NF90_NOERR) then
      call log_error('Failed to open NetCDF file ' // trim(dataset%file_path) // ' to input! ' // trim(NF90_STRERROR(ierr)))
    end if

  end subroutine io_start_input

  subroutine io_get_dim(name, dataset_name, size)

    character(*), intent(in) :: name
    character(*), intent(in) :: dataset_name
    integer, intent(out), optional :: size

    type(dataset_type), pointer :: dataset
    type(dim_type), pointer :: dim
    integer ierr, dimid

    ! TODO: Try to refactor mode usage in dataset key.
    dataset => get_dataset(name=dataset_name, mode='input')

    if (dataset%dims%hashed(name)) then
      dim => dataset%get_dim(name)
    else
      allocate(dim)
      dim%name = name
      call dataset%dims%insert(name, dim)
      deallocate(dim)
      dim => dataset%get_dim(name)

      ! Temporally open the data file.
      ierr = NF90_OPEN(dataset%file_path, NF90_NOWRITE, dataset%id)
      if (ierr /= NF90_NOERR) then
        call log_error('Failed to open NetCDF file ' // trim(dataset%file_path) // '! ' // trim(NF90_STRERROR(ierr)), __FILE__, __LINE__)
      end if

      ierr = NF90_INQ_DIMID(dataset%id, name, dimid)
      if (ierr /= NF90_NOERR) then
        call log_error('Failed to inquire dimension ' // trim(name) // ' in NetCDF file ' // trim(dataset%file_path) // '! ' // trim(NF90_STRERROR(ierr)), __FILE__, __LINE__)
      end if

      ierr = NF90_INQUIRE_DIMENSION(dataset%id, dimid, len=dim%size)
      if (ierr /= NF90_NOERR) then
        call log_error('Failed to inquire size of dimension ' // trim(name) // ' in NetCDF file ' // trim(dataset%file_path) // '! ' // trim(NF90_STRERROR(ierr)), __FILE__, __LINE__)
      end if
    end if
    if (present(size)) size = dim%size

  end subroutine io_get_dim

  function io_get_att_str(name, dataset_name) result(res)

    character(*), intent(in) :: name
    character(*), intent(in), optional :: dataset_name
    character(:), allocatable :: res

    type(dataset_type), pointer :: dataset
    character(256) att
    integer ierr

    if (present(dataset_name)) then
      dataset => get_dataset(name=dataset_name, mode='input')
    else
      dataset => get_dataset(mode='input')
    end if

    ierr = NF90_GET_ATT(dataset%id, NF90_GLOBAL, name, att)
    if (ierr /= NF90_NOERR) then
      call log_error('Failed to get att "' // trim(name) // '" from file ' // trim(dataset%file_path) // '!')
    end if
    res = trim(att)

  end function io_get_att_str

  subroutine io_input_1d(name, array, dataset_name)

    character(*), intent(in) :: name
    class(*), intent(out) :: array(:)
    character(*), intent(in), optional :: dataset_name

    type(dataset_type), pointer :: dataset
    integer lb, ub
    integer ierr, varid

    if (present(dataset_name)) then
      dataset => get_dataset(name=dataset_name, mode='input')
    else
      dataset => get_dataset(mode='input')
    end if

    lb = lbound(array, 1)
    ub = ubound(array, 1)

    ierr = NF90_INQ_VARID(dataset%id, name, varid)
    if (ierr /= NF90_NOERR) then
      call log_error('No variable "' // trim(name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    end if
    select type (array)
    type is (integer)
      ierr = NF90_GET_VAR(dataset%id, varid, array)
    type is (real(8))
      ierr = NF90_GET_VAR(dataset%id, varid, array)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    if (ierr /= NF90_NOERR) then
      call log_error('Failed to read variable "' // trim(name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    end if

  end subroutine io_input_1d

  subroutine io_input_2d(name, array, dataset_name)

    character(*), intent(in) :: name
    class(*), intent(out) :: array(:,:)
    character(*), intent(in), optional :: dataset_name

    type(dataset_type), pointer :: dataset
    integer lb1, ub1, lb2, ub2
    integer ierr, varid

    if (present(dataset_name)) then
      dataset => get_dataset(name=dataset_name, mode='input')
    else
      dataset => get_dataset(mode='input')
    end if

    lb1 = lbound(array, 1)
    ub1 = ubound(array, 1)
    lb2 = lbound(array, 2)
    ub2 = ubound(array, 2)

    ierr = NF90_INQ_VARID(dataset%id, name, varid)
    if (ierr /= NF90_NOERR) then
      call log_error('No variable "' // trim(name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    end if
    select type (array)
    type is (integer)
      ierr = NF90_GET_VAR(dataset%id, varid, array)
    type is (real(8))
      ierr = NF90_GET_VAR(dataset%id, varid, array)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    if (ierr /= NF90_NOERR) then
      call log_error('Failed to read variable "' // trim(name) // '" in dataset "' // trim(dataset%file_path) // '"! ' // NF90_STRERROR(ierr), __FILE__, __LINE__)
    end if

  end subroutine io_input_2d

  subroutine io_end_input(dataset_name)

    character(*), intent(in), optional :: dataset_name

    type(dataset_type), pointer :: dataset
    integer ierr

    if (present(dataset_name)) then
      dataset => get_dataset(name=dataset_name, mode='input')
    else
      dataset => get_dataset(mode='input')
    end if

    ierr = NF90_CLOSE(dataset%id)

  end subroutine io_end_input

  function get_dataset(name, mode) result(res)

    character(*), intent(in), optional :: name
    character(*), intent(in), optional :: mode
    type(dataset_type), pointer :: res

    character(30) name_, mode_

    if (present(name)) then
      name_ = name
    else
      name_ = 'hist0'
    end if
    if (present(mode)) then
      mode_ = mode
    else
      mode_ = 'output'
    end if
    select type (value => datasets%value(trim(name_) // '.' // trim(mode_)))
    type is (dataset_type)
      res => value
    end select

  end function get_dataset

  function get_dim_from_dataset(this, name) result(res)

    class(dataset_type), intent(in) :: this
    character(*), intent(in) :: name
    type(dim_type), pointer :: res

    select type (value => this%dims%value(name))
    type is (dim_type)
      res => value
    end select

  end function get_dim_from_dataset

  function get_var_from_dataset(this, name) result(res)

    class(dataset_type), intent(in) :: this
    character(*), intent(in) :: name
    type(var_type), pointer :: res

    select type (value => this%vars%value(name))
    type is (var_type)
      res => value
    end select

  end function get_var_from_dataset

end module io_mod
