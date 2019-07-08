module io_mod

  use netcdf
  use flogger
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

  interface io_output
    module procedure io_output_0d
    module procedure io_output_1d
    module procedure io_output_2d
    module procedure io_output_3d
    module procedure io_output_4d
    module procedure io_output_5d
  end interface io_output

  interface io_get_att
    module procedure io_get_att_str
    module procedure io_get_att_i4
    module procedure io_get_att_i8
    module procedure io_get_att_r4
    module procedure io_get_att_r8
  end interface io_get_att

  interface io_input
    module procedure io_input_1d
    module procedure io_input_2d
    module procedure io_input_3d
    module procedure io_input_4d
    module procedure io_input_5d
  end interface io_input

  character(30) time_units_str
  character(30) start_time_str

contains

  subroutine io_init(time_units, start_time)

    character(*), intent(in), optional :: time_units
    character(*), intent(in), optional :: start_time

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

    datasets = hash_table()

    call log_notice('IO module is initialized.')

  end subroutine io_init

  subroutine io_create_dataset(name, desc, file_prefix, file_path, mode, time_step_size)

    character(*), intent(in) :: name
    character(*), intent(in), optional :: desc
    character(*), intent(in), optional :: file_prefix
    character(*), intent(in), optional :: file_path
    character(*), intent(in), optional :: mode
    real, intent(in), optional :: time_step_size

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

    if (mode_ == 'input') then
      inquire(file=file_path_, exist=is_exist)
      if (.not. is_exist) then
        call log_error('io_create_dataset: Input file "' // trim(file_path_) // '" does not exist!')
      end if
    end if

    if (datasets%hashed(name)) then
      call log_error('Already created dataset ' // trim(name) // '!')
    end if

    dataset%name = name
    dataset%desc = desc_
    dataset%atts = hash_table()
    dataset%dims = hash_table()
    dataset%vars = hash_table()
    if (file_prefix_ /= '' .and. file_path_ == '') then
      dataset%file_prefix = trim(file_prefix_) // '.' // trim(name)
    else if (file_prefix_ == '' .and. file_path_ /= '') then
      dataset%file_path = file_path_
    end if
    dataset%mode = mode_

    call datasets%insert(trim(dataset%name) // '.' // trim(dataset%mode), dataset)

    if (dataset%file_path /= 'N/A') then
      call log_notice('Create ' // trim(dataset%mode) // ' dataset ' // trim(dataset%file_path) // '.')
    else if (dataset%file_prefix /= 'N/A') then
      call log_notice('Create ' // trim(dataset%mode) // ' dataset ' // trim(dataset%file_prefix) // '.')
    end if

  end subroutine io_create_dataset

  subroutine io_add_att(dataset_name, name, value)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*), intent(in) :: value

    type(dataset_type), pointer :: dataset

    dataset => get_dataset(dataset_name, mode='output')

    call dataset%atts%insert(name, value)

  end subroutine io_add_att

  subroutine io_add_dim(dataset_name, name, long_name, units, size, add_var)

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
      call io_add_var(dataset_name, name, long_name=dim%long_name, units=dim%units, dim_names=[name], data_type='real(8)')
    end if

  end subroutine io_add_dim

  subroutine io_add_var(dataset_name, name, long_name, units, dim_names, data_type, missing_value)

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

  end subroutine io_add_var

  subroutine io_start_output(dataset_name, time_in_seconds, new_file, tag)

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
        file_path = trim(string_delete(dataset%file_path, '.nc')) // '.' // trim(tag) // '.nc'
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

    if (dataset%time_step == 0 .or. new_file_) then
      ierr = NF90_CREATE(file_path, NF90_CLOBBER + NF90_64BIT_OFFSET, dataset%id)
      call handle_error(ierr, 'Failed to create NetCDF file to output!', __FILE__, __LINE__)
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

      dataset%time_step = 0 ! Reset to zero!
      dataset%last_file_path = file_path
    else
      ierr = NF90_OPEN(dataset%last_file_path, NF90_WRITE + NF90_64BIT_OFFSET, dataset%id)
      call handle_error(ierr, 'Failed to open NetCDF file to output! ' // trim(NF90_STRERROR(ierr)), __FILE__, __LINE__)
    end if
    
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
      ierr = NF90_ENDDEF(dataset%id)
      call handle_error(ierr, 'Failed to end definition!', __FILE__, __LINE__)
      ierr = NF90_PUT_VAR(dataset%id, dataset%time_var%id, [time_in_seconds / time_units_in_seconds], [dataset%time_step], [1])
      call handle_error(ierr, 'Failed to write variable time!', __FILE__, __LINE__)
    else
      ierr = NF90_ENDDEF(dataset%id)
      call handle_error(ierr, 'Failed to end definition!', __FILE__, __LINE__)
    end if

  end subroutine io_start_output

  subroutine io_output_0d(dataset_name, name, value)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*), intent(in) :: value

    type(dataset_type), pointer :: dataset
    type(var_type), pointer :: var
    integer ierr

    dataset => get_dataset(dataset_name, mode='output')
    var => dataset%get_var(name)

    select type (value)
    type is (integer)
      ierr = NF90_PUT_VAR(dataset%id, var%id, value)
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, value)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' in dataset ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine io_output_0d

  subroutine io_output_1d(dataset_name, name, array)

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
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb:ub), start, count)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' in dataset ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine io_output_1d

  subroutine io_output_2d(dataset_name, name, array)

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
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2), start, count)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' to ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine io_output_2d

  subroutine io_output_3d(dataset_name, name, array)

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
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2,lb3:ub3), start, count)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' to ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine io_output_3d

  subroutine io_output_4d(dataset_name, name, array)

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
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2,lb3:ub3,lb4:ub4), start, count)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' to ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine 
  
  subroutine io_output_5d(dataset_name, name, array)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*)    , intent(in) :: array(:,:,:,:,:)

    type(dataset_type), pointer :: dataset
    type(var_type    ), pointer :: var
    integer lb1, ub1, lb2, ub2, lb3, ub3, lb4, ub4, lb5, ub5
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
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array(lb1:ub1,lb2:ub2,lb3:ub3,lb4:ub4,lb5:ub5), start, count)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' to ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine io_output_5d

  subroutine io_end_output(dataset_name)

    character(*), intent(in) :: dataset_name

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='output')

    ierr = NF90_CLOSE(dataset%id)
    call handle_error(ierr, 'Failed to close dataset ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine io_end_output

  subroutine io_start_input(dataset_name)

    character(*), intent(in) :: dataset_name

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_OPEN(dataset%file_path, NF90_NOWRITE + NF90_64BIT_OFFSET, dataset%id)
    call handle_error(ierr, 'Failed to open NetCDF file ' // trim(dataset%file_path) // ' to input!', __FILE__, __LINE__)

  end subroutine io_start_input

  subroutine io_get_dim(dataset_name, name, size)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    integer, intent(out), optional :: size

    type(dataset_type), pointer :: dataset
    type(dim_type), pointer :: dim
    integer ierr, dimid

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
      ierr = NF90_OPEN(dataset%file_path, NF90_NOWRITE + NF90_64BIT_OFFSET, dataset%id)
      call handle_error(ierr, 'Failed to open NetCDF file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

      ierr = NF90_INQ_DIMID(dataset%id, name, dimid)
      call handle_error(ierr, 'Failed to inquire dimension ' // trim(name) // ' in NetCDF file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

      ierr = NF90_INQUIRE_DIMENSION(dataset%id, dimid, len=dim%size)
      call handle_error(ierr, 'Failed to inquire size of dimension ' // trim(name) // ' in NetCDF file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)
    end if
    if (present(size)) size = dim%size

  end subroutine io_get_dim

  subroutine io_get_att_str(dataset_name, name, value)

    character(*), intent(in )              :: dataset_name
    character(*), intent(in )              :: name
    character(:), intent(out), allocatable :: value

    type(dataset_type), pointer :: dataset
    character(256) att
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_GET_ATT(dataset%id, NF90_GLOBAL, name, att)
    call handle_error(ierr, 'Failed to get att "' // trim(name) // '" from file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)
    value = trim(att)

  end subroutine io_get_att_str

  subroutine io_get_att_i4(dataset_name, name, value)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: name
    integer  (4), intent(out) :: value

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_GET_ATT(dataset%id, NF90_GLOBAL, name, value)
    call handle_error(ierr, 'Failed to get global attribute "' // trim(name) // '" from file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

  end subroutine io_get_att_i4

  subroutine io_get_att_i8(dataset_name, name, value)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: name
    integer  (8), intent(out) :: value

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_GET_ATT(dataset%id, NF90_GLOBAL, name, value)
    call handle_error(ierr, 'Failed to get global attribute "' // trim(name) // '" from file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

  end subroutine io_get_att_i8

  subroutine io_get_att_r4(dataset_name, name, value)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: name
    real     (4), intent(out) :: value

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_GET_ATT(dataset%id, NF90_GLOBAL, name, value)
    call handle_error(ierr, 'Failed to get global attribute "' // trim(name) // '" from file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

  end subroutine io_get_att_r4

  subroutine io_get_att_r8(dataset_name, name, value)

    character(*), intent(in)  :: dataset_name
    character(*), intent(in)  :: name
    real     (8), intent(out) :: value

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_GET_ATT(dataset%id, NF90_GLOBAL, name, value)
    call handle_error(ierr, 'Failed to get global attribute "' // trim(name) // '" from file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

  end subroutine io_get_att_r8
  
  subroutine io_input_1d(dataset_name, name, array)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*), intent(out) :: array(:)

    type(dataset_type), pointer :: dataset
    integer lb, ub
    integer ierr, varid

    dataset => get_dataset(dataset_name, mode='input')

    lb = lbound(array, 1)
    ub = ubound(array, 1)

    ierr = NF90_INQ_VARID(dataset%id, name, varid)
    call handle_error(ierr, 'No variable "' // trim(name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    select type (array)
    type is (integer)
      ierr = NF90_GET_VAR(dataset%id, varid, array)
    type is (real(8))
      ierr = NF90_GET_VAR(dataset%id, varid, array)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine io_input_1d

  subroutine io_input_2d(dataset_name, name, array)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*), intent(out) :: array(:,:)

    type(dataset_type), pointer :: dataset
    integer lb1, ub1, lb2, ub2
    integer ierr, varid

    dataset => get_dataset(dataset_name, mode='input')

    lb1 = lbound(array, 1)
    ub1 = ubound(array, 1)
    lb2 = lbound(array, 2)
    ub2 = ubound(array, 2)

    ierr = NF90_INQ_VARID(dataset%id, name, varid)
    call handle_error(ierr, 'No variable "' // trim(name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    select type (array)
    type is (integer)
      ierr = NF90_GET_VAR(dataset%id, varid, array)
    type is (real(8))
      ierr = NF90_GET_VAR(dataset%id, varid, array)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine io_input_2d

  subroutine io_input_3d(dataset_name, name, array)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: name
    class    (*), intent(out) :: array(:,:,:)

    type(dataset_type), pointer :: dataset
    integer lb1, ub1, lb2, ub2, lb3, ub3
    integer ierr, varid

    dataset => get_dataset(dataset_name, mode='input')

    lb1 = lbound(array, 1)
    ub1 = ubound(array, 1)
    lb2 = lbound(array, 2)
    ub2 = ubound(array, 2)
    lb3 = lbound(array, 3)
    ub3 = ubound(array, 3)

    ierr = NF90_INQ_VARID(dataset%id, name, varid)
    call handle_error(ierr, 'No variable "' // trim(name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    select type (array)
    type is (integer)
      ierr = NF90_GET_VAR(dataset%id, varid, array)
    type is (real(8))
      ierr = NF90_GET_VAR(dataset%id, varid, array)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine io_input_3d

  subroutine io_input_4d(dataset_name, name, array)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: name
    class    (*), intent(out) :: array(:,:,:,:)

    type(dataset_type), pointer :: dataset
    integer lb1, ub1, lb2, ub2, lb3, ub3, lb4, ub4
    integer ierr, varid

    dataset => get_dataset(dataset_name, mode='input')

    lb1 = lbound(array, 1)
    ub1 = ubound(array, 1)
    lb2 = lbound(array, 2)
    ub2 = ubound(array, 2)
    lb3 = lbound(array, 3)
    ub3 = ubound(array, 3)
    lb4 = lbound(array, 4)
    ub4 = ubound(array, 4)

    ierr = NF90_INQ_VARID(dataset%id, name, varid)
    call handle_error(ierr, 'No variable "' // trim(name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    select type (array)
    type is (integer)
      ierr = NF90_GET_VAR(dataset%id, varid, array)
    type is (real(8))
      ierr = NF90_GET_VAR(dataset%id, varid, array)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine io_input_4d
  
  subroutine io_input_5d(dataset_name, name, array)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: name
    class    (*), intent(out) :: array(:,:,:,:,:)

    type(dataset_type), pointer :: dataset
    integer lb1, ub1, lb2, ub2, lb3, ub3, lb4, ub4, lb5, ub5
    integer ierr, varid

    dataset => get_dataset(dataset_name, mode='input')

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

    ierr = NF90_INQ_VARID(dataset%id, name, varid)
    call handle_error(ierr, 'No variable "' // trim(name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    select type (array)
    type is (integer)
      ierr = NF90_GET_VAR(dataset%id, varid, array)
    type is (real(8))
      ierr = NF90_GET_VAR(dataset%id, varid, array)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine io_input_5d

  subroutine io_end_input(dataset_name)

    character(*), intent(in) :: dataset_name

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_CLOSE(dataset%id)
    call handle_error(ierr, 'Failed to end input!', __FILE__, __LINE__)

  end subroutine io_end_input

  function get_dataset(name, mode) result(res)

    character(*), intent(in) :: name
    character(*), intent(in), optional :: mode
    type(dataset_type), pointer :: res

    character(30) mode_

    if (present(mode)) then
      mode_ = mode
    else
      mode_ = 'output'
    end if
    select type (value => datasets%value(trim(name) // '.' // trim(mode_)))
    type is (dataset_type)
      res => value
    class default
      call log_error('Failed to get dataset ' // trim(name) // '.' // trim(mode_) // '!')
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
      call log_error(msg // ' ' // trim(NF90_STRERROR(ierr)), file, line)
    end if

  end subroutine handle_error

end module io_mod
