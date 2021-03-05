module fiona_mod

  use netcdf
  use flogger
  use string
  use container
#ifdef HAS_MPI
  use mpi
#endif

  implicit none

  private

  public fiona_init
  public fiona_create_dataset
  public fiona_open_dataset
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
  public fiona_quick_output

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
    real(8) :: time_in_seconds = -1
    real(8) time_units_in_seconds
    character(30) :: time_units_str = 'N/A'
    character(30) :: start_time_str = 'N/A'
    ! --------------------------------------------------------------------------
    ! Parallel IO
#ifdef HAS_MPI
    integer :: num_proc = 0
    integer :: mpi_comm = MPI_COMM_NULL
    integer :: proc_id  = MPI_PROC_NULL
#endif
    ! Parallel input for serial files
    character(256), allocatable :: file_paths(:)
    character(30) :: variant_dim = 'N/A'
    integer file_start_idx, file_end_idx
    ! --------------------------------------------------------------------------
  contains
    procedure :: open => dataset_open
    procedure :: is_open => dataset_is_open
    procedure :: close => dataset_close
    procedure :: get_dim => get_dim_from_dataset
    procedure :: get_var => get_var_from_dataset
    final :: dataset_final
  end type dataset_type

  type dim_type
    integer id
    character(30) name
    character(256) long_name
    character(60) units
    integer size
    ! --------------------------------------------------------------------------
    ! Parallel IO
    logical :: decomp = .false.
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
    integer(4), pointer :: i4_missing_value => null()
    integer(8), pointer :: i8_missing_value => null()
    real(4), pointer :: r4_missing_value => null()
    real(8), pointer :: r8_missing_value => null()
    type(var_dim_type), allocatable :: dims(:)
  contains
    final :: var_final
  end type var_type

  type(hash_table_type) datasets

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

  interface fiona_quick_output
    module procedure fiona_quick_output_1d_r8
    module procedure fiona_quick_output_2d_r8
  end interface fiona_quick_output

  real(8) time_units_in_seconds
  character(30) :: time_units_str = 'N/A'
  character(30) :: start_time_str = 'N/A'

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

  end subroutine fiona_init

  subroutine fiona_create_dataset(dataset_name, desc, file_prefix, file_path, start_time, time_units, mpi_comm)

    character(*), intent(in) :: dataset_name
    character(*), intent(in), optional :: desc
    character(*), intent(in), optional :: file_prefix
    character(*), intent(in), optional :: file_path
    character(*), intent(in), optional :: start_time
    character(*), intent(in), optional :: time_units
    integer, intent(in), optional :: mpi_comm

    character(256) desc_, file_prefix_, file_path_
    type(dataset_type) dataset
    logical is_exist
    integer ierr

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

    if (datasets%hashed(dataset_name)) then
      call log_error('Already created dataset ' // trim(dataset_name) // '!')
    end if

    dataset%name = dataset_name
    dataset%desc = desc_
    call create_hash_table(table=dataset%atts)
    call create_hash_table(table=dataset%dims)
    call create_hash_table(table=dataset%vars)
    if (file_prefix_ /= '' .and. file_path_ == '') then
      dataset%file_prefix = trim(file_prefix_) // '.' // trim(dataset_name)
    else if (file_prefix_ == '' .and. file_path_ /= '') then
      dataset%file_path = file_path_
    end if
    dataset%mode = 'output'
#ifdef HAS_MPI
    if (present(mpi_comm)) then
      dataset%mpi_comm = mpi_comm
      call MPI_COMM_SIZE(mpi_comm, dataset%num_proc, ierr)
      call MPI_COMM_RANK(mpi_comm, dataset%proc_id, ierr)
    end if
#endif

    if (present(start_time) .and. present(time_units)) then
      select case (time_units)
      case ('days')
        dataset%time_units_in_seconds = 86400.0
      case ('hours')
        dataset%time_units_in_seconds = 3600.0
      case ('minutes')
        dataset%time_units_in_seconds = 60.0
      case ('seconds')
        dataset%time_units_in_seconds = 1.0
      case default
        call log_error('Invalid time_units ' // trim(time_units) // '!')
      end select
      dataset%start_time_str = start_time
      dataset%time_units_str = time_units
    else
      dataset%time_units_in_seconds = time_units_in_seconds
      dataset%start_time_str = start_time_str
      dataset%time_units_str = time_units_str
    end if

    call datasets%insert(trim(dataset%name) // '.' // trim(dataset%mode), dataset)

  end subroutine fiona_create_dataset

  subroutine fiona_open_dataset(dataset_name, file_path, file_paths, parallel, mpi_comm, variant_dim)

    character(*), intent(in) :: dataset_name
    character(*), intent(in), optional :: file_path
    character(*), intent(in), optional :: file_paths(:)
    logical, intent(in), optional :: parallel
    integer, intent(in), optional :: mpi_comm
    character(*), intent(in), optional :: variant_dim

    type(dataset_type) dataset
    integer ierr, tmp_id, unlimited_dimid, n1, n2, n3, i

    if (datasets%hashed(dataset_name)) then
      call log_error('Already created dataset ' // trim(dataset_name) // '!')
    end if

    dataset%name = dataset_name
    call create_hash_table(table=dataset%atts)
    call create_hash_table(table=dataset%dims)
    call create_hash_table(table=dataset%vars)
    dataset%mode = 'input'
    if (present(file_path)) then
      dataset%file_path = file_path
    else if (present(file_paths)) then
      dataset%file_paths = file_paths
      dataset%file_path = file_paths(1)
    else
      call log_error('fiona_open_dataset needs file_path or file_paths argument!', __FILE__, __LINE__)
    end if

#ifdef HAS_MPI
    if (present(mpi_comm)) then
      dataset%mpi_comm = mpi_comm
      if (merge(parallel, .false., present(parallel))) then
        ! Partition the files to each process.
        call MPI_COMM_SIZE(mpi_comm, dataset%num_proc, ierr)
        call MPI_COMM_RANK(mpi_comm, dataset%proc_id, ierr)
        n1 = size(file_paths) / dataset%num_proc
        n2 = mod(size(file_paths), dataset%num_proc)
        n3 = merge(n1 + 1, n1, dataset%proc_id < n2)
        dataset%file_start_idx = 1
        do i = 0, dataset%proc_id - 1
          dataset%file_start_idx = dataset%file_start_idx + merge(n1 + 1, n1, i < n2)
        end do
        dataset%file_end_idx = dataset%file_start_idx + n3 - 1
      end if
    end if
    ! Check unlimited dimension.
    ierr = NF90_OPEN(dataset%file_path, NF90_NOWRITE, tmp_id)
    call handle_error(ierr, 'Failed to open NetCDF file "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    ierr = NF90_INQUIRE(tmp_id, unlimitedDimId=unlimited_dimid)
    call handle_error(ierr, 'Failed to inquire unlimited dimension ID in "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    if (unlimited_dimid == -1) then
      if (present(variant_dim)) then
        dataset%variant_dim = variant_dim
      end if
    else
      ierr = NF90_INQUIRE_DIMENSION(tmp_id, unlimited_dimid, name=dataset%variant_dim)
      call handle_error(ierr, 'Failed to inquire unlimited dimension name in "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    end if
#endif

    call datasets%insert(trim(dataset%name) // '.' // trim(dataset%mode), dataset)

  end subroutine fiona_open_dataset

  subroutine fiona_add_att(dataset_name, name, value)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*), intent(in) :: value

    type(dataset_type), pointer :: dataset

    dataset => get_dataset(dataset_name, mode='output')

    call dataset%atts%insert(name, value)

  end subroutine fiona_add_att

  subroutine fiona_add_dim(dataset_name, name, long_name, units, size, add_var, decomp)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    character(*), intent(in), optional :: long_name
    character(*), intent(in), optional :: units
    integer, intent(in), optional :: size
    logical, intent(in), optional :: add_var
    logical, intent(in), optional :: decomp

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
    if (present(decomp)) then
      dim%decomp = decomp
    else
      dim%decomp = .false.
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
          case ('lev', 'ilev')
            dim%long_name = 'Vertical level'
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
          case ('lev', 'ilev')
            dim%units = '1'
          case ('time', 'Time')
            if (dataset%time_units_str == 'N/A') then
              dataset%time_units_str = 'hours'
              dataset%time_units_in_seconds = 3600.0d0
            end if
            if (dataset%start_time_str == 'N/A') then
              dataset%start_time_str = '1970-01-01'
            end if
            write(dim%units, '(A, " since ", A)') trim(dataset%time_units_str), trim(dataset%start_time_str)
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
      case ('real', 'real(4)', 'r4')
        var%data_type = NF90_FLOAT
      case ('real(8)', 'r8')
        var%data_type = NF90_DOUBLE
      case ('integer', 'integer(4)', 'i4')
        var%data_type = NF90_INT
      case ('integer(8)', 'i8')
        var%data_type = NF90_INT64
      case ('char', 'character', 'c')
        var%data_type = NF90_CHAR
      case default
        call log_error('Unknown data type ' // trim(data_type) // ' for variable ' // trim(name) // '!')
      end select
    end if

    if (present(missing_value)) then
      select type (missing_value)
      type is (integer(4))
        allocate(var%i4_missing_value)
        var%i4_missing_value = missing_value
      type is (integer(8))
        allocate(var%i8_missing_value)
        var%i8_missing_value = missing_value
      type is (real(4))
        allocate(var%r4_missing_value)
        var%r4_missing_value = missing_value
      type is (real(8))
        allocate(var%r8_missing_value)
        var%r8_missing_value = missing_value
      class default
        call log_error('Unknown missing_value type!', __FILE__, __LINE__)
      end select
    end if

    allocate(var%dims(size(dim_names)))
    do i = 1, size(dim_names)
      found = .false.
      call create_hash_table_iterator(dataset%dims, iter)
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
        call log_error('Unknown dimension ' // trim(dim_names(i)) // ' for variable ' // trim(name) // '!', __FILE__, __LINE__)
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
    integer i, ierr
#ifdef HAS_MPI
    integer status(MPI_STATUS_SIZE)
#endif

    dataset => get_dataset(dataset_name, mode='output')

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

    call apply_dataset_to_netcdf_master(file_path, dataset, new_file)

    ! Write time dimension variable.
    if (associated(dataset%time_var)) then
      if (.not. present(time_in_seconds)) then
        call log_error('Time in seconds is needed!', __FILE__, __LINE__)
      end if
      if (time_in_seconds /= dataset%time_in_seconds) then
        dataset%time_step = dataset%time_step + 1
        dataset%time_in_seconds = time_in_seconds
        ! Update time units because restart may change it.
        write(dataset%time_var%units, '(A, " since ", A)') trim(dataset%time_units_str), trim(dataset%start_time_str)
#ifdef HAS_MPI
        ierr = NF90_VAR_PAR_ACCESS(dataset%id, dataset%time_var%id, NF90_COLLECTIVE)
        call handle_error(ierr, 'Failed to set parallel access for variable time!', __FILE__, __LINE__)
#endif
        ierr = NF90_PUT_ATT(dataset%id, dataset%time_var%id, 'units', trim(dataset%time_var%units))
        call handle_error(ierr, 'Failed to add attribute to variable time!', __FILE__, __LINE__)
        ierr = NF90_PUT_VAR(dataset%id, dataset%time_var%id, [time_in_seconds / dataset%time_units_in_seconds], [dataset%time_step], [1])
        call handle_error(ierr, 'Failed to write variable time!', __FILE__, __LINE__)
      end if
    end if

  end subroutine fiona_start_output

  subroutine apply_dataset_to_netcdf_master(file_path, dataset, new_file, append)

    character(*), intent(in) :: file_path
    type(dataset_type), intent(inout) :: dataset
    logical, intent(in), optional :: new_file
    logical, intent(in), optional :: append

    type(hash_table_iterator_type) iter
    type(dim_type), pointer :: dim
    type(var_type), pointer :: var
    integer i, ierr
    integer, allocatable :: dimids(:)

    if (present(new_file)) then
      if (new_file) then
#ifdef HAS_MPI
        ierr = NF90_CREATE(file_path, ior(NF90_NETCDF4, NF90_MPIIO), dataset%id, comm=dataset%mpi_comm, info=MPI_INFO_NULL)
#else
        ierr = NF90_CREATE(file_path, NF90_NETCDF4, dataset%id)
#endif
        call handle_error(ierr, 'Failed to create NetCDF file to output!', __FILE__, __LINE__)
      else
#ifdef HAS_MPI
        ierr = NF90_OPEN(dataset%last_file_path, ior(NF90_NETCDF4, ior(NF90_WRITE, NF90_MPIIO)), dataset%id, comm=dataset%mpi_comm, info=MPI_INFO_NULL)
#else
        ierr = NF90_OPEN(dataset%last_file_path, ior(NF90_NETCDF4, NF90_WRITE), dataset%id)
#endif
        call handle_error(ierr, 'Failed to open NetCDF file to output!', __FILE__, __LINE__)
        ierr = NF90_REDEF(dataset%id)
        call handle_error(ierr, 'Failed to enter definition mode!', __FILE__, __LINE__)
      end if
    else
#ifdef HAS_MPI
      ierr = NF90_CREATE(file_path, ior(NF90_NETCDF4, NF90_MPIIO), dataset%id, comm=dataset%mpi_comm, info=MPI_INFO_NULL)
#else
      ierr = NF90_CREATE(file_path, NF90_NETCDF4, dataset%id)
#endif
      call handle_error(ierr, 'Failed to create NetCDF file to output!', __FILE__, __LINE__)
    end if
    ierr = NF90_PUT_ATT(dataset%id, NF90_GLOBAL, 'dataset', dataset%name)
    ierr = NF90_PUT_ATT(dataset%id, NF90_GLOBAL, 'desc', dataset%desc)
    ierr = NF90_PUT_ATT(dataset%id, NF90_GLOBAL, 'author', dataset%author)

    call create_hash_table_iterator(dataset%atts, iter)
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
        ierr = NF90_PUT_ATT(dataset%id, NF90_GLOBAL, iter%key, to_str(value))
      end select
      call iter%next()
    end do

    call create_hash_table_iterator(dataset%dims, iter)
    do while (.not. iter%ended())
      dim => dataset%get_dim(iter%key)
      ierr = NF90_INQ_DIMID(dataset%id, dim%name, dim%id)
      if (ierr /= NF90_NOERR) then
        ierr = NF90_DEF_DIM(dataset%id, dim%name, dim%size, dim%id)
        call handle_error(ierr, 'Failed to define dimension ' // trim(dim%name) // '!', __FILE__, __LINE__)
      end if
      call iter%next()
    end do

    call create_hash_table_iterator(dataset%vars, iter)
    do while (.not. iter%ended())
      var => dataset%get_var(iter%key)
      ierr = NF90_INQ_VARID(dataset%id, var%name, var%id)
      if (ierr /= NF90_NOERR) then
        allocate(dimids(size(var%dims)))
        do i = 1, size(var%dims)
          dimids(i) = var%dims(i)%ptr%id
        end do
        ierr = NF90_DEF_VAR(dataset%id, var%name, var%data_type, dimids(1:size(var%dims)), var%id)
        call handle_error(ierr, 'Failed to define variable ' // trim(var%name) // '!', __FILE__, __LINE__)
        deallocate(dimids)
        ierr = NF90_PUT_ATT(dataset%id, var%id, 'long_name', trim(var%long_name))
        ierr = NF90_PUT_ATT(dataset%id, var%id, 'units', trim(var%units))
        if (associated(var%i4_missing_value)) then
          ierr = NF90_PUT_ATT(dataset%id, var%id, '_FillValue', var%i4_missing_value)
        else if (associated(var%i8_missing_value)) then
          ierr = NF90_PUT_ATT(dataset%id, var%id, '_FillValue', var%i8_missing_value)
        else if (associated(var%r4_missing_value)) then
          ierr = NF90_PUT_ATT(dataset%id, var%id, '_FillValue', var%r4_missing_value)
        else if (associated(var%r8_missing_value)) then
          ierr = NF90_PUT_ATT(dataset%id, var%id, '_FillValue', var%r8_missing_value)
        end if
        call handle_error(ierr, 'Failed to put attribute _FillValue for variable ' // trim(var%name) // '!', __FILE__, __LINE__)
      end if
      call iter%next()
    end do

    ierr = NF90_ENDDEF(dataset%id)
    call handle_error(ierr, 'Failed to end definition!', __FILE__, __LINE__)

    if (present(new_file)) then
      if (new_file) then
        dataset%time_step = 0 ! Reset to zero!
        dataset%last_file_path = file_path
      end if
    else
      dataset%time_step = 0 ! Reset to zero!
      dataset%last_file_path = file_path
    end if

  end subroutine apply_dataset_to_netcdf_master

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

#ifdef HAS_MPI
    ierr = NF90_VAR_PAR_ACCESS(dataset%id, var%id, NF90_COLLECTIVE)
    call handle_error(ierr, 'Failed to set parallel access for variable ' // trim(var%name) // '!', __FILE__, __LINE__)
#endif
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

  subroutine fiona_output_1d(dataset_name, name, array, start, count)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*), intent(in) :: array(:)
    integer, intent(in), optional :: start(:)
    integer, intent(in), optional :: count(:)

    type(dataset_type), pointer :: dataset
    type(var_type), pointer :: var
    integer i, j, ierr
    integer start_(2), count_(2)

    dataset => get_dataset(dataset_name, mode='output')
    var => dataset%get_var(name)

    j = 1
    do i = 1, size(var%dims)
      if (var%dims(i)%ptr%size == NF90_UNLIMITED) then
        start_(i) = dataset%time_step
        count_(i) = 1
      else if (var%dims(i)%ptr%decomp .and. present(start) .and. present(count)) then
        start_(i) = start(j)
        count_(i) = count(j)
        j = j + 1
      else
        start_(i) = 1
        count_(i) = var%dims(i)%ptr%size
      end if
    end do

#ifdef HAS_MPI
    ierr = NF90_VAR_PAR_ACCESS(dataset%id, var%id, NF90_COLLECTIVE)
    call handle_error(ierr, 'Failed to set parallel access for variable ' // trim(var%name) // '!', __FILE__, __LINE__)
#endif
    select type (array)
    type is (integer)
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    type is (real(4))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    type is (character(*))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' in dataset ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine fiona_output_1d

  subroutine fiona_output_2d(dataset_name, name, array, start, count)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*), intent(in) :: array(:,:)
    integer, intent(in), optional :: start(:)
    integer, intent(in), optional :: count(:)

    type(dataset_type), pointer :: dataset
    type(var_type), pointer :: var
    integer i, j, ierr
    integer start_(3), count_(3)

    dataset => get_dataset(dataset_name, mode='output')
    var => dataset%get_var(name)

    j = 1
    do i = 1, size(var%dims)
      if (var%dims(i)%ptr%size == NF90_UNLIMITED) then
        start_(i) = dataset%time_step
        count_(i) = 1
      else if (var%dims(i)%ptr%decomp .and. present(start) .and. present(count)) then
        start_(i) = start(j)
        count_(i) = count(j)
        j = j + 1
      else
        start_(i) = 1
        count_(i) = var%dims(i)%ptr%size
      end if
    end do

#ifdef HAS_MPI
    ierr = NF90_VAR_PAR_ACCESS(dataset%id, var%id, NF90_COLLECTIVE)
    call handle_error(ierr, 'Failed to set parallel access for variable ' // trim(var%name) // '!', __FILE__, __LINE__)
#endif
    select type (array)
    type is (integer)
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    type is (real(4))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' to ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine fiona_output_2d

  subroutine fiona_output_3d(dataset_name, name, array, start, count)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*), intent(in) :: array(:,:,:)
    integer, intent(in), optional :: start(:)
    integer, intent(in), optional :: count(:)

    type(dataset_type), pointer :: dataset
    type(var_type    ), pointer :: var
    integer i, j, ierr
    integer start_(4), count_(4)

    dataset => get_dataset(dataset_name, mode='output')
    var => dataset%get_var(name)

    j = 1
    do i = 1, size(var%dims)
      if (var%dims(i)%ptr%size == NF90_UNLIMITED) then
        start_(i) = dataset%time_step
        count_(i) = 1
      else if (var%dims(i)%ptr%decomp .and. present(start) .and. present(count)) then
        start_(i) = start(j)
        count_(i) = count(j)
        j = j + 1
      else
        start_(i) = 1
        count_(i) = var%dims(i)%ptr%size
      end if
    end do

#ifdef HAS_MPI
    ierr = NF90_VAR_PAR_ACCESS(dataset%id, var%id, NF90_COLLECTIVE)
    call handle_error(ierr, 'Failed to set parallel access for variable ' // trim(var%name) // '!', __FILE__, __LINE__)
#endif
    select type (array)
    type is (integer)
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    type is (real(4))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' to ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine fiona_output_3d

  subroutine fiona_output_4d(dataset_name, name, array, start, count)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*)    , intent(in) :: array(:,:,:,:)
    integer, intent(in), optional :: start(:)
    integer, intent(in), optional :: count(:)

    type(dataset_type), pointer :: dataset
    type(var_type    ), pointer :: var
    integer i, j, ierr
    integer start_(4), count_(4)

    dataset => get_dataset(dataset_name, mode='output')
    var => dataset%get_var(name)

    j = 1
    do i = 1, size(var%dims)
      if (var%dims(i)%ptr%size == NF90_UNLIMITED) then
        start_(i) = dataset%time_step
        count_(i) = 1
      else if (var%dims(i)%ptr%decomp .and. present(start) .and. present(count)) then
        start_(i) = start(j)
        count_(i) = count(j)
        j = j + 1
      else
        start_(i) = 1
        count_(i) = var%dims(i)%ptr%size
      end if
    end do

#ifdef HAS_MPI
    ierr = NF90_VAR_PAR_ACCESS(dataset%id, var%id, NF90_COLLECTIVE)
    call handle_error(ierr, 'Failed to set parallel access for variable ' // trim(var%name) // '!', __FILE__, __LINE__)
#endif
    select type (array)
    type is (integer)
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    type is (real(4))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' to ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine fiona_output_4d
  
  subroutine fiona_output_5d(dataset_name, name, array, start, count)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    class(*)    , intent(in) :: array(:,:,:,:,:)
    integer, intent(in), optional :: start(:)
    integer, intent(in), optional :: count(:)

    type(dataset_type), pointer :: dataset
    type(var_type    ), pointer :: var
    integer i, j, ierr
    integer start_(5), count_(5)

    dataset => get_dataset(dataset_name, mode='output')
    var => dataset%get_var(name)

    j = 1
    do i = 1, size(var%dims)
      if (var%dims(i)%ptr%size == NF90_UNLIMITED) then
        start_(i) = dataset%time_step
        count_(i) = 1
      else if (var%dims(i)%ptr%decomp .and. present(start) .and. present(count)) then
        start_(i) = start(j)
        count_(i) = count(j)
        j = j + 1
      else
        start_(i) = 1
        count_(i) = var%dims(i)%ptr%size
      end if
    end do

#ifdef HAS_MPI
    ierr = NF90_VAR_PAR_ACCESS(dataset%id, var%id, NF90_COLLECTIVE)
    call handle_error(ierr, 'Failed to set parallel access for variable ' // trim(var%name) // '!', __FILE__, __LINE__)
#endif
    select type (array)
    type is (integer)
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    type is (real(4))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    type is (real(8))
      ierr = NF90_PUT_VAR(dataset%id, var%id, array, start_, count_)
    end select
    call handle_error(ierr, 'Failed to write variable ' // trim(name) // ' to ' // trim(dataset%name) // '!', __FILE__, __LINE__)

  end subroutine fiona_output_5d

  subroutine fiona_end_output(dataset_name)

    character(*), intent(in) :: dataset_name

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='output')

    call dataset%close()

  end subroutine fiona_end_output

  subroutine fiona_start_input(dataset_name, file_idx)

    character(*), intent(in) :: dataset_name
    integer, intent(in), optional :: file_idx

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    if (present(file_idx)) then
      dataset%file_path = dataset%file_paths(file_idx)
    end if
    call dataset%open()

  end subroutine fiona_start_input

  subroutine fiona_get_dim(dataset_name, name, size, start_idx, end_idx)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: name
    integer, intent(out), optional :: size
    integer, intent(out), optional :: start_idx
    integer, intent(out), optional :: end_idx

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

      call dataset%open()

      ierr = NF90_INQ_DIMID(dataset%id, name, dimid)
      call handle_error(ierr, 'Failed to inquire dimension ' // trim(name) // ' in NetCDF file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

      ierr = NF90_INQUIRE_DIMENSION(dataset%id, dimid, len=dim%size)
      call handle_error(ierr, 'Failed to inquire size of dimension ' // trim(name) // ' in NetCDF file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

      call dataset%close()
    end if
    if (present(size)) size = dim%size
    if (present(start_idx)) start_idx = dataset%file_start_idx
    if (present(end_idx)) end_idx = dataset%file_end_idx

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

    call dataset%open()

    ierr = NF90_GET_ATT(dataset%id, NF90_GLOBAL, att_name, value)
    call handle_error(ierr, 'Failed to get global attribute "' // trim(att_name) // '" from file ' // trim(dataset%file_path) // '!', __FILE__, __LINE__)

    call dataset%close()

  end subroutine fiona_get_att_r4

  subroutine fiona_get_att_r8(dataset_name, att_name, value)

    character(*), intent(in)  :: dataset_name
    character(*), intent(in)  :: att_name
    real     (8), intent(out) :: value

    type(dataset_type), pointer :: dataset
    integer ierr

    dataset => get_dataset(dataset_name, mode='input')

    call dataset%open()

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
  
  subroutine fiona_input_1d(dataset_name, var_name, array, start, count, time_step)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: var_name
    class(*), intent(out) :: array(:)
    integer, intent(in), optional :: start
    integer, intent(in), optional :: count
    integer, intent(in), optional :: time_step

    type(dataset_type), pointer :: dataset
    integer ierr, varid

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_INQ_VARID(dataset%id, var_name, varid)
    call handle_error(ierr, 'No variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    select type (array)
    type is (integer)
      if (present(time_step)) then
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array, start=[start,time_step], count=[count,1])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,time_step], count=[size(array),1])
        end if
      else
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array, start=[start], count=[count])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array)
        end if
      end if
    type is (real(4))
      if (present(time_step)) then
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array, start=[start,time_step], count=[count,1])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,time_step], count=[size(array),1])
        end if
      else
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array, start=[start], count=[count])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array)
        end if
      end if
    type is (real(8))
      if (present(time_step)) then
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array, start=[start,time_step], count=[count,1])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array, start=[1,time_step], count=[size(array),1])
        end if
      else
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array, start=[start], count=[count])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array)
        end if
      end if
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine fiona_input_1d

  subroutine fiona_input_2d(dataset_name, var_name, array, start, count, time_step)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: var_name
    class(*), intent(out) :: array(:,:)
    integer, intent(in), optional :: start(2)
    integer, intent(in), optional :: count(2)
    integer, intent(in), optional :: time_step

    type(dataset_type), pointer :: dataset
    real add_offset, scale_factor
    integer ierr, varid, xtype

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_INQ_VARID(dataset%id, var_name, varid)
    call handle_error(ierr, 'No variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

    ! Check variable type in file.
    ierr = NF90_INQUIRE_VARIABLE(dataset%id, varid, xtype=xtype)
    if (xtype == NF90_SHORT) then
      ierr = NF90_GET_ATT(dataset%id, varid, 'add_offset', add_offset)
      call handle_error(ierr, 'No add_offset attribute for  "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
      ierr = NF90_GET_ATT(dataset%id, varid, 'scale_factor', scale_factor)
      call handle_error(ierr, 'No scale_factor attribute for  "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    end if

    select type (array)
    type is (integer)
      if (present(time_step)) then
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array, &
            start=[start(1),start(2),time_step],        &
            count=[count(1),count(2),1])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array,      &
            start=[             1,             1,time_step], &
            count=[size(array, 1),size(array, 2),        1])
        end if
      else
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array, &
            start=[start(1),start(2)], count=[count(1),count(2)])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array)
        end if
      end if
    type is (real(4))
      if (present(time_step)) then
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array, &
            start=[start(1),start(2),time_step],        &
            count=[count(1),count(2),1])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array,      &
            start=[             1,             1,time_step], &
            count=[size(array, 1),size(array, 2),        1])
        end if
      else
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array, &
            start=[start(1),start(2)], count=[count(1),count(2)])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array)
        end if
      end if
      if (xtype == NF90_SHORT) then
        array = scale_factor * array + add_offset
      end if
    type is (real(8))
      if (present(time_step)) then
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array, &
            start=[start(1),start(2),time_step],        &
            count=[count(1),count(2),1])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array,      &
            start=[             1,             1,time_step], &
            count=[size(array, 1),size(array, 2),        1])
        end if
      else
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array, &
            start=[start(1),start(2)], count=[count(1),count(2)])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array)
        end if
      end if
      if (xtype == NF90_SHORT) then
        array = scale_factor * array + add_offset
      end if
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine fiona_input_2d

  subroutine fiona_input_3d(dataset_name, var_name, array, start, count, time_step)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: var_name
    class    (*), intent(out) :: array(:,:,:)
    integer     , intent(in ), optional :: start(3)
    integer     , intent(in ), optional :: count(3)
    integer     , intent(in ), optional :: time_step

    type(dataset_type), pointer :: dataset
    real add_offset, scale_factor
    integer ierr, varid, xtype

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_INQ_VARID(dataset%id, var_name, varid)
    call handle_error(ierr, 'No variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

    ! Check variable type in file.
    ierr = NF90_INQUIRE_VARIABLE(dataset%id, varid, xtype=xtype)
    if (xtype == NF90_SHORT) then
      ierr = NF90_GET_ATT(dataset%id, varid, 'add_offset', add_offset)
      call handle_error(ierr, 'No add_offset attribute for  "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
      ierr = NF90_GET_ATT(dataset%id, varid, 'scale_factor', scale_factor)
      call handle_error(ierr, 'No scale_factor attribute for  "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    end if

    select type (array)
    type is (integer)
      if (present(time_step)) then
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,   &
            start=[start(1),start(2),start(3),time_step], &
            count=[count(1),count(2),count(3),time_step])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array,                     &
            start=[             1,             1,             1,time_step], &
            count=[size(array, 1),size(array, 2),size(array, 3),         1])
        end if
      else
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,   &
            start=[start(1),start(2),start(3)],           &
            count=[count(1),count(2),count(3)])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array)
        end if
      end if
    type is (real(4))
      if (present(time_step)) then
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,   &
            start=[start(1),start(2),start(3),time_step], &
            count=[count(1),count(2),count(3),time_step])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array,                     &
            start=[             1,             1,             1,time_step], &
            count=[size(array, 1),size(array, 2),size(array, 3),         1])
        end if
      else
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,   &
            start=[start(1),start(2),start(3)],           &
            count=[count(1),count(2),count(3)])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array)
        end if
      end if
      if (xtype == NF90_SHORT) then
        array = scale_factor * array + add_offset
      end if
    type is (real(8))
      if (present(time_step)) then
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,   &
            start=[start(1),start(2),start(3),time_step], &
            count=[count(1),count(2),count(3),time_step])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array,                     &
            start=[             1,             1,             1,time_step], &
            count=[size(array, 1),size(array, 2),size(array, 3),         1])
        end if
      else
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,   &
            start=[start(1),start(2),start(3)],           &
            count=[count(1),count(2),count(3)])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array)
        end if
      end if
      if (xtype == NF90_SHORT) then
        array = scale_factor * array + add_offset
      end if
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine fiona_input_3d

  subroutine fiona_input_4d(dataset_name, var_name, array, start, count, time_step)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: var_name
    class    (*), intent(out) :: array(:,:,:,:)
    integer     , intent(in ), optional :: start(4)
    integer     , intent(in ), optional :: count(4)
    integer     , intent(in ), optional :: time_step

    type(dataset_type), pointer :: dataset
    real add_offset, scale_factor
    integer ierr, varid, xtype

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_INQ_VARID(dataset%id, var_name, varid)
    call handle_error(ierr, 'No variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

    ! Check variable type in file.
    ierr = NF90_INQUIRE_VARIABLE(dataset%id, varid, xtype=xtype)
    if (xtype == NF90_SHORT) then
      ierr = NF90_GET_ATT(dataset%id, varid, 'add_offset', add_offset)
      call handle_error(ierr, 'No add_offset attribute for  "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
      ierr = NF90_GET_ATT(dataset%id, varid, 'scale_factor', scale_factor)
      call handle_error(ierr, 'No scale_factor attribute for  "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    end if

    select type (array)
    type is (integer)
      if (present(time_step)) then
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,            &
            start=[start(1),start(2),start(3),start(4),time_step], &
            count=[count(1),count(2),count(3),count(4),        1])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array,                                    &
            start=[             1,             1,             1,             1,time_step], &
            count=[size(array, 1),size(array, 2),size(array, 3),size(array, 4),        1])
        end if
      else
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,  &
            start=[start(1),start(2),start(3),start(4)], &
            count=[count(1),count(2),count(3),count(4)])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array)
        end if
      end if
    type is (real(4))
      if (present(time_step)) then
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,            &
            start=[start(1),start(2),start(3),start(4),time_step], &
            count=[count(1),count(2),count(3),count(4),        1])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array,                                    &
            start=[             1,             1,             1,             1,time_step], &
            count=[size(array, 1),size(array, 2),size(array, 3),size(array, 4),        1])
        end if
      else
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,  &
            start=[start(1),start(2),start(3),start(4)], &
            count=[count(1),count(2),count(3),count(4)])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array)
        end if
      end if
      if (xtype == NF90_SHORT) then
        array = scale_factor * array + add_offset
      end if
    type is (real(8))
      if (present(time_step)) then
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,            &
            start=[start(1),start(2),start(3),start(4),time_step], &
            count=[count(1),count(2),count(3),count(4),        1])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array,                                    &
            start=[             1,             1,             1,             1,time_step], &
            count=[size(array, 1),size(array, 2),size(array, 3),size(array, 4),        1])
        end if
      else
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,  &
            start=[start(1),start(2),start(3),start(4)], &
            count=[count(1),count(2),count(3),count(4)])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array)
        end if
      end if
      if (xtype == NF90_SHORT) then
        array = scale_factor * array + add_offset
      end if
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine fiona_input_4d
  
  subroutine fiona_input_5d(dataset_name, var_name, array, start, count, time_step)

    character(*), intent(in ) :: dataset_name
    character(*), intent(in ) :: var_name
    class    (*), intent(out) :: array(:,:,:,:,:)
    integer     , intent(in ), optional :: start(5)
    integer     , intent(in ), optional :: count(5)
    integer     , intent(in ), optional :: time_step

    type(dataset_type), pointer :: dataset
    real add_offset, scale_factor
    integer ierr, varid, xtype

    dataset => get_dataset(dataset_name, mode='input')

    ierr = NF90_INQ_VARID(dataset%id, var_name, varid)
    call handle_error(ierr, 'No variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

    ! Check variable type in file.
    ierr = NF90_INQUIRE_VARIABLE(dataset%id, varid, xtype=xtype)
    if (xtype == NF90_SHORT) then
      ierr = NF90_GET_ATT(dataset%id, varid, 'add_offset', add_offset)
      call handle_error(ierr, 'No add_offset attribute for  "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
      ierr = NF90_GET_ATT(dataset%id, varid, 'scale_factor', scale_factor)
      call handle_error(ierr, 'No scale_factor attribute for  "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)
    end if

    select type (array)
    type is (integer)
      if (present(time_step)) then
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,                     &
            start=[start(1),start(2),start(3),start(4),start(5),time_step], &
            count=[count(1),count(2),count(3),count(4),count(5),        1])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array,                                                   &
            start=[             1,             1,             1,             1,             1,time_step], &
            count=[size(array, 1),size(array, 2),size(array, 3),size(array, 4),size(array, 5),1])
        end if
      else
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,           &
            start=[start(1),start(2),start(3),start(4),start(5)], &
            count=[count(1),count(2),count(3),count(4),count(5)])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array)
        end if
      end if
    type is (real(4))
      if (present(time_step)) then
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,                     &
            start=[start(1),start(2),start(3),start(4),start(5),time_step], &
            count=[count(1),count(2),count(3),count(4),count(5),        1])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array,                                                   &
            start=[             1,             1,             1,             1,             1,time_step], &
            count=[size(array, 1),size(array, 2),size(array, 3),size(array, 4),size(array, 5),1])
        end if
      else
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,           &
            start=[start(1),start(2),start(3),start(4),start(5)], &
            count=[count(1),count(2),count(3),count(4),count(5)])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array)
        end if
      end if
      if (xtype == NF90_SHORT) then
        array = scale_factor * array + add_offset
      end if
    type is (real(8))
      if (present(time_step)) then
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,                     &
            start=[start(1),start(2),start(3),start(4),start(5),time_step], &
            count=[count(1),count(2),count(3),count(4),count(5),        1])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array,                                                   &
            start=[             1,             1,             1,             1,             1,time_step], &
            count=[size(array, 1),size(array, 2),size(array, 3),size(array, 4),size(array, 5),1])
        end if
      else
        if (present(start) .and. present(count)) then
          ierr = NF90_GET_VAR(dataset%id, varid, array,           &
            start=[start(1),start(2),start(3),start(4),start(5)], &
            count=[count(1),count(2),count(3),count(4),count(5)])
        else
          ierr = NF90_GET_VAR(dataset%id, varid, array)
        end if
      end if
      if (xtype == NF90_SHORT) then
        array = scale_factor * array + add_offset
      end if
    class default
      call log_error('Unsupported array type!', __FILE__, __LINE__)
    end select
    call handle_error(ierr, 'Failed to read variable "' // trim(var_name) // '" in dataset "' // trim(dataset%file_path) // '"!', __FILE__, __LINE__)

  end subroutine fiona_input_5d

  subroutine fiona_end_input(dataset_name)

    character(*), intent(in) :: dataset_name

    type(dataset_type), pointer :: dataset

    dataset => get_dataset(dataset_name, mode='input')

    call dataset%close()

  end subroutine fiona_end_input

  subroutine fiona_quick_output_1d_r8(dataset_name, var_name, dim_names, array, long_name, units, file_prefix, time_in_seconds, new_file)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: var_name
    character(*), intent(in) :: dim_names(:)
    real(8), intent(in) :: array(:)
    character(*), intent(in), optional :: long_name
    character(*), intent(in), optional :: units
    character(*), intent(in), optional :: file_prefix
    real(8), intent(in), optional :: time_in_seconds
    logical, intent(in), optional :: new_file

    type(dataset_type), pointer :: dataset
    character(:), allocatable :: long_name_
    character(:), allocatable :: units_
    character(:), allocatable :: file_prefix_
    logical new_file_

    if (present(long_name)) then
      long_name_ = long_name
    else
      long_name_ = var_name
    end if
    if (present(units)) then
      units_ = units
    else
      units_ = ''
    end if
    if (present(file_prefix)) then
      file_prefix_ = file_prefix
    else
      file_prefix_ = 'fiona'
    end if

    if (datasets%hashed(trim(dataset_name) // '.output')) then
      dataset => get_dataset(dataset_name, mode='output')
      if (.not. dataset%dims%hashed(dim_names(1))) then
        call fiona_add_dim(dataset_name, dim_names(1), size=size(array, 1))
      end if
      if (.not. dataset%vars%hashed(var_name)) then
        call fiona_add_var(dataset_name, var_name, long_name_, units_, dim_names, data_type='real(8)')
      end if
      if (present(new_file)) then
        new_file_ = new_file
      else
        new_file_ = .false.
      end if
    else
      call fiona_create_dataset(dataset_name, 'Fiona quick output', file_prefix=file_prefix_)
      if (present(time_in_seconds)) call fiona_add_dim(dataset_name, 'time', add_var=.true.)
      call fiona_add_dim(dataset_name, dim_names(1), size=size(array, 1))
      call fiona_add_var(dataset_name, var_name, long_name_, units_, dim_names, data_type='real(8)')
      new_file_ = .true.
    end if

    call fiona_start_output(dataset_name, time_in_seconds, new_file_)
    call fiona_output(dataset_name, var_name, array)
    call fiona_end_output(dataset_name)

  end subroutine fiona_quick_output_1d_r8

  subroutine fiona_quick_output_2d_r8(dataset_name, var_name, dim_names, array, long_name, units, file_prefix, time_in_seconds, new_file)

    character(*), intent(in) :: dataset_name
    character(*), intent(in) :: var_name
    character(*), intent(in) :: dim_names(:)
    real(8), intent(in) :: array(:,:)
    character(*), intent(in), optional :: long_name
    character(*), intent(in), optional :: units
    character(*), intent(in), optional :: file_prefix
    real(8), intent(in), optional :: time_in_seconds
    logical, intent(in), optional :: new_file

    type(dataset_type), pointer :: dataset
    character(:), allocatable :: long_name_
    character(:), allocatable :: units_
    character(:), allocatable :: file_prefix_
    logical new_file_
    integer i

    if (present(long_name)) then
      long_name_ = long_name
    else
      long_name_ = var_name
    end if
    if (present(units)) then
      units_ = units
    else
      units_ = ''
    end if
    if (present(file_prefix)) then
      file_prefix_ = file_prefix
    else
      file_prefix_ = dataset_name
    end if

    if (datasets%hashed(trim(dataset_name) // '.output')) then
      dataset => get_dataset(dataset_name, mode='output')
      do i = 1, 2
        if (.not. dataset%dims%hashed(dim_names(i))) then
          call fiona_add_dim(dataset_name, dim_names(i), size=size(array, i))
        end if
      end do
      if (.not. dataset%vars%hashed(var_name)) then
        call fiona_add_var(dataset_name, var_name, long_name_, units_, dim_names, data_type='real(8)')
      end if
      if (present(new_file)) then
        new_file_ = new_file
      else
        new_file_ = .false.
      end if
    else
      call fiona_create_dataset(dataset_name, 'Fiona quick output', file_prefix=file_prefix_)
      if (present(time_in_seconds)) call fiona_add_dim(dataset_name, 'time', add_var=.true.)
      do i = 1, 2
        call fiona_add_dim(dataset_name, dim_names(i), size=size(array, i))
      end do
      call fiona_add_var(dataset_name, var_name, long_name_, units_, dim_names, data_type='real(8)')
      new_file_ = .true.
    end if

    call fiona_start_output(dataset_name, time_in_seconds, new_file_)
    call fiona_output(dataset_name, var_name, array)
    call fiona_end_output(dataset_name)

  end subroutine fiona_quick_output_2d_r8

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
      call log_error('Failed to get dataset ' // trim(dataset_name) // '.' // trim(mode_) // '!', __FILE__, __LINE__)
    end select

  end function get_dataset

  subroutine dataset_open(this)

    class(dataset_type), intent(inout) :: this

    integer ierr

    if (this%is_open()) call this%close()
#ifdef HAS_MPI
    ierr = NF90_OPEN(this%file_path, ior(NF90_NOWRITE, NF90_MPIIO), this%id, comm=this%mpi_comm, info=MPI_INFO_NULL)
#else
    ierr = NF90_OPEN(this%file_path, NF90_NOWRITE, this%id)
#endif
    if (ierr == -51) then ! Uknown file format error
      ierr = NF90_OPEN(this%file_path, NF90_NOWRITE, this%id)
    end if
    call handle_error(ierr, 'Failed to open NetCDF file "' // trim(this%file_path) // '"!', __FILE__, __LINE__)

  end subroutine dataset_open

  logical function dataset_is_open(this) result(res)

    class(dataset_type), intent(in) :: this

    integer ierr

    ierr = NF90_INQUIRE(this%id)
    res = ierr == NF90_NOERR

  end function dataset_is_open

  subroutine dataset_close(this)

    class(dataset_type), intent(inout) :: this

    integer ierr

    if (this%is_open()) then
      ierr = NF90_CLOSE(this%id)
      call handle_error(ierr, 'Failed to close file "' // trim(this%file_path) // '"!', __FILE__, __LINE__)
    end if

  end subroutine dataset_close

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
    class default
      call log_error('Variable ' // trim(var_name) // ' is not in dataset!', __FILE__, __LINE__)
    end select

  end function get_var_from_dataset

  subroutine dataset_final(this)

    type(dataset_type), intent(inout) :: this

    if (allocated(this%file_paths)) deallocate(this%file_paths)

  end subroutine dataset_final

  subroutine var_final(this)

    type(var_type), intent(inout) :: this

    if (associated(this%i4_missing_value)) deallocate(this%i4_missing_value)
    if (associated(this%i8_missing_value)) deallocate(this%i8_missing_value)
    if (associated(this%r4_missing_value)) deallocate(this%r4_missing_value)
    if (associated(this%r8_missing_value)) deallocate(this%r8_missing_value)
    if (allocated(this%dims)) deallocate(this%dims)

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
