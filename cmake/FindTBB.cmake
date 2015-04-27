find_path(TBB_INCLUDE_DIR tbb/atomic.h)
find_library(TBB_LIBRARY NAMES tbb libtbb)

set(TBB_LIBRARIES ${TBB_LIBRARY})
set(TBB_INCLUDE_DIRS ${TBB_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(TBB DEFAULT_MSG TBB_LIBRARY TBB_INCLUDE_DIR)
mark_as_advanced(TBB_INCLUDE_DIR TBB_LIBRARY)
