# - Find Jemalloc

# Look for the jemalloc header
find_path(Jemalloc_INCLUDE_DIR
        NAMES jemalloc/jemalloc.h
        HINTS ${Jemalloc_ROOT} ENV Jemalloc_ROOT
        PATH_SUFFIXES include)

# Look for the jemalloc library
find_library(Jemalloc_LIBRARY
        NAMES jemalloc
        HINTS ${Jemalloc_ROOT} ENV Jemalloc_ROOT
        PATH_SUFFIXES lib)

set(Jemalloc_LIBRARIES ${Jemalloc_LIBRARY})
set(Jemalloc_INCLUDE_DIRS ${Jemalloc_INCLUDE_DIR})

mark_as_advanced(Jemalloc_INCLUDE_DIR Jemalloc_LIBRARY)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Jemalloc
        FOUND_VAR Jemalloc_FOUND
        REQUIRED_VARS Jemalloc_LIBRARY Jemalloc_INCLUDE_DIR)
