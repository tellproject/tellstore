# - Find Sparsehash

# Look for the sparsehash header
find_path(Sparsehash_INCLUDE_DIR
        NAMES sparsehash/sparsetable
        HINTS ${Sparsehash_ROOT} ENV Sparsehash_ROOT
        PATH_SUFFIXES include)

set(Sparsehash_INCLUDE_DIRS ${Sparsehash_INCLUDE_DIR})

mark_as_advanced(Sparsehash_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Sparsehash
        FOUND_VAR Sparsehash_FOUND
        REQUIRED_VARS Sparsehash_INCLUDE_DIR)
