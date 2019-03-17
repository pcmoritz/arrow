def take(Array values, Array indices):
    cdef:
        CTakeOptions options
        shared_ptr[CArray] result

    with nogil:
        check_status(Take(_context(), deref(values.sp_array.get()),
                          deref(indices.sp_array.get()), options, &result))

    return pyarrow_wrap_array(result)
