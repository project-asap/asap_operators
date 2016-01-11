/* -*-C++-*-
 */

#ifndef INCLUDED_ASAP_MEMORY_H
#define INCLUDED_ASAP_MEMORY_H

namespace asap {

struct mm_ownership_policy {
    static const bool has_ownership = true;

    static const bool allocate = true;
    static const bool deallocate = true;
    static const bool assign = true;
    static const bool assign_rvalue = false;
};

struct mm_no_ownership_policy {
    static const bool has_ownership = false;

    static const bool allocate = false;
    static const bool deallocate = false;
    static const bool assign = false;
    static const bool assign_rvalue = false;
};

}

#endif // INCLUDED_ASAP_MEMORY_H
