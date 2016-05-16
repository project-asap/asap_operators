/*
 * Copyright 2016 EU Project ASAP 619706.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

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
