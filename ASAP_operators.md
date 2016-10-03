This document describes the ASAP library of data analytics operators and the Swan parallel programming language on which it is built.

# The Swan Parallel Programming Language

Swan is an extension to the Cilk parallel programming language,
which was originally designed at the Supercomputing Technologies group at MIT,
and is currently supported by Intel
under the name [Cilk Plus](https://www.cilkplus.org).
Swan extends Cilk by adding dataflow dependences to express more complex
parallel patterns than Cilk. One of these is pipeline parallelism.
Moreover, Swan adds annotations to parallel for loops that help to
increase performance.

## The Cilk Parallel Programming Language

The following is a brief overview of the key aspects of Cilk.
Full details are provided
[online](https://www.cilkplus.org/cilk-documentation-full).

### Spawn and Sync

Parallelism is expressed by indicating that two (or more) pieces of code
may execute in parallel. Typically, this implies that these pieces of code
do not write to variables or memory locations that the other reads or writes to.
These pieces of code may be any legal C/C++ code. In practice, Cilk requires
that at least one piece of code is extracted in a function or isolated
in a C++ lambda expression (an anonymous function).

Parallelism is introduced by adding the `cilk_spawn` keyword to the function
call statement. We say that the function is _spawned_ rather than _called_.
The spawned function may execute in parallel with the remainder
of the calling function. This is called the _continuation_ of the calling
function. The parallelism exists until
a `cilk_sync` statement is encountered, or until the end of the calling
function, whichever is encountered first.

The Cilk runtime is allowed to execute the spawned function with the
continuation of the calling function, but is not obliged to do so.
In fact, the Cilk runtime only executes in parallel as many spawns
as is required to keep all CPU cores busy. Beyond this, it executes
the spawned statements in a sequential manner, as this is much more efficient.
Adding spawn statements to a program, thus, has little overhead in case
they are not selected for parallel execution by the runtime.

The following is an example of Quicksort expressed in Cilk ([source: https://github.com/tjd/sqlite_cpp/blob/master/cilk_test/qsort/qsort.cilk](https://github.com/tjd/sqlite_cpp/blob/master/cilk_test/qsort/qsort.cilk)):

```
void qsort(int *begin, int *end) {  
    if (begin != end) {  
        --end;  // Exclude last element (pivot) from partition  
        int * middle = std::partition(begin, end,  
                          std::bind2nd(std::less<int>(), *end));  
        std::swap(*end, *middle);    // move pivot to middle  
        cilk_spawn qsort(begin, middle);  
        qsort(++middle, ++end); // Exclude pivot and restore end  
        cilk_sync;  
    }  
}
```

Quicksort first partitions the range of values to sort using a pivot.
It then recursively sorts the range of values less than the pivot
and the range of values larger than the pivot. As these subranges
are independent (non-overlapping), they can be sorted in parallel.
This is indicated by labelling the first recursive call with `cilk_spawn`.

Note that one could also add the `cilk_spawn` keyword to the second
recursive call. This is however redundant as it was already apparent that
this call may execute in parallel with the first recursive call.

### Parallel for loops
The spawn/sync mechanism is very versatile. It can be used to express
parallel for loops, a common idiom, as well. However, this is somewhat
tedious. The Cilk compiler allows programmers to annotate parallel for
loops using the `cilk_for` keyword, e.g.:

```
cilk_for(int i=0; i < n; ++i) {
     a[i] = ...;
}
```

The compiler outlines the body of a `cilk_for` loop in a distinct function
and generates code to call the loop body in parallel, using the `cilk_spawn`
statement.

Every iteration of the loop should modify distinct memory locations.
There are moreover restrictions on the structure of the loop iteration.
In essence, the number of iterations of the loop must be known at execution
time just before starting the loop. This implies that the loop should not
have `break` statements and that the loop iteration variable (`i`) is
modified only by the loop increment statement (`++i`, the third part in the
for loop syntax).

### Generalized Reductions
Cilk provides definitions for generalized reductions
that are associative but not necessarily commutative.
As the reduction operation need not be commutative, many operations
such as list prepend/append and hash-map insert can now be expressed
as reduction operations. In these cases it is guaranteed that the reduction
variable contains the same value as computed by the serial elision
of the Cilk program.

Cilk defines reductions with three components:
a data type, an associative operation and an identity value for that operation.
These components are defined in a monoid class that serves as
the basis for a _reducer_ class definition.

The following is the definition of a Cilk reducer
for a hash-map data type:
```
template<class map_type>
class map_reducer {
   struct Monoid : cilk::monoid_base<map_type> (-*\label{lst:red:map:monoid}*-){
      static void reduce(map_type * left, map_type * right) {(-*\label{lst:red:map:reduce}*-)
         for(typename map_type::const_iterator
             I=right->cbegin(), E=right->cend(); I != E; ++I)
            (*left)[I->first] += I->second;
         right->clear();
      }
      static void identity(map_type * p) const {(-*\label{lst:red:map:identity}*-)
         new (p) map_type();
      }
   };
   cilk::reducer<Monoid> imp_;(-*\label{lst:red:map:imp}*-)

public:
   map_reducer() : imp_() { }
   typename map_type::value_type & operator[](
          const typename map_type::key_type & key) {
      return imp_.view()[key];
   }
   typename map_type::const_iterator cbegin() {
      return imp_.view().cbegin();
   }
   typename map_type::const_iterator cend() {
      return imp_.view().cend();
   }
   void swap(map_type & other) {
      return imp_.view().swap(other);
   }
   map_type & get_value() {
      return imp_.view();
   }
};
```

It is assumed that the template parameter
`map_type` defines a hash-map type that
is compatible to the C++ standard's `std::map`.
The definition consists of a Monoid class,
which defines the base type
(through the `monoid_base` template parameter),
the identity
value (through an initialization function) and the reduction function.
It is assumed that hash-maps are reduced
by taking the join of all keys and that the values for common keys
are further reduced using an `operator +=`.
This behavior is specified in the `reduce` function.

The runtime system dynamically creates copies of the reduction variable,
and reduces those copies as needed. These copies are called _views_.
A view is created for a worker thread when it first accesses
the reduction variable. The view is initialized with the identity element.
The worker retains the view when spawning a task. When
an idle worker steals a continuation from another worker's deque, it does
not receive a view for that reduction variable. The view is created only on the
first access to the reduction variable.
When a worker completes a spawned task leaving its spawn deque
empty, or when a worker executes a `cilk_sync` statement,
the view is reduced with that of a sibling task.

The example above defines a `map_reducer` class.
The member value `imp_` is declared as an instance of the
`reducer` class, specialized by the
`Monoid` definition.
The object `imp_` manages the creation, lookup and destruction of views.
The `map_reducer` class further provides access to the underlying
view through the `operator []` in order to add items to the
hash-map.

The `map_reducer` class may be used in parallel code as follows:
```
map_reducer<std::map<std::string,size_t>> map;
cilk_for(std::vector<std::string>::const_iterator
         I=vec.cbegin(); I != vec.cend(); ++I) {
   map[*I]++;
}
```

The `cilk_for` construct
creates parallelism. Each concurrently executing loop iteration
references the same instance of the `map_reducer` class,
but the `cilk::reducer` object `imp_` serves up
different views in concurrently executing iterations.
All views are reduced prior to
completion of the `cilk_for` loop.

Note that the reduction operation should ideally execute in
constant-time, otherwise the execution time of the program will depend
on the number of reduction operations performed.
The number of reduction operations is, in any case,
proportional to the number of steal operations.

### Array Notation

Cilk Plus supports an array notation that facilitates
auto-vectorization, i.e., the use of SIMD vector instructions
to accelerate processing. The array notation allows
for 3 fields in an array section expression: `a[i:l:s]`, where
`i` is the start index of the array section,
`l` is the length and
`s` is the stride. Each element of the array notation is optional,
but at least one colon must be present.
Default values are
0 for `i`,
the length of the array for `l`, provided it is known at compile-time,
and 1 for `s`.
E.g., `a[:]` indicates the full array if its size is statically known,
while `a[:10:2]` indicates the elements at indices 0, 2, 4, 6, 8.

Expressions may be built up using array notations, e.g.,
the statement
`c[:] = a[:]+2*b[:];` is equivalent to
```
for(int i=0; i < n; ++i)
    c[i] = a[i] + 2*b[i];
```
assuming each array was declared with length `n`.

One can also map functions over all elements of an array section.
E.g., `a[:] = pow(b[:])` applies the function `pow` to each
element of array `b`
and stores the result in the corresponding element of array `a`.
Reductions are specified using built-in functions that may be applied
to arbitrary array sections. E.g., 
`__sec_reduce_add(a[::2])` returns the sum of the array elements
at even positions of `a`.

The key advantage of the array notation is that it enables the compiler
to auto-vectorize the code. Vectorization can be important towards performance
as map-reduce programs often exhibit a data streaming pattern.

## Swan's Extensions to Cilk

### Dataflow dependences

Dependencies are tracked at the object level.
An object must be declared as a `versioned` object
in order to enable dependency tracking.
Versioned objects
support automatic tracking of dependencies as well as creating
new versions of the object in order to increase task-level parallelism
(a.k.a. renaming).

Dependency tracking is enabled on tasks that
take particular types as arguments:
the `indep`, `outdep` and `inoutdep` types.
These types are little more than a wrapper around
a versioned object that extends its type with the memory access mode
of the task: input, ouput or input/output (in/out).
The language allows only to pass versioned objects to such
arguments.

When spawning a task, the scheduler analyzes the signature of the spawned
procedure for arguments with a memory access mode. If none of the arguments
describe a memory access mode, then the spawn statement is an
_unconditional spawn_ and it has the same semantics as a
Cilk spawn.
Otherwise, the spawn statement 
is a _conditional spawn_.
The memory accesses of the task are tracked and,
depending on runtime conditions, the task either executes
immediately or it is queued up in a set of pending tasks.

The `sync` statement in our language has the same semantics
as the Cilk sync statement: it postpones the execution of a procedure 
until all child tasks have finished execution.

We consider only situations where dependencies are tracked between the
children of a single parent procedure. Each dynamic procedure instance
may have a task graph that restricts the execution order of its children.
This restriction ensures that
all parallel executions compute the same value as the sequential
elision of the program.
Note that the sequential elision of the program always respects the dependencies
in the program: by deducing dependencies from input/output properties,
there can never be backward dependencies in the sequential elision.
Furthermore, by having multiple independent task graphs in a program,
we can mitigate the performance impact of building the task graph in
serial fashion.

Our model allows arbitrarily mixing fork/join style and task graph execution.
The only problematic issue to allow this is that we must take care
when nesting task graphs, in particular when passing versioned objects
across multiple dependent spawns. To make this work correctly, we must
use distinct metadata for every dependent spawn
to track its dependencies separately.

The following is an example of square matrix multiplication expressed
in Swan using runtime tracking and enforcement of task dependencies.
Here, the matrix multiplication is performed _by blocks_, i.e., matrices
are partitioned in sub-blocks and parallelism between operations
on sub-blocks is made explicit using data-flow annotations.

```
typedef float (*block_t)[16]; // 16x16 tile
typedef swan::versioned<float[16][16]> vers_block_t;
typedef swan::indep<float[16][16]> in_block_t;
typedef swan::inoutdep<float[16][16]> inout_block_t;

void mul_add(in_block_t A, in_block_t B, inout_block_t C) {
    block_t a = (block_t)A; // Recover pointers
    block_t b = (block_t)B; // to the raw data
    block_t c = (block_t)C; // from the versioned objects
    // ... serial implementation on a 16x16 tile ...
}

void matmul(vers_block_t * A, vers_block_t * B,
             vers_block_t * C, unsigned n) {
    for( unsigned i=0; i < n; ++i ) {
        for( unsigned j=0; j < n; ++j ) {
            for( unsigned k=0; k < n; ++k ) {
                cilk_spawn mul_add( (in_block_t)A[i*n+j],
                                 (in_block_t)B[j*n+k],
                                 (inout_block_t)C[i*n+k] );
            }
        }
    }
    cilk_sync;
}
```

### Performance hints for parallel for loops

Swan adds two annotations to `cilk_for` loops. These annotations
inform the runtime how to schedule tasks on CPU cores most efficiently.

The _NUMA_ annotation facilitates performance tuning for systems with
a Non-Uniform Memory Architecture (NUMA), e.g., multi-socket machines.
This annotation indicates that the iterations of the loop should be
scheduled on distinct NUMA domains (sockets). It is the programmer's
responsibility to ensure that there are no more loop iterations than
NUMA domains.

Example usage:
```
    int chunk = (len + num_numa_domains - 1) / num_numa_domains;
#pragma cilk numa(strict)
    cilk_for(int d=0; d < num_numa_domains; ++d)
    	cilk_for(int i=d*chunk; i < std::min((d+1)*chunk,len); ++i)
	    a[i] = ...;
```

In the example above, the outer loop (with loop iteration variable `d`)
is annotated as a NUMA loop. Each iteration of this loop will be executed
on a distinct NUMA domain. The iterations of the inner loop
(using loop iteration variable `i`) are spread over the CPU cores of
one the NUMA domain of the corresponding `d` value.


Swan furthermore accelerates execution of fine-grain parallel loops through
an alternative runtime with much lower overhead compared to the Cilk runtime.
Fine-grain loops should currently not be nested inside
other parallel constructs.
They can be invoked as follows:

```
#pragma cilk finegrain
    cilk_for(int i=0; i < n; ++i) {
	 a[i] = ...;
    }
```


# The ASAP operator library for text analytics

## Data types

### Vector types

### Data set types

### Word banks

## Input/output formats

### WEKA file format

## Analytics Operators

### Term frequency

### TF-IDF

### K-Means clustering
