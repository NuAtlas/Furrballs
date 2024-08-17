# Furrballs
Basic Archiving and caching solution, It'll feature LZ4 on-the-fly compression, LRU eviction plan 
and Offset-based Caching. It's primary target is Game Engines, thus Furrballs aims to have as low latency
as possible.

The plan is to have a basic solution with minimal dependencies (Simply because Cachelib had to many 
dependencies)
# Setup
**Requirements:**

**-CMake**

**-C++17**

**-LZ4**

**-RocksDB**

**-64-bit System**

**-libnuma-dev for linux \*<sup>1</sup>**

<sup>*1</sup>: Build with NO_NUMA flag to avoid the requirement. *(Removes NUMA support)*

# AMP (Adaptive Memory Pooling): 

AMP employs a counter that increments on eviction when a cache entry is accessed. 
If this counter exceeds a specified threshold, the cache expands to accommodate more values. 
Each expansion also triggers an increment of a secondary counter. 
If this secondary counter reaches its threshold, 
the number of pages allocated in the expansion increases to prevent temporary fragmentation.

The memory pool introduces an indirection layer for pointers returned by the library, 
using virtual pointers that remain valid even if the underlying data is moved. 
This enables defragmentation without invalidating pointers, allowing the use of the upper bits for metadata.

**Note:** The defragmentation operation should block all threads. 
Consider researching non-blocking memory management solutions such as Hoard or TBB, 
or implementing incremental memory expansion using a separate thread.

**Important:** This approach assumes that very large memory usage or large pointers are not considered. 
If such large memory usage occurs, the system will break and may require an alternative approach that avoids using virtual pointers. 
This issue is relevant only for servers with very large memory available, which are not the target for this library.

Additionally, this solution is designed exclusively for 64-bit systems, as enforced by the build system.

# Coding Guidelines:

**Use 'const' on member functions and arguments when ever it's possible.**

**All functions should be noexcept unless they explicitly throw as an indication to user.**

**Everything should be designed with Multi-threading in mind.**

**When possible use atomic types/operations.**

**Allow for modularity and integration with and into other projects.**
(ex: allow for the use of a threadpool or a job system provided externally for burst)

**Only paging system is allowed to allocated memory, 
any allocation goes through it (or MemoryManager class) if independent buffer is needed**

**Exceptions are only for unrecoverable errors or rare uses** 
otherwise continue gracefully and inform user.

**In case of Exceptions:** 
- Rethrow if you didn't recover and don't catch.
- Rethrown exceptions should never be caught again.
- Have only one layer of try/except.
- Do NOT use catch to catch all exception types.
- Only handle exceptions that are supposed to be recovered from.
- Do not catch exceptions in multi-threaded execution.
- Do not catch Memory Exceptions thrown by the library.
- NEVER throw in constructor, if the constructor is throwing use a factory design instead. 
and let the factory return errors or (nullptr to indicate failure to initialize)
> This is to **guarantee** that all constructed objects are ***initialized*** correctly 
***without*** requiring additional checks after creation or else 'nullptr' is returned.
- Destructors should never under any cirumstance throw. Unlike destructors, they should not
be replaced by cleanup function that throws, simply put **DESTRUCTORS/CLEANUP MUST NEVER THROW!**
> This is to **guarantee** that **Cleanup** is always ***reliable***, 
***stable*** and callable ***without additional precaution***.

*In any other case Prioritize **performance and low latency** over **all**.*
> Mark code as critical to indicate that the guidelines are overriden. Do not abuse this.

### Notes:
Priorities are Unix(POSIX) and Windows.
macOS is not a priority.

>Because of limited use (none to be exact) or compatibility considerations, 
macOS-related issues will not be the primary focus. 
Specifically, the author will not engage with macOS-related issues, 
conform to macOS guidelines, or implement macOS-specific code. 
(This decision is based on a personal preference to avoid Apple products,
though POSIX compliance allows for indirect support.)
