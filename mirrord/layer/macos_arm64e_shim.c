#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>

// This function is executed when this library is loaded.
__attribute__((constructor))
void on_library_load() {
    const char *lib_path = getenv("MIRRORD_MACOS_ARM64_LIBRARY");

    if (lib_path && *lib_path) {
        dlopen(lib_path, RTLD_LAZY);
    }
}