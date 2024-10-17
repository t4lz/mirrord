/*
 * This is compiled to a dynamic library with arm64e architecture, that loads the arm64 mirrord layer.
 * Support for creating arm64e binaries in Rust is not yet good enough, so in order to load the layer into arm64e
 * binaries, we load this library that loads our arm64 layer library.
 */


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