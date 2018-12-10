#pragma once
#include <vector>
#include <cstring>
//Immutable data chunk for throwing back and forth FFI.
struct Buffer{
    char* _content;
};

extern "C"{
    char* unwrapBuffer(Buffer*);
    Buffer* wrapBuffer(char*, int);
    void freeBuffer(Buffer*);
}
//Callback comparator with buffer
typedef bool (Cmp)(Buffer*);