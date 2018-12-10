#include "defs.h"

char* unwrapBuffer(Buffer* buf){
    return buf->_content;
}

Buffer* wrapBuffer(char* content, int size){
    Buffer* buf=new Buffer;
    char* mem=new char[size];
    memcpy(mem, content, size);
    buf->_content=mem;
    return buf;
}

void freeBuffer(Buffer* buf){
    delete buf;
}

