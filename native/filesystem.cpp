#include "filesystem.h"
PageFile::PageFile(const char* fname){
    file=fopen(fname, "rb+");
}
PageFile::~PageFile(){
    evictAll();
    fclose(file);
}
Buffer PageFile::onLoad(int index){
    fseek(file, PAGE_SIZE*index, SEEK_SET);
    char* buf=new char[PAGE_SIZE];
    fread(buf, PAGE_SIZE, 1, file);
    Buffer b;
    b._content=buf;
    return b;
};
void PageFile::onDropOut(int index, Buffer val){
    delete val._content;
};
void PageFile::onWriteBack(int index, Buffer val){
    fseek(file, PAGE_SIZE*index, SEEK_SET);
    fwrite(val._content, PAGE_SIZE, 1, file);
};