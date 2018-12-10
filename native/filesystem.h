#include "defs.h"
#include <vector>
#include <queue>
#include <map>
#include <cstdio>
template <class T>
class LRUCache{
public:
    struct CacheItem{
        T val;
        bool dirty;
    };

protected:
    virtual T onLoad(int index){};
    virtual void onDropOut(int index, T val){};
    virtual void onWriteBack(int index, T val){};
    //virtual void afterClose();
private:
    void evict(int index, const T& val, bool dirty){
        if(dirty){
            onWriteBack(index, val);
        }
        onDropOut(index, val);
    }
    std::map<int, CacheItem> _cache;
    struct _lrunode{
        int older;
        int newer;
    };
    std::map<int, _lrunode> _lru;
    int _old=-1;
    int _new=-1;
    int maxsize=1000;
    void insertInternal(int n, T v){
        _lrunode node;
        node.older=_new;
        node.newer=-1;
        if(_new!=-1){
            auto& latest=_lru.find(_new)->second;
            latest.newer=n;
        }
        node.older=_new;
        node.newer=-1;
        _lru.insert(std::make_pair(n, node));
        CacheItem item;
        item.val=v;
        item.dirty=false;
        _cache.insert(std::make_pair(n, item));
        if(size==maxsize){
            auto& onode=_lru.find(_old)->second;
            if(onode.newer!=-1){
                auto& sonode=_lru.find(onode.newer)->second;
                sonode.older=-1;
            }
            int nw=onode.newer;
            _lru.erase(_old);
            CacheItem oval=_cache[_old];
            evict(_old, oval.val, oval.dirty);
            //ptr->release(_old);
            _cache.erase(_old);
            _old=nw;
        }
    }
    void pump(int n){
        auto& node=_lru.find(n)->second;
        if(node.older!=-1){
            auto& onode=_lru.find(node.older)->second;
            onode.newer=node.newer;
        }
        if(node.newer!=-1){
            auto& nnode=_lru.find(node.newer)->second;
            nnode.older=node.older;
        }
        if(_new==n) _new=node.older;
        if(_old==n) _old=node.newer;

        if(_new!=-1){
            auto& latest=_lru.find(_new)->second;
            latest.newer=n;
        }
        node.older=_new;
        node.newer=-1;
        _new=n;
    }
public:
    T get(int index){
        if(_cache.count(index)){
            pump(index);
            return _cache[index].val;
        }else{
            T val=onLoad(index);
            insertInternal(index, val);
            return val;
        }
    }
    //void insert(int index, const T& value);
    void taint(int index){
        _cache.find(index)->second.ditry=true;
        pump(index);
    }
    void flush(){
        for(auto iter=_cache.begin();iter!=_cache.end();iter++){
            auto& val=iter->second;
            if(val.dirty){
                onWriteBack(iter->first, val.val);
                val.dirty=false;
            }
        }
    }
    void evictAll(){
        for(auto iter=_cache.begin();iter!=_cache.end();iter++){
            evict(iter->first, iter->second.val, iter->second.dirty);
        }
    }
    ~LRUCache(){

    }
};
#define PAGE_SIZE 8192

class PageFile : public LRUCache<Buffer>{
public:
    
private:
    FILE* file;
public:
    PageFile(const char* fname){
        file=fopen(fname, "rb+");
    }
    ~PageFile(){
        evictAll();
        fclose(file);
    }
protected:
    Buffer onLoad(int index){
        fseek(file, PAGE_SIZE*index, SEEK_SET);
        char* buf=new char[PAGE_SIZE];
        fread(buf, PAGE_SIZE, 1, file);
        Buffer b;
        b._content=buf;
        return b;
    };
    void onDropOut(int index, Buffer val){
        delete val._content;
    };
    void onWriteBack(int index, Buffer val){
        fseek(file, PAGE_SIZE*index, SEEK_SET);
        fwrite(val._content, PAGE_SIZE, 1, file);
    };
};