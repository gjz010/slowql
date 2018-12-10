// Buffer-based table storage.
// Offers low-level storage as "linked table".
// Innocent about high-level types and values.
#include "defs.h"
#include "filesystem.h"

class Table{
private:
    PageFile f;
public:
    
    Table(const char* name);
    

};