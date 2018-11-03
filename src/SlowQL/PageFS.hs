module SlowQL.PageFS where
    import System.IO
    import Data.IORef
    import qualified Data.ByteString as B
    import qualified Data.Cache.LRU.IO as LRU
    import qualified Data.Cache.LRU as ILRU
    import Data.Maybe
    cacheCapacity=60000
    pageSize=8192
    data LRUCacheItem = LRUCacheItem {content :: !B.ByteString, dirty :: !Bool}
    type LRUCache=LRU.AtomicLRU Int LRUCacheItem

    createLRUCache :: IO LRUCache
    createLRUCache=LRU.newAtomicLRU (Just cacheCapacity)

    createLRUItem :: B.ByteString -> LRUCacheItem
    createLRUItem bs=LRUCacheItem{content=bs, dirty=False}

    createDirtyLRUItem :: B.ByteString -> LRUCacheItem
    createDirtyLRUItem bs=LRUCacheItem{content=bs, dirty=True}
    data DataFile = DataFile {file_handle :: !Handle, cache :: !LRUCache};
    openDataFile :: String -> IO DataFile
    openDataFile filename= do
        handle <- openFile filename ReadWriteMode
        lru<-createLRUCache
        return DataFile {file_handle=handle,cache=lru}
    closeDataFile :: DataFile -> IO()
    closeDataFile DataFile{file_handle=handle}= do
        -- Write Cache Back
        hClose handle
        return ()
    getFileSize :: DataFile -> IO Integer
    getFileSize DataFile{file_handle=handle}=hFileSize handle
    ensureFilePage :: Handle->Int->IO()
    ensureFilePage handle pid=do
        fsize<-hFileSize handle
        if toInteger(fsize)<toInteger((pid+1)*pageSize) then
            writeFilePage handle pid $ B.pack [0|a<-[1..8192]]
        else return ()
    readFilePage :: Handle->Int->IO B.ByteString
    readFilePage handle pid=do
        ensureFilePage handle pid
        hSeek handle AbsoluteSeek (toInteger(pid*pageSize))
        B.hGet handle pageSize
    writeFilePage :: Handle->Int->B.ByteString->IO()
    writeFilePage handle pid buffer=do
        --putStrLn("Write!")
        hSeek handle AbsoluteSeek (toInteger(pid*pageSize))
        B.hPut handle buffer
    tryRemoveLast :: LRUCache->Handle->IO()
    tryRemoveLast c handle=do
        current_size<-LRU.size c
        if toInteger(current_size)==cacheCapacity
            then do
                write_op<-LRU.pop c
                let Just(write_pid, LRUCacheItem {content=cont, dirty=dirt})=write_op
                if(dirt)
                    then do
                        writeFilePage handle write_pid cont
                        return ()
                    else return ()
            else
                return ()
    readPage :: DataFile->Int->IO B.ByteString
    readPage DataFile{file_handle=handle, cache=c} pid=do
        pair<-LRU.lookup pid c
        if  isJust pair
            then do
                let Just LRUCacheItem{content=val} = pair
                return val
            else do
                val<-readFilePage handle pid
                tryRemoveLast c handle
                LRU.insert pid (createLRUItem val) c
                return val

    writePage :: DataFile->Int->B.ByteString->IO()
    writePage DataFile{file_handle=handle, cache=c} pid val=do --This is only a write-back.
        pair<-LRU.delete pid c
        if isJust pair
            then do --
                LRU.insert pid (createDirtyLRUItem val) c
                return ()
            else do -- Allocate?
                tryRemoveLast c handle
                --putStrLn("Write!")
                LRU.insert pid (createDirtyLRUItem val) c
                return ()
    writeBackAll :: DataFile->IO()
    writeBackAll DataFile{file_handle=handle, cache=c}=
        LRU.modifyAtomicLRU' f c
        where f= \ imcache-> do{
            mapM (\ (pid, LRUCacheItem {content=val, dirty=dirt})->do{
                if dirt
                    then do
                        writeFilePage handle pid val
                        return ()
                    else do
                        return ()
            }) (ILRU.toList imcache);

            return imcache
        }
