module SlowQL.PageFS where
    import System.IO
    import Data.IORef
    --import qualified Data.ByteString as B
    import qualified Data.Cache.LRU.IO as LRU
    import qualified Data.Cache.LRU as ILRU
    import qualified Data.ByteArray as BA
    import qualified Data.ByteString.Lazy as B
    import System.Directory
    import Data.Word
    import Data.Maybe
    cacheCapacity=1
    pageSize=8192
    type DataChunk = BA.Bytes

    
    data LRUCacheItem = LRUCacheItem {content :: !DataChunk, dirty :: !Bool} deriving (Show)
    type LRUCache=LRU.AtomicLRU Int LRUCacheItem
    

    emptyChunk :: DataChunk
    emptyChunk=BA.pack [0|a<-[1..pageSize]]


    createLRUCache :: IO LRUCache
    createLRUCache=LRU.newAtomicLRU (Just cacheCapacity)

    createLRUItem :: DataChunk -> LRUCacheItem
    createLRUItem bs=LRUCacheItem{content=bs, dirty=False}

    createDirtyLRUItem :: DataChunk -> LRUCacheItem
    createDirtyLRUItem bs=LRUCacheItem{content=bs, dirty=True}
    data DataFile = DataFile {file_handle :: !Handle, cache :: !LRUCache} 
    instance Show DataFile where
        show a="<DataFile>"
    
    exists :: String->IO Bool
    exists=doesFileExist
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
            writeFilePage handle pid emptyChunk
        else return ()
    hPut :: BA.ByteArrayAccess ba=>Handle->ba->IO()
    hPut handle arr=BA.withByteArray arr (\p->hPutBuf handle p (BA.length arr))
    hGet :: Handle->Int->IO DataChunk
    hGet handle len=BA.alloc len (\p->do hGetBuf handle p len; return ())
        
    readFilePage :: Handle->Int->IO DataChunk
    readFilePage handle pid=do
        ensureFilePage handle pid
        hSeek handle AbsoluteSeek (toInteger(pid*pageSize))
        hGet handle pageSize
    writeFilePage :: Handle->Int->DataChunk->IO()
    writeFilePage handle pid buffer=do
        --putStrLn("Write!")
        hSeek handle AbsoluteSeek (toInteger(pid*pageSize))
        hPut handle buffer
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
    readPage :: DataFile->Int->IO DataChunk
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

    writePage :: DataFile->Int->DataChunk->IO()
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
    iterate :: DataChunk->Int->[Word8] 
    iterate chunk start
        | start<0 = []
        | toInteger start >=toInteger (BA.length chunk)= []
        | otherwise = map (BA.index chunk) [start..BA.length chunk -1]
    
    compact :: B.ByteString->Maybe DataChunk
    compact bs=let old_chunk=BA.pack (B.unpack bs)
               in if BA.length old_chunk>pageSize then Nothing
               else Just $ BA.concat [old_chunk, BA.zero (pageSize-(BA.length old_chunk))::BA.Bytes]

    {-
    class Iterator iter where
        at :: iter->Word8
        next :: iter->iter
        done :: iter->Bool
    data DataChunkIterator = DataChunkIterator DataChunk Int | DataChunkEnd

    instance Iterator DataChunkIterator where
        at (DataChunkIterator chunk i)=BA.index chunk i
        next (DataChunkIterator chunk i)=iterateDataChunk chunk (i+1)
        done (DataChunkIterator chunk i)=False
        done DataChunkEnd =True
    iterateDataChunk :: DataChunk->Int->DataChunkIterator
    iterateDataChunk chunk i=if BA.length chunk<=i then DataChunkEnd else DataChunkIterator chunk i

    data DataChunkIteratorUnsafe = DataChunkIteratorUnsafe DataChunk Int

    instance Iterator DataChunkIteratorUnsafe where
        at (DataChunkIteratorUnsafe chunk i)=BA.index chunk i
        next (DataChunkIteratorUnsafe chunk i)=iterateDataChunkUnsafe chunk (i+1)
        done _=False
    iterateDataChunkUnsafe :: DataChunk->Int->DataChunkIteratorUnsafe
    iterateDataChunkUnsafe=DataChunkIteratorUnsafe

    iteratorToList :: Iterator iter=>iter->[Word8]
    iteratorToList iter=if done iter then [] else at iter : iteratorToList ( next iter)
    toList :: DataChunk->Int->[Word8]
    toList a b=iteratorToList $ iterateDataChunk a b
    -}