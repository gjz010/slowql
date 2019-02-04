module SlowQL.PageFS where
    import System.IO
    import Data.IORef
    --import qualified Data.ByteString as B
    import qualified Data.Cache.LRU.IO as LRU
    import qualified Data.Cache.LRU as ILRU
    import qualified Data.ByteArray as BA
    import qualified Data.ByteString.Lazy as B
    import qualified Data.ByteString as BS
    import qualified Foreign.C.String as CStr
    import System.Directory
    import Data.Word
    import Data.Maybe
    import System.IO.Unsafe

    import qualified Foreign.Marshal.Alloc as FAlloc
    import qualified Foreign.Marshal.Array as FArray
    import qualified Foreign.Storable as FStore
    import qualified Foreign.Marshal.Utils as FUtils
    import Foreign.Ptr
    data Page=Page {raw :: Ptr Word8, rdirty :: IORef Bool}
    instance Show Page where
        show p="<Page>"
    
    withPage :: (Ptr Word8->IO (a, Bool))->Page->IO a
    withPage f page=do
        (a, modified)<-f $ raw page
        modifyIORef' (rdirty page) (||modified)
        return a
    modifyPage :: (Ptr Word8->IO a)->Page->IO a
    modifyPage m=let f= \p->do
                        ret<-m p
                        return (ret, True)
                      in withPage f
    inspectPage :: (Ptr Word8->IO a)->Page->IO a
    inspectPage m=let f= \p->do
                        ret<-m p
                        return (ret, False)
                    in withPage f
    dirty :: Page->IO Bool
    dirty=readIORef . rdirty
    
    emptyPage :: Int->Bool->IO Page
    emptyPage size dty=do
        d<-newIORef dty
        --print "FALLoc.callocBytes"
        r<-FAlloc.callocBytes (size)
        --print "Alloc done"
        return $ Page r d
    freePage :: Page->IO()
    freePage=(FAlloc.free).raw

    sliceBS :: Int->Ptr a->IO BS.ByteString
    sliceBS size p=BS.packCStringLen (castPtr p, size)
    
    sliceBSOff :: Int->Int->Ptr a->IO BS.ByteString
    sliceBSOff offset size p=sliceBS size (p `plusPtr` offset)

    writeBSOff :: Int->BS.ByteString->Ptr a->IO ()
    writeBSOff offset bs p=BS.useAsCStringLen bs (uncurry $ FUtils.copyBytes (p `plusPtr` offset)) 
                                                
    writeLBSOff :: Int->B.ByteString->Ptr a->IO ()
    writeLBSOff offset lbs p=(B.foldlChunks go (return offset) lbs) >>= const (return ())
                                where go ofs chunk=do
                                            o<-ofs
                                            writeBSOff o chunk p
                                            return ((BS.length chunk)+o)
    cacheCapacity=256
    pageSize=8192
    -- type DataChunk = BA.Bytes
    --type DataChunk=BS.ByteString
    
    --data LRUCacheItem = LRUCacheItem {content :: !DataChunk, dirty :: !Bool} deriving (Show)
    --type LRUCache=LRU.AtomicLRU Int LRUCacheItem
    type LRUCache=LRU.AtomicLRU Int Page

    --emptyChunk :: DataChunk
    --emptyChunk=BS.pack [0|a<-[1..pageSize]]
    createLRUCache :: IO LRUCache
    createLRUCache=LRU.newAtomicLRU (Just cacheCapacity)

    --createLRUItem :: DataChunk -> LRUCacheItem
    --createLRUItem bs=LRUCacheItem{content=bs, dirty=False}

    --createDirtyLRUItem :: DataChunk -> LRUCacheItem
    --createDirtyLRUItem bs=LRUCacheItem{content=bs, dirty=True}
    data DataFile = DataFile {file_handle :: Handle, cache :: LRUCache} 
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
    closeDataFile file= do
        -- Write Cache Back
        writeBackAll file
        hClose $file_handle file
        return ()
    getFileSize :: DataFile -> IO Integer
    getFileSize DataFile{file_handle=handle}=hFileSize handle
    {-ensureFilePage :: Handle->Int->IO Page
    ensureFilePage handle pid=do
        fsize<-hFileSize handle
        if toInteger(fsize)<toInteger((pid+1)*pageSize) then
            writeFilePage handle pid emptyChunk
        else return ()
    hPut :: Handle->DataChunk->IO()
    hPut handle arr=BS.hPut handle arr
    hGet :: Handle->Int->IO DataChunk
    hGet handle len=BS.hGet handle len
       -} 
    readFilePage :: Handle->Int->IO Page
    readFilePage handle pid=do
        fsize<-hFileSize handle
        if toInteger(fsize)<toInteger((pid+1)*pageSize) then do
            --putStrLn "Empty!"
            --putStrLn "Alloc"
            p<-emptyPage pageSize True
            --putStrLn "Success"
            return p
        else do
            --putStrLn "Read!"
            hSeek handle AbsoluteSeek (toInteger(pid*pageSize))
            page<-emptyPage pageSize False
            inspectPage (\ptr->do
                    hGetBuf handle ptr pageSize
                ) page
            return page
    writeFilePage' :: Bool->Handle->Int->Page->IO()
    writeFilePage' dofree handle pid page=do
        --putStrLn("Write!")
        hSeek handle AbsoluteSeek (toInteger(pid*pageSize))
        inspectPage (\ptr->do
                hPutBuf handle ptr pageSize
            ) page
        if dofree then freePage page else return ()
    writeFilePage :: Handle->Int->Page->IO()
    writeFilePage=writeFilePage' False
    tryRemoveLast :: LRUCache->Handle->IO()
    tryRemoveLast c handle=do
        current_size<-LRU.size c
        if toInteger(current_size)==cacheCapacity
            then do
                write_op<-LRU.pop c
                let Just(write_pid, page)=write_op
                dirt<-dirty page
                if(dirt)
                    then writeFilePage' True handle write_pid page
                    else return ()
            else
                return ()
    --Now the page is mutable and everyone is happy.
    getPage :: DataFile->Int->IO Page
    getPage DataFile{file_handle=handle, cache=c} pid=do
        --putStrLn ("Getting "++(show pid))
        page<-LRU.lookup pid c
        if isJust page then
            do
                --putStrLn "Hit!"
                let Just jp=page in return jp
        else do
            newpage<-readFilePage handle pid
            --putStrLn "R"
            tryRemoveLast c handle
            --putStrLn "R"
            LRU.insert pid newpage c
            return newpage
    writeBackAll' :: Bool->DataFile->IO()
    writeBackAll' dofree DataFile{file_handle=handle, cache=c}=do
        l<-LRU.toList c
        let f (pid, page)= do
                dirt<-dirty page
                if dirt then do
                    --putStrLn $ "Writing back a page! "++(show pid)
                    writeFilePage' dofree handle pid page
                    writeIORef (rdirty page) False
                else return ()--putStrLn $"Not dirty "++(show pid)
        mapM_ f l
    writeBackAll :: DataFile->IO()
    writeBackAll=writeBackAll' False
    --readPageLazy :: DataFile->Int->IO DataChunk
    --readPageLazy a b= unsafeInterleaveIO $ readPage a b
    {-
    readPage :: DataFile->Int->IO Ptr
    readPage DataFile{file_handle=handle, cache=c} pid=do
        pair<-LRU.lookup pid c
        if  isJust pair
            then do
                let Just Page{raw=ptr} = pair
                return ptr
            else do
                val<-readFilePage handle pid
                tryRemoveLast c handle
                LRU.insert pid (createLRUItem val) c
                return val
    -}
    {-
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
    -}

    {-
    iterate :: DataChunk->Int->[Word8] 
    iterate chunk start=BS.unpack $ BS.drop start chunk
--        | start<0 = []
--        | toInteger start >=toInteger (BA.length chunk)= []
--        | otherwise = map (BA.index chunk) [start..BA.length chunk -1]
    compact :: B.ByteString->Maybe DataChunk
    compact bs=if (fromIntegral $ B.length bs)<=pageSize then Just $ B.toStrict $ B.concat [bs, B.fromStrict $ BS.take (pageSize-(fromIntegral $ B.length bs)) emptyChunk] else Nothing
                --let old_chunk=BA.pack (B.unpack bs)
               --in if BA.length old_chunk>pageSize then Nothing
               --else Just $ BA.concat [old_chunk, BA.zero (pageSize-(BA.length old_chunk))::BA.Bytes]
    -}
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