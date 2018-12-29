{-
Linear table that can store some fixed-length data and provides an enumerate-everything-from-head enumerator.
-}
module SlowQL.Data.LinearTable where
    import Data.IORef
    import Foreign.Storable
    import Foreign.Ptr
    --import Data.Iteratee.Iteratee
    import Conduit
    import Data.Conduit
    import qualified SlowQL.PageFS as FS
    import qualified Data.ByteString as BS 
    import Data.List.Split
    import Data.Bits
    import Data.Maybe
    import Data.Word
    import Data.Binary.Get as Get
    import Control.Monad.IO.Class
    import SlowQL.Utils
    type ByteString=BS.ByteString
    data LinearTableStat=LinearTableStat{currentItems :: Int, freeStackPtr :: Maybe Int} deriving (Show)
    data LinearTable=LinearTable {file :: FS.DataFile, stat :: IORef LinearTableStat, recordSize :: Int, recordsPerPage :: Int}
    instance Show LinearTable where
        show table="<LinearTable recordSize="++(show $ recordSize table)++" recordsPerPage="++(show $ recordsPerPage table)++" >"
    type Updater=ByteString->IO (Maybe ByteString)
    type Deleter=ByteString->IO Bool
    type MoveHandler=(Int, ByteString, Int)->IO ()
    printStats :: LinearTable->IO()
    printStats table=(readIORef $ stat table) >>= (putStrLn.show)
    encodeMaybe :: a->Maybe a->a
    encodeMaybe dft Nothing=dft
    encodeMaybe _ (Just v)=v
    decodeMaybe ::Eq a=>a->a->Maybe a
    decodeMaybe d a=if a==d then Nothing else (Just a)
    tagEndOfList :: Int
    tagEndOfList=0xffffffff
    tagNotEmpty :: Int
    tagNotEmpty=0xfffffffe

    type BinaryHook=(Int->ByteString->ByteString->IO ())
    type UnitaryHook=((ByteString, Int)->IO ())
    
    unitaryId :: UnitaryHook
    unitaryId _=return ()
    binaryId :: BinaryHook
    binaryId _ _ _=return ()

    totalItems :: LinearTable->IO Int
    totalItems table=do
        s<-readIORef $ stat table
        return $ currentItems s
    writeToEmptyPos :: ByteString->LinearTable->IO Int
    writeToEmptyPos dat table=do
        st<-readIORef $ stat table
        (newstat, inserted_index)<-if isJust $ freeStackPtr st then
            do
                let Just index=freeStackPtr st
                let (pn, id)=(quot index $ recordsPerPage table, rem index $ recordsPerPage table)
                page<-FS.getPage (file table) (pn+1)
                let start=id*((recordSize table)+4)
                let f p=do
                            newptr<-peekElemOff (castPtr (p `plusPtr` start) :: Ptr Word32) 0
                            pokeElemOff (castPtr (p `plusPtr` start):: Ptr Word32) 0 (fromIntegral (tagNotEmpty))
                            FS.writeBSOff (start+4) dat p 
                            return $ decodeMaybe tagEndOfList (fromIntegral newptr)
                newptr<-FS.modifyPage f page
                return (LinearTableStat (currentItems st) newptr, index)
        else
            do
                let index=currentItems st
                let (pn, id)=(quot index $ recordsPerPage table, rem index $ recordsPerPage table)
                --print (recordsPerPage table, recordSize table, pn, id, BS.length dat)
                page<-FS.getPage (file table) (pn+1)
                let start=id*((recordSize table)+4)
                let f p=do
                            pokeElemOff (castPtr (p `plusPtr` start):: Ptr Word32) 0 (fromIntegral (tagNotEmpty) )
                            FS.writeBSOff (start+4) dat p
                --print "A"
                newptr<-FS.modifyPage f page
                --print "B"
                return (LinearTableStat (index+1) Nothing, index)
        writeIORef (stat table) newstat
        return inserted_index
    initialize :: String->Int->IO ()
    initialize path recordsize=do
        file<-FS.openDataFile path
        metadata<-FS.getPage file 0
        let recordsperpage=quot FS.pageSize (recordsize+4)
        let f p=do
                    pokeElemOff (castPtr p) 0 (fromIntegral (recordsize) :: Word32)
                    pokeElemOff (castPtr p) 1 (fromIntegral recordsperpage :: Word32)
                    pokeElemOff (castPtr p) 2 (0::Word32)
                    pokeElemOff (castPtr p) 3 (fromIntegral tagEndOfList :: Word32)
        FS.modifyPage f metadata
        FS.closeDataFile file
    open :: String->IO LinearTable
    open path=do
        file<-FS.openDataFile path
        metadata<-FS.getPage file 0
        let f p=do
                    recordsize<-peekElemOff (castPtr p :: Ptr Word32) 0
                    recordsperpage<-peekElemOff (castPtr p :: Ptr Word32) 1
                    currentitems<-peekElemOff (castPtr p :: Ptr Word32) 2
                    freestackptr<-peekElemOff (castPtr p :: Ptr Word32) 3
                    stat<-newIORef $ LinearTableStat (fromIntegral currentitems) (decodeMaybe tagEndOfList (fromIntegral freestackptr))
                    return $ LinearTable file stat (fromIntegral recordsize) (fromIntegral recordsperpage)
        FS.inspectPage f metadata
    
    flushMetadata::LinearTable->IO()
    flushMetadata table=do
        metadata<-FS.getPage (file table) 0
        st<-readIORef $ stat table
        let currentitems=currentItems st
        let fsp=encodeMaybe tagEndOfList $ freeStackPtr st
        let f p=do
                    pokeElemOff (castPtr p :: Ptr Word32) 2 (fromIntegral currentitems)
                    pokeElemOff (castPtr p :: Ptr Word32) 3 (fromIntegral fsp)
        FS.modifyPage f metadata
    close :: LinearTable->IO ()
    close table=do
        flushMetadata table
        FS.closeDataFile $ file table
    
    
    iteratePages :: (Int->Int->IO ())->LinearTable->IO()
    iteratePages task table=do
        n<-totalItems table
        let full_pages=n `div` (recordsPerPage table)
        let last_page_size=n `rem` (recordsPerPage table)
        mapM_ (flip task $ recordsPerPage table) [0..full_pages-1] 
        if last_page_size>0 then task full_pages last_page_size else return ()

    update :: Updater->LinearTable->(Int->ByteString->ByteString->IO ())->IO Int
    update f table hook=do
        counter<-newIORef 0
        let task pn ni=do
                            
                            page<-FS.getPage (file table) (pn+1)
                            let record_size=recordSize table
                            let trymodify i p=do
                                    header<-peekElemOff (castPtr (p `plusPtr` (i*(record_size+4))) :: Ptr Word32) 0
                                    if (fromIntegral header)==tagNotEmpty then do
                                        bs<-FS.sliceBSOff (i*(record_size+4)+4) record_size p
                                        nbs<-f bs
                                        if isJust nbs then do
                                            let Just jnbs=nbs
                                            hook (pn*recordsPerPage table+i) bs jnbs
                                            modifyIORef' counter (+1)
                                            FS.writeBSOff (i*(record_size+4)+4) jnbs p
                                        else return ()
                                    else return ()
                            let total_op p=mapM_ (`trymodify` p) [0..ni-1]
                            FS.modifyPage total_op page
        iteratePages task table
        readIORef counter
    delete :: Deleter->LinearTable->((ByteString, Int)->IO ())->IO Int
    delete f table hook=do
        counter<-newIORef 0
        let task pn ni=do
                            
                            page<-FS.getPage (file table) (pn+1)
                            let record_size=recordSize table
                            let trydelete i p=do
                                    header<-peekElemOff (castPtr (p `plusPtr` (i*(record_size+4))) :: Ptr Word32) 0
                                    if (fromIntegral header)==tagNotEmpty then do
                                        bs<-FS.sliceBSOff (i*(record_size+4)+4) record_size p
                                        todelete<-f bs
                                        if todelete then do
                                            modifyIORef' counter (+1)
                                            st<-readIORef $ stat table
                                            let stack_top=encodeMaybe tagEndOfList $ freeStackPtr st
                                            pokeElemOff (castPtr (p `plusPtr` (i*(record_size+4))) :: Ptr Word32) 0 (fromIntegral stack_top)
                                            let remove_index=pn*(recordsPerPage table)+i
                                            writeIORef (stat table) (LinearTableStat (currentItems st) (Just remove_index))
                                            hook (bs, remove_index)
                                        else return ()
                                    else return ()
                            let total_op p=mapM_ (`trydelete` p) [0..ni-1]
                            FS.modifyPage total_op page
        iteratePages task table
        readIORef counter
    insert :: ByteString->LinearTable->((ByteString, Int)->IO ())->IO ()
    insert bs table hook=do
        idx<-writeToEmptyPos bs table
        hook (bs, idx)

    enumerate :: LinearTable->ConduitT () ByteString IO ()
    enumerate lt=enumerateWithIdx lt .| (mapC snd)
    enumerateWithIdx :: LinearTable->ConduitT () (Int, ByteString) IO ()
    enumerateWithIdx table=do
        liftIO $ putStrLn "Table Begin!"
        (full_pages, last_page_size)<-liftIO $ do
                n<-totalItems table
                let full_pages=n `div` (recordsPerPage table)
                let last_page_size=n `rem` (recordsPerPage table)
                return (full_pages, last_page_size)
        let rpp=recordsPerPage table
        let rs=recordSize table
        let read_page pn sz=do
                --liftIO $ putStrLn "Page Begin!"
                page<-FS.getPage (file table) (pn+1)
                let fetchItems n p=if n<=0 
                        then 
                            return [] 
                        else
                            do
                                chunk<-FS.sliceBSOff 0 ((rs+4)*n) p
                                let items=map (\x->(BS.splitAt 4) $ (rangeBS (x*(rs+4)) (rs+4)) chunk) [0..(n-1)]
                                let get_header hdr=fromIntegral((fromIntegral $ BS.index hdr 0:: Word32) 
                                                .|. ((fromIntegral $ BS.index hdr 1:: Word32) `shiftL` 8)
                                                .|. ((fromIntegral $ BS.index hdr 2:: Word32) `shiftL` 16)
                                                .|. ((fromIntegral $ BS.index hdr 3:: Word32) `shiftL` 24))
                                let filtered_items=filter (\(h, t)->get_header h==tagNotEmpty) $ zipWith (\i (h,t)->(h, (i, t))) [(pn*rpp)..] items
                                return $ map snd filtered_items
                FS.inspectPage (fetchItems sz) page
        let loop pn=do
                if(pn>full_pages || (pn==full_pages && last_page_size==0))
                    then return ()
                    else do
                        items<-liftIO $ read_page pn (if pn==full_pages then last_page_size else recordsPerPage table)
                        mapM_ yield items
                        loop (pn+1)
        loop 0
                
    -- Linear enumerator
    {-
    enumerate :: LinearTable->Enumerator [BS.ByteString] IO a
    enumerate table iter=
        let task pn ni=do
                page<-FS.getPage (file table) pn
                record_size<-recordSize table
                let sendelem i p=do
                        header<-peekElemOff (castPtr (p `plusPtr` (i*(record_size+4))) :: Ptr Word32) 0
                        if (fromIntegral header)==tagNotEmpty then do
                            bs<-FS.sliceBSOff (i*(record_size+4)+4) record_size p
                            enumChunk [bs] iter
                        else 
                            return iter
                let step i it p=
                        if i==ni then 
                            enumEof it
                        else do
                                (done, t)<-enumCheckIfDone it
                                if done then return t
                                else do
                                        sendelem i p
                                        step (i+1) t p
                let send_page=step 0 iter
                FS.inspectPage send_page page
                return ()
        in iteratePages task table
    -}