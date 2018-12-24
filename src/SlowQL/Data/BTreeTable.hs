module SlowQL.Data.BTreeTable where
    import Data.IORef
    import Foreign.Storable
    import Foreign.Ptr
    import Foreign.Marshal.Utils
    --import Data.Iteratee.Iteratee
    import Conduit
    import Data.Conduit
    import qualified SlowQL.PageFS as FS
    import qualified Data.ByteString as BS 
    import Data.List.Split
    import Data.Maybe
    import Data.Word
    import Control.Monad.IO.Class
    import SlowQL.Utils
    import Control.Monad
    type ByteString=BS.ByteString
    type KeyExtractor=ByteString->ByteString
    type KeyComparator=ByteString->ByteString->Ordering
    type KeyHint=(KeyExtractor, KeyComparator)
    --data BTree=BTreeNode {index:: Int,  key::[ByteString], children :: [Int], parent :: Maybe Int, prevNode :: Maybe Int, nextNode :: Maybe Int} | 
    --           BTreeLeaf {index::Int,  value :: [[Word8]], prevLeaf:: Maybe Int, nextLeaf :: Maybe Int, parent :: Maybe Int} deriving(Show)
    --data BTree=BTreeNode {index :: Int, page :: FS.Page} | BTreeLeaf {index :: Int, page :: FS.Page} --Temporary data structure
    -- BTreeNode: size prev next keys children
    -- BTreeLeaf: size prev next values
    data BTreeHeader=NodeHeader {size :: Int, prev :: Int, next :: Int}
                                          | LeafHeader {size :: Int, prev :: Int, next :: Int}


    data BTreeTableStat=BTreeTableStat {firstLeaf :: Int, lastLeaf :: Int, root :: Int, lastPage :: Int, freePage :: Int}
    data BTreeTable=BTreeTable {file :: FS.DataFile, stat :: IORef BTreeTableStat, recordSize::Int, recordsPerLeaf::Int, keySize::Int, keysPerNode::Int}

    placeholder:: Int
    placeholder=0xffffffff;
    headerNode:: Word8
    headerNode=0x01
    headerLeaf :: Word8
    headerLeaf=0x00
    tagEmpty :: Int
    tagEmpty=0
    readHeader::Ptr a->IO BTreeHeader
    readHeader ptr=do
        a<-readByte ptr 0
        b<-readInt ptr 1
        c<-readInt ptr 5
        d<-readInt ptr 9
        return $ case a of
            headerLeaf ->LeafHeader b c d
            headerNode->NodeHeader b c d
    isLeaf :: BTreeHeader->Bool
    isLeaf LeafHeader{}=True
    isLeaf NodeHeader{}=False
    isNode :: BTreeHeader->Bool
    isNode =not . isLeaf
    writeHeader::BTreeHeader->Ptr a->IO ()
    writeHeader header ptr=do
        case header of
            NodeHeader{} -> writeByte ptr 0 headerNode
            LeafHeader{} -> writeByte ptr 0 headerLeaf
        writeInt ptr 1 (size header)
        writeInt ptr 5 (prev header)
        writeInt ptr 9 (next header)
    allocatePage :: BTreeTable->IO Int
    allocatePage table=do
        st<-readIORef $ stat table
        if freePage st==placeholder 
            then do
                let new_stat=st {lastPage=lastPage st+1}
                writeIORef (stat table) new_stat
                return $ lastPage st+1
            else do
                pg<-FS.getPage (file table) (freePage st)
                let go=flip readInt 0
                fp<-FS.inspectPage go pg
                writeIORef (stat table) (st {freePage=fp})
                return $ freePage st
    delPage :: BTreeTable->Int->IO ()
    delPage table pn=do
        st<-readIORef $ stat table
        pg<-FS.getPage (file table) pn
        let go p = writeInt p 0 (freePage st)
        FS.modifyPage go pg
        writeIORef (stat table) (st {freePage=pn})

    type IKey=ByteString
    type IValue=ByteString
    data InsertResult=InsertSuccess Int | InsertRequireSplit IKey Int Int
    data DeleteResult=DeleteFinish | DeleteMerged Int | DeleteJuggled IKey Int | DeleteFailed
{-
    --Find the position of the first element greater than or equal to given key.
    binsearch :: (Monad m)=>(Int->m Ordering)->Int->Int->m Int
    binsearch funct l r=do

    binsearch' :: (Monad m)=>(Int->m Ordering)->Int->Int->m Int
    binsearch' funct l r=do
        ret<-funct r
        if(ret==LT) -- val is less than r
            then if l==r then return l else return r
            else return r
    insert :: BTreeTable->KeyHint->IValue->Bool->IO Bool
    insert table (fext, fcmp) val allowdup=do
        st<-readIORef $ stat table
        let key=fext val
        let ccmp=fcmp key
        let step pn=do
                pg<-FS.getPage (file table) pn
                header<-FS.inspectPage readHeader pg
                if isNode header --Need recursive insertion, note that after recursion "pg" may has been invalid
                    then --scale down for a value
                        
                        let find_leaf ptr=do
                                let key_start=ptr `plusPtr` 13
                                let pcmp n=do
                                        s<-FS.sliceBSOff 0 (keySize table) (key_start `plusPtr` (n*(keySize table)))
                                        return $ ccmp s
                                v<-binsearch pcmp 0 (size header)
                        FS.inspectPage find_leaf pg
                    else
                        if (size header) == (recordsPerLeaf table)
                            then do -- The page is already full and we need a split
                                    let left_size=(recordsPerLeaf table+1)`quot` 2
                                    let right_size=recordsPerLeaf table+1-left_size
                                    new_pn<-allocatePage table
                                    pg<-FS.getPage (file table) pn
                                    
                                    let gol ptr=do
                                            let lheader=header {size=left_size, next=new_pn}
                                            writeHeader lheader ptr
                                            FS.sliceBSOff  (13+left_size*recordSize table) (right_size*(recordSize table)) ptr
                                    right_part<-FS.modifyPage gol pg
                                    let gor ptr=do
                                            let rheader=LeafHeader right_size pn (next header)
                                            writeHeader rheader ptr
                                            FS.writeBSOff 13 right_part ptr
                                    newpg<-FS.getPage (file table) new_pn
                                    FS.modifyPage gor newpg
                                    when (pn==lastLeaf st) $ writeIORef (stat table) (st {lastLeaf=new_pn})
                                    return $ InsertRequireSplit (BS.take (recordSize table) right_part) pn new_pn
                            else do -- Can still insert into the page
                                    let op ptr=do
                                            let f n=do
                                                        let offset=13+n*(recordSize table)
                                                        nv<-FS.sliceBSOff offset (recordSize table) ptr
                                                        return $ ccmp (fext nv)
                                            
                                            after<-binsearch f 0 (size header-1)
                                            -- Move other records one unit forward
                                            let src=ptr `plusPtr` (13+(after+1)*(recordSize table))
                                            let dst=src `plusPtr` (recordSize table)
                                            moveBytes dst src ((size header-after)*(recordSize table))
                                            FS.writeBSOff 0 val src --Insert the record
                                            writeHeader (header {size=(size header+1)}) ptr
                                    FS.modifyPage op pg
                                    return $ InsertSuccess pn

        result<-step $ root st
        return True
    delete :: BTreeTable->KeyHint->IValue->IO Bool
    delete table (fext, fcmp) val=return False
-}


    initialize :: String->Int->Int->IO ()
    initialize path rsz ksz=do
        file<-FS.openDataFile path
        let records_per_leaf=(FS.pageSize-13) `quot` rsz
        let keys_per_node=(FS.pageSize-13-4) `quot` (ksz+4)
        st<-newIORef $ BTreeTableStat 1 1 1 1 placeholder
        flushMetadata $ BTreeTable file st  rsz records_per_leaf ksz keys_per_node
        FS.getPage file 1
        homepage<-FS.getPage file 2
        FS.closeDataFile file

    flushMetadata :: BTreeTable->IO ()
    flushMetadata table=do
        st<-readIORef $ stat table
        metadata<-FS.getPage (file table) 0
        let f p=do
                writeInt p 0 $ root st
                writeInt p 4 $ firstLeaf st
                writeInt p 8 $ lastLeaf st
                writeInt p 12 $ recordSize table
                writeInt p 16 $ recordsPerLeaf table
                writeInt p 20 $ keySize table
                writeInt p 24 $ keysPerNode table
                writeInt p 28 $ lastPage st
                writeInt p 32 $ freePage st
        FS.modifyPage f metadata

    open :: String->IO BTreeTable
    open path=do
        file<-FS.openDataFile path
        metadata<-FS.getPage file 0
        let f p=do
                root<-readInt p 0
                firstleaf<-readInt p 4
                lastleaf<-readInt p 8
                recordsize<-readInt p 12
                recordsperleaf<-readInt p 16
                keysize<-readInt p 20
                keyspernode<-readInt p 24
                lp<-readInt p 28
                fp<-readInt p 32
                st<-newIORef $ BTreeTableStat firstleaf lastleaf root lp fp
                return $ BTreeTable file st recordsize recordsperleaf keysize keyspernode 
        FS.inspectPage f metadata
    close :: BTreeTable->IO ()
    close table=do
        flushMetadata table
        FS.closeDataFile $ file table

    