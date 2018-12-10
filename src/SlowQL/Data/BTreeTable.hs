module SlowQL.Data.BTreeTable where
    import Data.IORef
    import Foreign.Storable
    import Foreign.Ptr
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
    type ByteString=BS.ByteString
    type KeyExtractor=ByteString->ByteString
    type KeyComparator=ByteString->ByteString->Bool
    --data BTree=BTreeNode {index:: Int,  key::[ByteString], children :: [Int], parent :: Maybe Int, prevNode :: Maybe Int, nextNode :: Maybe Int} | 
    --           BTreeLeaf {index::Int,  value :: [[Word8]], prevLeaf:: Maybe Int, nextLeaf :: Maybe Int, parent :: Maybe Int} deriving(Show)
    data BTree=BTreeNode {index :: Int, page :: FS.Page} | BTreeLeaf {index :: Int, page :: FS.Page} --Temporary data structure
    -- BTreeNode: size prev next keys children
    -- BTreeLeaf: size prev next values
    
    data BTreeTableStat=BTreeTableStat {firstLeaf :: Int, lastLeaf :: Int, root :: Int}
    data BTreeTable=BTreeTable {file :: FS.DataFile, stat :: IORef BTreeTableStat, recordSize::Int, recordsPerLeaf::Int, keySize::Int, keysPerNode::Int}

    headerNode:: Word8
    headerNode=0x01
    headerLeaf :: Word8
    headerLeaf=0x00
    tagEmpty :: Int
    tagEmpty=0
    initialize :: String->Int->Int->IO ()
    initialize path rsz ksz=do
        file<-FS.openDataFile path
        let records_per_leaf=(FS.pageSize-13) `quot` rsz
        let keys_per_node=(FS.pageSize-13-4) `quot` (ksz+4)
        st<-newIORef $ BTreeTableStat 2 2 2
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
                st<-newIORef $ BTreeTableStat firstleaf lastleaf root
                return $ BTreeTable file st recordsize recordsperleaf keysize keyspernode 
        FS.inspectPage f metadata
    close :: BTreeTable->IO ()
    close table=do
        flushMetadata table
        FS.closeDataFile $ file table

    