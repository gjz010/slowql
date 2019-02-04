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
    import System.Exit
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
                   | LeafHeader {size :: Int, prev :: Int, next :: Int} deriving (Show)


    data BTreeTableStat=BTreeTableStat {firstLeaf :: Int, lastLeaf :: Int, root :: Int, lastPage :: Int, freePage :: Int} deriving (Show)
    data BTreeTable=BTreeTable {file :: FS.DataFile, stat :: IORef BTreeTableStat, recordSize::Int, recordsPerLeaf::Int, keySize::Int, keysPerNode::Int}
    printBT :: BTreeTable->IO ()
    printBT table=do
        putStrLn "BTreeTable"
        print (recordSize table, recordsPerLeaf table, keySize table, keysPerNode table)
        st<-readIORef $ stat table
        print st
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
            1 ->NodeHeader b c d
            0 ->LeafHeader b c d
            
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
        --print st
        if freePage st==placeholder 
            then do
                let new_stat=st {lastPage=((lastPage st)+1)}
                writeIORef (stat table) new_stat
                --print new_stat
                s<-readIORef $ stat table
                --print s
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
    data InsertResult=InsertSuccess Int | InsertRequireSplit IKey Int Int | InsertFailed
    

    --Find the position to "insert" the value.
    binsearch :: (Monad m)=>(Int->m Ordering)->Int->Int->m Int
    binsearch funct{- compare to-insert key with the existing key -} l r=
        let go l r=if l==r 
                        then return l
                        else do
                            let mid=(l+r) `quot` 2 -- l<=mid<mid+1<=r
                            ret<-funct mid
                            if(ret==GT || ret==EQ)
                                then go (mid+1) r
                                else go l mid
        in go l (r+1)
    insert :: BTreeTable->KeyHint->IValue->IO ()
    insert table (fext, fcmp) val=do
        st<-readIORef $ stat table
        let key=fext val
        let ccmp=fcmp key
        --putStrLn $ "Inserting "++(show val)
        let step pn=do
                --putStrLn $ "Going down to page "++show pn
                pg<-FS.getPage (file table) pn
                header<-FS.inspectPage readHeader pg
                if isNode header --Need recursive insertion, note that after recursion "pg" may has been invalid
                    then do--scale down for a value
                        let find_leaf ptr=do
                                let key_start=ptr `plusPtr` 13
                                all_keys<-FS.sliceBSOff 0 ((keySize table)*(keysPerNode table)) key_start
                                --when(size header>=5) $ print all_keys
                                --print $ size header
                                let pcmp n=let s=BS.take (keySize table)$ BS.drop (n*(keySize table)) all_keys in return $ ccmp s
                                branch<-binsearch pcmp 0 ((size header)-1)
                                let branch_start=key_start `plusPtr` ((keySize table)*(keysPerNode table))
                                branch_num<-readInt branch_start (branch*4)
                                return (branch, branch_num, BS.take (keySize table)$ BS.drop (branch*(keySize table)) all_keys)
                        (branch, branch_num, temp_k)<-FS.inspectPage find_leaf pg
                        -- Go down
                        --print (branch, branch_num, temp_k)
                        --putStrLn $ "Going down "++(show temp_k)++" before key "++(show branch_num)
                        --die "Stop!"
                        result<-step branch_num
                        -- And back
                        --putStrLn $ "Back to page "++show pn
                        case result of
                            InsertFailed -> return InsertFailed --Impossible.
                            InsertSuccess pn ->return $ InsertSuccess pn
                            InsertRequireSplit ret_key ret_l ret_r->do
                                pg<-FS.getPage (file table) pn --Reload the page
                                header<-FS.inspectPage readHeader pg
                                let modifier ptr=do
                                        writeHeader (header {size=size header+1}) ptr
                                        let key_start=ptr `plusPtr` 13
                                        let branch_start=ptr `plusPtr` 13 `plusPtr` ((keySize table)*(keysPerNode table))
                                        -- move keys
                                        let ok=branch
                                        let rk=(size header)-ok
                                        let ob=ok+1
                                        let rb=rk
                                        let k_start=key_start `plusPtr` ((keySize table)*ok)
                                        let b_start=branch_start `plusPtr` (4*ob)
                                        moveBytes (k_start `plusPtr` (keySize table)) k_start ((keySize table)*rk)
                                        moveBytes (b_start `plusPtr` 4) b_start (4*rb)
                                        writeInt b_start 0 ret_r
                                        FS.writeBSOff 0 ret_key k_start
                                FS.modifyPage modifier pg
                                if (size header==(keysPerNode table-1)) 
                                    then do -- Still need split!
                                        let left_size=(keysPerNode table) `quot` 2
                                        let right_size=keysPerNode table-left_size --need to take out one as key!
                                        new_pn<-allocatePage table
                                        pg<-FS.getPage (file table) pn --reload the page after allocating
                                        let gol ptr=do
                                                let key_start=ptr `plusPtr` 13
                                                let branch_start=ptr `plusPtr` 13 `plusPtr` ((keySize table)*(keysPerNode table))
                                                let lheader=header {size=left_size, next=new_pn}
                                                writeHeader lheader ptr
                                                right_keys<-FS.sliceBSOff (left_size*(keySize table)) (right_size*(keySize table)) key_start --need to take out one
                                                let (right_keys_a, right_keys_b)=BS.splitAt (keySize table) right_keys
                                                right_branches<-FS.sliceBSOff ((left_size+1)*4) (right_size*4) branch_start
                                                return $ (right_keys_a, right_keys_b, right_branches)
                                        (right_keys_a, right_keys_b, right_branches)<-FS.modifyPage gol pg
                                        let gor ptr=do
                                                let rheader=NodeHeader (right_size-1) pn (next header)
                                                let key_start=ptr `plusPtr` 13
                                                let branch_start=ptr `plusPtr` 13 `plusPtr` ((keySize table)*(keysPerNode table))
                                                writeHeader rheader ptr
                                                FS.writeBSOff 0 right_keys_b key_start
                                                FS.writeBSOff 0 right_branches branch_start
                                        newpg<-FS.getPage (file table) new_pn
                                        FS.modifyPage gor newpg
                                        --when (pn==lastLeaf st) $ writeIORef (stat table) (st {lastLeaf=new_pn})
                                        return $ InsertRequireSplit right_keys_a pn new_pn   
                                    else return $ InsertSuccess pn --No need to split
                                


                    else do -- First insert into the page
                        let op ptr=do
                                let f n=do
                                            let offset=13+n*(recordSize table)
                                            nv<-FS.sliceBSOff offset (recordSize table) ptr
                                            return $ ccmp (fext nv)
                                
                                after<-binsearch f 0 (size header-1)
                                --putStrLn $ "Inserting after " ++ show after
                                -- Move other records one unit forward
                                let src=ptr `plusPtr` (13+(after)*(recordSize table))
                                let dst=src `plusPtr` (recordSize table)
                                moveBytes dst src ((size header-after)*(recordSize table))
                                FS.writeBSOff 0 val src --Insert the record
                                writeHeader (header {size=(size header+1)}) ptr
                        FS.modifyPage op pg
                        if (size header==((recordsPerLeaf table)-1)) --If the page is already full **after** insertion, do the split.
                            then do
                                    let left_size=(recordsPerLeaf table) `quot` 2
                                    let right_size=recordsPerLeaf table-left_size
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
                                    if(pn==lastLeaf st) 
                                        then modifyIORef' (stat table) (\st->st {lastLeaf=new_pn})
                                        else do
                                            npg<-FS.getPage (file table )(next header) 
                                            let g ptr=do
                                                    h<-readHeader ptr
                                                    writeHeader (h{prev=new_pn}) ptr
                                            FS.modifyPage g npg
                                    --putStrLn $ "Creating leaf link "++show (pn, new_pn, next header)
                                    let ekey=(fext $ BS.take (recordSize table) right_part)
                                    --putStrLn $ "Leaf splitted into "++show (ekey, pn, new_pn)
                                    return $ InsertRequireSplit ekey pn new_pn
                            else return $ InsertSuccess pn
        result<-step $ root st
        case result of
            InsertSuccess pn->return ()
            InsertRequireSplit ret_key ret_l ret_r->do
                    new_pn<-allocatePage table
                    let write ptr=do
                            let header=NodeHeader 1 tagEmpty tagEmpty
                            writeHeader header ptr
                            let key_start=ptr `plusPtr` 13
                            let branch_start=ptr `plusPtr` 13 `plusPtr` ((keySize table)*(keysPerNode table))
                            FS.writeBSOff 0 ret_key key_start
                            writeInt branch_start 0 ret_l
                            writeInt branch_start 4 ret_r
                    newpg<-FS.getPage (file table) new_pn
                    FS.modifyPage write newpg
                    modifyIORef' (stat table) (\st->st{root=new_pn})
                    --putStrLn $ "Root splitted into "++show (ret_key, new_pn, ret_l, ret_r)
                    return ()

    
    data DeleteResult=DeleteFinish | DeleteMerged Int | DeleteJuggled IKey Int | DeleteFailed deriving (Eq) --DeleteFail becomes possible, representing a deletion miss
    --This tree does not welcome duplicated keys.
    -- Do you like pointer game?
    delete :: BTreeTable->KeyHint->IValue->IO Bool
    delete table (fext, fcmp) val=do
        st<-readIORef $ stat table
        let key=fext val
        let ccmp=fcmp key
        --putStrLn $ "Inserting "++(show val)
        let firstKey spn=do
                pg<-FS.getPage (file table) spn
                let inspector ptr=do
                        h<-readHeader ptr
                        case h of
                            LeafHeader{} -> do
                                    first_val<-FS.sliceBSOff 13 (recordSize table) ptr
                                    return $ fext first_val
                            NodeHeader{} -> do
                                    first_key<-FS.sliceBSOff 13 (keySize table) ptr
                                    return first_key
                FS.inspectPage inspector pg

        let juggleLeaf lson rson=do
                pg<-FS.getPage (file table) rson
                rheader<-FS.inspectPage readHeader pg
                pg<-FS.getPage (file table) lson
                lheader<-FS.inspectPage readHeader pg
                let total_size=(size lheader)+(size rheader)
                if(total_size <= (recordsPerLeaf table))
                    then do --can perform a merge, thus merge
                            pg<-FS.getPage (file table) rson
                            righthand<-FS.inspectPage (FS.sliceBSOff 13 ((size rheader)*(recordSize table))) pg
                            delPage table rson
                            let append ptr=do
                                    writeHeader (lheader {size=total_size, next=next rheader}) ptr
                                    FS.writeBSOff (13+((size lheader)*(recordSize table))) righthand ptr
                            pg<-FS.getPage (file table) lson
                            FS.modifyPage append pg
                            if (next rheader==0)
                                then modifyIORef' (stat table) (\st->st {lastLeaf=lson})
                                else do
                                        pg<-FS.getPage (file table) (next rheader)
                                        let fixprev ptr=do
                                                h<-readHeader ptr
                                                writeHeader (h{prev=lson}) ptr
                                        FS.modifyPage fixprev pg
                            return $ DeleteMerged lson
                    else do --Evenly split
                            -- We have to consider that this can always be done, since at least one of lh and rh satisfies < recordsPerLeaf `quot` 2
                            -- If after even-split one leaf still cannot satisfy the requirement,
                            --  this means total_size `quot` 2 < recordsPerLeaf `quot` 2, which would have been merged.
                            -- Not juggling will make it unable to satisfy the requirement, so a juggle is necessary.
                            let left_size=total_size `quot` 2
                            let right_size=total_size-left_size
                            if((size lheader)>left_size)
                                then do --Moving some left elements to right
                                    pg<-FS.getPage (file table) lson
                                    let delta=(size lheader)-left_size
                                    let extract ptr=do
                                            writeHeader (lheader {size=left_size}) ptr --Just remark the size
                                            FS.sliceBSOff (13+(left_size*recordSize table)) (delta*(recordSize table)) ptr
                                    l2r<-FS.modifyPage extract pg
                                    let attach ptr=do
                                            let start=ptr `plusPtr` 13
                                            moveBytes (start `plusPtr` (delta*recordSize table)) start ((size rheader)*(recordSize table))
                                            FS.writeBSOff 0 l2r start
                                            writeHeader (rheader {size=right_size}) ptr
                                    pg<-FS.getPage (file table) rson
                                    FS.modifyPage attach pg
                                else do --Moving some right elements to left
                                    pg<-FS.getPage (file table) rson
                                    let delta=left_size-(size lheader)
                                    let extract ptr=do
                                            writeHeader (rheader {size=right_size}) ptr
                                            str<-FS.sliceBSOff 13 (delta*(recordSize table)) ptr
                                            moveBytes (ptr `plusPtr` 13) (ptr `plusPtr` 13 `plusPtr` (delta*(recordSize table))) ((right_size)*(recordSize table))
                                            return str
                                    r2l<-FS.modifyPage extract pg
                                    let attach ptr=do
                                            writeHeader (lheader {size=left_size}) ptr
                                            FS.writeBSOff (13+((size lheader)*(recordSize table))) r2l ptr
                                    pg<-FS.getPage (file table ) lson
                                    FS.modifyPage attach pg
                            v<-firstKey rson
                            return $ DeleteJuggled v lson
                                    

        let juggleNode lson rson=do
                pg<-FS.getPage (file table) rson
                rheader<-FS.inspectPage readHeader pg
                new_key_page<-FS.inspectPage (\p->readInt p (13+(keySize table)*(keysPerNode table))) pg
                new_key<-firstKey new_key_page
                pg<-FS.getPage (file table) lson
                lheader<-FS.inspectPage readHeader pg
                let total_size=(size lheader)+(size rheader)
                if(total_size+1 <= (recordsPerLeaf table))
                    then do --can perform a merge, thus merge
                            pg<-FS.getPage (file table) rson
                            right_keys<-FS.inspectPage (FS.sliceBSOff 13 ((size rheader)*(keySize table))) pg
                            right_branches<-FS.inspectPage (FS.sliceBSOff (13+(keySize table)*(keysPerNode table)) ((1+size rheader)*4)) pg
                            delPage table rson
                            let append ptr=do
                                    writeHeader (lheader {size=total_size+1, next=next rheader}) ptr
                                    FS.writeBSOff (13+((size lheader)*(keySize table))) new_key ptr
                                    FS.writeBSOff (13+((1+size lheader)*(keySize table))) right_keys ptr
                                    FS.writeBSOff (13+(keySize table)*(keysPerNode table)+4*(1+size lheader)) right_branches ptr
                            pg<-FS.getPage (file table) lson
                            FS.modifyPage append pg
                            when (next rheader/=0) $ do
                                        pg<-FS.getPage (file table) (next rheader)
                                        let fixprev ptr=do
                                                h<-readHeader ptr
                                                writeHeader (h{prev=lson}) ptr
                                        FS.modifyPage fixprev pg
                            return $ DeleteMerged lson
                    else do --Evenly split
                            let left_size=(total_size+1) `quot` 2
                            let right_size=total_size-left_size
                            if((size lheader)>left_size)
                                then do --Moving some left elements to right, new key at right
                                    pg<-FS.getPage (file table) rson
                                    let delta=(size lheader)-left_size
                                    let extract ptr=do
                                            writeHeader (lheader {size=left_size}) ptr --Just remark the size
                                            delta_all_keys<-FS.sliceBSOff (13+(left_size*keySize table)) (delta*(keySize table)) ptr
                                            delta_branches<-FS.sliceBSOff (13+(keysPerNode table*keySize table)+((left_size+1)*4)) (delta*4) ptr
                                            let (extra_key, delta_keys)=BS.splitAt 4 delta_all_keys
                                            return $ (extra_key, delta_keys, delta_branches)
                                    (extra_key, delta_keys, delta_branches)<-FS.modifyPage extract pg
                                    let attach ptr=do
                                            let keys_start=ptr `plusPtr` 13
                                            let branches_start=keys_start `plusPtr` (keysPerNode table*keySize table)
                                            moveBytes (keys_start `plusPtr` (delta*keySize table)) keys_start ((size rheader)*(keySize table))
                                            moveBytes (branches_start `plusPtr` (delta*4)) branches_start ((size rheader)*4)
                                            FS.writeBSOff 0 delta_keys keys_start
                                            FS.writeBSOff (BS.length delta_keys) new_key keys_start
                                            FS.writeBSOff 0 delta_branches branches_start
                                            writeHeader (rheader {size=right_size}) ptr
                                    pg<-FS.getPage (file table) rson
                                    FS.modifyPage attach pg
                                    return $ DeleteJuggled extra_key lson
                                else do --Moving some right elements to left, new key at left
                                    pg<-FS.getPage (file table) rson
                                    let delta=left_size-(size lheader)
                                    let extract ptr=do
                                            writeHeader (rheader {size=right_size}) ptr --Just remark the size
                                            let keys_start=ptr `plusPtr` 13
                                            let branches_start=keys_start `plusPtr` (keysPerNode table*keySize table)
                                            delta_all_keys<-FS.sliceBSOff 0 (delta*keySize table) keys_start
                                            delta_branches<-FS.sliceBSOff 0 (delta*4) branches_start
                                            let (delta_keys, extra_key)=BS.splitAt ((delta-1)*(keySize table)) delta_all_keys
                                            moveBytes keys_start (keys_start `plusPtr` (delta*keySize table)) (right_size*keySize table)
                                            moveBytes branches_start (branches_start `plusPtr` (delta*4)) ((right_size+1)*4)
                                            return $ (extra_key, delta_keys, delta_branches)
                                    (extra_key, delta_keys, delta_branches)<-FS.modifyPage extract pg
                                    let attach ptr=do
                                            writeHeader (lheader {size=left_size}) ptr
                                            let keys_start=ptr `plusPtr` 13
                                            let branches_start=keys_start `plusPtr` (keysPerNode table*keySize table)
                                            FS.writeBSOff ((size lheader)*(keySize table)) new_key keys_start
                                            FS.writeBSOff ((size lheader+1)*(keySize table)) delta_keys keys_start
                                            FS.writeBSOff ((size lheader+2)*4) delta_branches branches_start
                                        
                                    pg<-FS.getPage (file table) rson
                                    FS.modifyPage attach pg
                                    return $ DeleteJuggled extra_key lson

        let step pn has_ln has_rn=do
                --putStrLn $ "Going down to page "++show pn
                pg<-FS.getPage (file table) pn
                header<-FS.inspectPage readHeader pg
                if isNode header --Need recursive deletion, note that after recursion "pg" may has been invalid
                    then do--scale down for a value
                        let find_leaf ptr=do
                                let key_start=ptr `plusPtr` 13
                                all_keys<-FS.sliceBSOff 0 ((keySize table)*(keysPerNode table)) key_start
                                --when(size header>=5) $ print all_keys
                                --print $ size header
                                let pcmp n=let s=BS.take (keySize table)$ BS.drop (n*(keySize table)) all_keys in return $ ccmp s
                                branch<-binsearch pcmp 0 ((size header)-1)
                                let branch_start=key_start `plusPtr` ((keySize table)*(keysPerNode table))
                                branch_num<-readInt branch_start (branch*4)
                                return (branch, branch_num, BS.take (keySize table)$ BS.drop (branch*(keySize table)) all_keys)
                        (branch, branch_num, temp_k)<-FS.inspectPage find_leaf pg
                        -- Go down
                        --print (branch, branch_num, temp_k)
                        --putStrLn $ "Going down "++(show temp_k)++" before key "++(show branch_num)
                        --die "Stop!"
                        result<-step branch_num (branch /= 0) (branch /= (size header-1))
                        -- And back
                        --putStrLn $ "Back to page "++show pn
                        case result of
                            DeleteFailed -> return DeleteFailed --Impossible.
                            DeleteFinish ->return DeleteFinish
                            DeleteJuggled mid_key left_son->do --Son juggled, need update key content
                                pg<-FS.getPage (file table) pn --Reload the page
                                let left_son_offset=if left_son==branch_num then branch else (branch-1)
                                FS.modifyPage (FS.writeBSOff (13+(keySize table)*left_son_offset) mid_key) pg
                                return DeleteFinish
                            DeleteMerged left_son->do --Some merge thing happens.
                                pg<-FS.getPage (file table) pn --Reload the page
                                header<-FS.inspectPage readHeader pg
                                let merge_target_offset=if left_son==branch_num then branch else (branch-1)
                                let compressor ptr=do
                                        let sz=size header
                                        let rsz=sz-merge_target_offset-1
                                        let keys_start=ptr `plusPtr` 13
                                        let branches_start=keys_start `plusPtr` ((keysPerNode table)*(keySize table))
                                        let right_dst=keys_start `plusPtr` ((merge_target_offset)*keySize table)
                                        moveBytes right_dst (right_dst `plusPtr` (keySize table)) (rsz*(keySize table))
                                        let right_branch_dst=branches_start `plusPtr` ((merge_target_offset+1)*4)
                                        moveBytes right_branch_dst (right_branch_dst `plusPtr` 4) (rsz*4)
                                        writeHeader (header{size=sz-1}) ptr
                                FS.modifyPage compressor pg
                                if((size header-1)<((keysPerNode table) `quot` 2))
                                    then do
                                        let pl=prev header
                                        let pr=next header
                                        if has_ln
                                            then juggleNode pl pn
                                            else if has_rn
                                                    then juggleNode pn pr
                                                    else case header of --Root node: remove when necessary
                                                            LeafHeader{} -> return DeleteFinish
                                                            NodeHeader{} -> do
                                                                    when (size header-1==0) $ do
                                                                            pg<-FS.getPage (file table) pn
                                                                            new_root<-FS.inspectPage (\ptr->readInt ptr (13+(keySize table)*(keysPerNode table))) pg
                                                                            modifyIORef' (stat table) (\st->st{root=new_root})
                                                                            delPage table pn
                                                                    return DeleteFinish
                                    else return DeleteFinish


                    else do -- First remove from into the page
                        let op ptr=do
                                let f n=do
                                            let offset=13+n*(recordSize table)
                                            nv<-FS.sliceBSOff offset (recordSize table) ptr
                                            return $ ccmp (fext nv)
                                after<-binsearch f 0 (size header-1)
                                
                                if(after==0)
                                    then return False
                                    else do
                                            let after_offset=13+(after-1)*(recordSize table)
                                            prev_str<-(FS.sliceBSOff after_offset (recordSize table) ptr)
                                            if prev_str/= val
                                                then return False
                                                else do
                                                    let src=ptr `plusPtr` (13+(after)*(recordSize table))
                                                    let dst=src `plusPtr` (-(recordSize table))
                                                    moveBytes dst src ((size header-after)*(recordSize table))
                                                    writeHeader (header {size=(size header-1)}) ptr
                                                    return True
                        success<-FS.modifyPage op pg
                        if success
                            then do
                                    if ((size header-1)<((recordsPerLeaf table) `quot` 2)) --If the page is half full **after** deletion, do the juggle.
                                        then do
                                                let pl=prev header
                                                let pr=next header
                                                if has_ln
                                                    then juggleLeaf pl pn
                                                    else if has_rn
                                                            then juggleLeaf pn pr
                                                            else return $ DeleteFinish --The leaf is the root leaf.
                                        else return $ DeleteFinish
                            else return DeleteFailed
        result<-step ( root st) False False
        return $ if result==DeleteFinish then True else False
                            
    
    --This finds a good place to insert an item with such key.
    --You need to look forward or backward to find the exact boundary.
    findLeafByKey :: BTreeTable->KeyHint->IKey->IO Int
    findLeafByKey table (fext, fcmp) key=do
        let ccmp=fcmp key
        let go pn=do
                pg<-FS.getPage (file table) pn
                header<-FS.inspectPage readHeader pg
                case header of 
                    NodeHeader size prev next->do
                                let find_leaf ptr=do
                                        let key_start=ptr `plusPtr` 13
                                        all_keys<-FS.sliceBSOff 0 ((keySize table)*(keysPerNode table)) key_start
                                        let pcmp n=let s=BS.take (keySize table)$ BS.drop (n*(keySize table)) all_keys in return $ ccmp s
                                        branch<-binsearch pcmp 0 ((size)-1)
                                        let branch_start=key_start `plusPtr` ((keySize table)*(keysPerNode table))
                                        branch_num<-readInt branch_start (branch*4)
                                        return (branch, branch_num)
                                (branch, branch_num)<-FS.inspectPage find_leaf pg
                                go branch_num
                    LeafHeader size prev next->return pn
        st<-readIORef $ stat table
        go (root st)
    findFirstPage :: BTreeTable->KeyHint->Maybe (IKey, Bool)->IO Int
    findFirstPage table (fext, fcmp) Nothing=do
        st<-readIORef $ stat table
        return $ firstLeaf st
    findFirstPage table (fext, fcmp) (Just (key, included))=do
        hint<-findLeafByKey table (fext, fcmp) key
        let ccmp=fcmp key
        let go pn=do
                    pg<-FS.getPage (file table) pn
                    header<-FS.inspectPage readHeader pg
                    first_item<-FS.inspectPage (FS.sliceBSOff 13 (recordSize table)) pg
                    let cr=ccmp $ fext first_item
                    let satisfied=(cr==LT) || (cr==EQ && included) --if satisfied then we need to go deeper!
                    if satisfied && ((prev header)/=tagEmpty)
                        then go (prev header)
                        else return pn
        go hint
    findLastPage :: BTreeTable->KeyHint->Maybe (IKey, Bool)->IO Int
    findLastPage table (fext, fcmp) Nothing=do
        st<-readIORef $ stat table
        return $ lastLeaf st
    findLastPage table (fext, fcmp) (Just (key, included))=do
        hint<-findLeafByKey table (fext, fcmp) key
        let ccmp=fcmp key
        let go pn=do
                    pg<-FS.getPage (file table) pn
                    header<-FS.inspectPage readHeader pg
                    last_item<-FS.inspectPage (FS.sliceBSOff (13+(size header-1)*(recordSize table)) (recordSize table)) pg
                    let cr=ccmp $ fext last_item
                    let satisfied=(cr==GT) || (cr==EQ && included) --if satisfied then we need to go deeper!
                    if satisfied && ((prev header)/=tagEmpty)
                        then go (prev header)
                        else return pn
        go hint

    startEnumerate :: BTreeTable->Int->ConduitT () BS.ByteString IO ()
    startEnumerate table pn=if pn==tagEmpty
            then return ()
            else do
                    let go =do
                            pg<-FS.getPage (file table) pn
                            header<-FS.inspectPage readHeader pg
                            --print header
                            items<-FS.inspectPage (FS.sliceBSOff 13 ((recordSize table)*(size header))) pg
                            return $ (map (\n->BS.take (recordSize table) $ BS.drop (n*(recordSize table)) items) [0..((size header)-1)], next header)
                    (items, nxt)<-liftIO go
                    mapM_ yield items
                    startEnumerate table nxt

    rstartEnumerate :: BTreeTable->Int->ConduitT () BS.ByteString IO ()
    rstartEnumerate table pn=if pn==tagEmpty
            then return ()
            else do
                    let go =do
                            pg<-FS.getPage (file table) pn
                            header<-FS.inspectPage readHeader pg
                            items<-FS.inspectPage (FS.sliceBSOff 13 ((recordSize table)*(size header))) pg
                            return $ (map (\n->BS.take (recordSize table) $ BS.drop (n*(recordSize table)) items) (reverse [0..((size header)-1)]), prev header)
                    (items, prv)<-liftIO go
                    mapM_ yield items
                    rstartEnumerate table prv
    satisfyLowerBound ::KeyHint->Maybe (IKey, Bool)->IValue->Bool
    satisfyLowerBound _ Nothing _=True
    satisfyLowerBound (fext, fcmp) (Just (key, included)) val=
            let ccmp=fcmp key
                cr=ccmp $ fext val
            in (cr==LT) || (cr==EQ && included) --key<fext val
    satisfyUpperBound ::KeyHint->Maybe (IKey, Bool)->IValue->Bool
    satisfyUpperBound _ Nothing _=True
    satisfyUpperBound (fext, fcmp) (Just (key, included)) val=
            let ccmp=fcmp key
                cr=ccmp $ fext val
            in (cr==GT) || (cr==EQ && included) -- key>fext val
    enumerateRange :: BTreeTable->KeyHint->Maybe (IKey, Bool)->Maybe (IKey, Bool)->ConduitT () BS.ByteString IO ()
    enumerateRange table hint lbound ubound=do
        since<-liftIO $ findFirstPage table hint lbound
        let wide_enum=startEnumerate table since
        wide_enum .| (dropWhileC (not . satisfyLowerBound hint lbound) >> mapC id) .| takeWhileC (satisfyUpperBound hint ubound)

    renumerateRange :: BTreeTable->KeyHint->Maybe( IKey, Bool)->Maybe( IKey, Bool)->ConduitT () BS.ByteString IO ()
    renumerateRange table hint lbound ubound=do
        till<-liftIO $ findLastPage table hint lbound
        let wide_enum=rstartEnumerate table till
        wide_enum .| (dropWhileC (not . satisfyUpperBound hint ubound) >> mapC id) .| takeWhileC (satisfyLowerBound hint lbound)
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

    