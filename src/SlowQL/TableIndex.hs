module SlowQL.TableIndex where
    import qualified Data.ByteString.Lazy as B
    import qualified Data.ByteString.Lazy.UTF8 as UB
    import qualified Data.ByteArray as BA
    import qualified SlowQL.DataType as Type
    import qualified SlowQL.PageFS as FS
    import qualified SlowQL.Table as Table
    import qualified Data.Set as Set
    import qualified SlowQL.ListReader as LR
    import Data.BitArray.IO
    import Data.BitArray
    import Data.Word
    import Data.List
    import Data.List.Split
    import Data.Char
    import Data.Maybe
    import System.IO.Unsafe
    -- No recursive definition, as if there were some mutable IO monad.
    data BTree = BTreeNode {index:: Int,  key::[[Word8]], children :: [Int], parent :: Maybe Int, prevNode :: Maybe Int, nextNode :: Maybe Int} | 
                 BTreeLeaf {index::Int,  value :: [[Word8]], prevLeaf:: Maybe Int, nextLeaf :: Maybe Int, parent :: Maybe Int} deriving(Show)
    -- Table index is record-innocent.
    -- This means table index knows nothing about the table structure at query, until insertion/deletion.
    
    
    

    data IndexCapacity = IndexCapacity {recordSize::Int, recordPerLeaf::Int, keySize::Int, keyPerNode::Int} deriving (Show)

    newtype BitSet=BitSet {bitsetContent::IOBitArray}

    instance Show BitSet where
        show a = "<BitSet>"

    data TableIndex = TableIndex {name :: String, table :: Table.Table, file :: FS.DataFile, tree :: Int, keys :: [(String, Bool)], capacity :: IndexCapacity,
                                usedChunks::BitSet, firstLeaf :: Int, lastLeaf :: Int} deriving (Show)
    
    calculateCapacity :: Table.Table->[String]->IndexCapacity
    calculateCapacity table paramName=let l=Table.recordSize table
                                          dom=Table.domains table
                                          f= flip Set.member (Set.fromList paramName)
                                          qualified_dom=filter (\d->f (Type.name $ Type.general d)) dom
                                          ksize=sum $ map Type.paramSize qualified_dom
                                          in
                                          -- 1 for separation nodes and leaves
                                          -- (4[used_slots]+6[header]+l[domains])
                                          -- (4[keys_current_page]+4+ksize[key]+4+ksize[key]+...+4)
                                          IndexCapacity (l+6) ((FS.pageSize-12-1) `div` (l+6)) ksize ((FS.pageSize-8-1-8) `div` (ksize+4))

    
    createIndex :: Table.Table->String->[(String, Bool)]->IO TableIndex
    createIndex table name keys=do
        let (keys_name, keys_asc)=unzip keys
        let capacity=calculateCapacity table keys_name
        let filename=name++".idx.dat"
        file<-FS.openDataFile filename
        FS.readPage file 0
        FS.readPage file 1
        FS.readPage file 2
        FS.readPage file 3
        let empty_tree=BTreeLeaf 2 [] Nothing Nothing Nothing
        
        chunk<-newBitArray (0, FS.pageSize) False
        writeBit chunk 0 True
        writeBit chunk 1 True
        writeBit chunk 2 True
        let tableindex=TableIndex name table file 2 keys capacity (BitSet chunk) 2 2
        saveIndexMetadata tableindex
        writeBTree tableindex empty_tree
        return tableindex
    allocatePage :: BitSet->IO Int
    allocatePage chunk=do
        schunk<-freezeBitArray (bitsetContent chunk)
        let (Just freepage)=elemIndex False $ bits schunk
        writeBit (bitsetContent chunk) freepage True
        return freepage
    
    freePage :: BitSet->Int->IO ()
    freePage chunk page=writeBit (bitsetContent chunk) page False
    compactChunk :: BitSet->IO [Word8]
    compactChunk chunk=do
        schunk<-freezeBitArray $ bitsetContent chunk
        return $ map LR.buildWord8 (chunksOf 8 $ bits schunk)
    readChunk :: [Word8]->IO BitSet
    readChunk list=
        let bs=concatMap LR.expandWord8 list
            br=listBitArray (0, FS.pageSize) bs
        in do
            c<-thawBitArray br
            return $ BitSet c
    
    readKey :: [Word8]->((String, Bool), [Word8])
    readKey iter=
        let (lstr, str)=LR.readInt32 iter
            (name,b)=splitAt (fromIntegral lstr) str
        in ((UB.toString (B.pack name), head b ==1), tail b)
    openIndex :: Table.Table->String->IO (Maybe TableIndex)
    openIndex table name=do
        let filename=name++".idx.dat"
        fexist<-FS.exists filename
        if(fexist) then do
            file<-FS.openDataFile filename
            chunk<-FS.readPage file 0
            let iter=FS.iterate chunk 0
            let (magic, header)=splitAt 8 iter
            if map (chr.fromIntegral) magic == indexFileMagicNumber then do
                bm<-FS.readPage file 1
                let bmi=FS.iterate bm 0
                ioba<-readChunk bmi
                let ([treeRoot, firstLeaf, lastLeaf, recordSize, recordPerLeaf, keySize, keyPerNode, lenKeys], keys)=(\(x, y)->(map fromIntegral x, y)) $ LR.rzip (replicate 8 LR.readInt32) [] header
                let (rkeys, _)=LR.rzip (replicate lenKeys readKey) [] keys
                return $ Just $ TableIndex name table file treeRoot rkeys (IndexCapacity recordSize recordPerLeaf keySize keyPerNode) ioba firstLeaf lastLeaf
            else return Nothing
        else return Nothing

    indexFileMagicNumber :: String
    indexFileMagicNumber="SLOWQLB+"

    saveIndexMetadata :: TableIndex->IO()
    saveIndexMetadata (TableIndex name table file tree keys capacity usedChunks firstLeaf lastLeaf)=do
        let magic=UB.fromString indexFileMagicNumber
        let wTr=LR.writeInt32 $ fromIntegral tree
        let wFl=LR.writeInt32 $ fromIntegral firstLeaf
        let wLl=LR.writeInt32 $ fromIntegral lastLeaf
        let wCap=concatMap (LR.writeInt32.fromIntegral) [(recordSize capacity), (recordPerLeaf capacity), keySize capacity, keyPerNode capacity]
        let wK=(LR.writeInt32$fromIntegral (length keys))++(concatMap (\(dname, asc)->((LR.writeInt32$fromIntegral (UB.length $ UB.fromString dname))++(B.unpack $ UB.fromString dname))++(if asc then [1] else [0])) keys)
        let Just chunk=FS.compact $ B.concat [magic, B.pack wTr, B.pack wFl, B.pack wLl, B.pack wCap, B.pack wK]
        wUc<-compactChunk usedChunks
        let Just chunk2=FS.compact $ B.pack wUc
        FS.writePage file 0 chunk
        FS.writePage file 1 chunk2


    writeBTree :: TableIndex->BTree->IO()
    writeBTree TableIndex {file=file} (BTreeNode index key children parent prevNode nextNode)=do
        let magic=[0] --1
        let wSize=LR.writeInt32 $ fromIntegral $ length key --4
        let wPrevNode=LR.writeInt32 $ fromIntegral $ extractMaybe 0 prevNode
        let wNextNode=LR.writeInt32 $ fromIntegral $ extractMaybe 0 nextNode
        let first_child=LR.writeInt32 $ fromIntegral $ head children --4
        let other_children=concat $ zipWith (\k c->[k, LR.writeInt32 $ fromIntegral c]) key (tail children) --(4+l)n
        let Just chunk=FS.compact $ B.concat $ map B.pack ([magic, wSize, wPrevNode, wNextNode, first_child]++other_children)
        FS.writePage file index chunk

    
    writeBTree TableIndex {file=file} (BTreeLeaf index value prevLeaf nextLeaf parent)=do
        let magic=[1] --1
        let wSize=LR.writeInt32 $ fromIntegral $ length value --4
        let wPrev=LR.writeInt32 $ fromIntegral $ extractMaybe 0 prevLeaf --4
        let wNext=LR.writeInt32 $ fromIntegral $ extractMaybe 0 nextLeaf --4
        let valdat=concat value
        let Just chunk=FS.compact $ B.concat $ map B.pack [magic, wSize, wPrev, wNext, valdat]
        FS.writePage file index chunk

    
    extractMaybe :: a->Maybe a->a
    extractMaybe _ (Just b)=b
    extractMaybe a Nothing=a
    wrapMaybe :: (Eq a)=>a->a->Maybe a
    wrapMaybe d a = if a==d then Nothing else Just a
    readBTree :: TableIndex->Int->Maybe Int->IO BTree
    readBTree TableIndex {file=file, capacity=capacity} page parent=do
        chunk<-FS.readPage file page
        let iter'=FS.iterate chunk 0
        let iter=tail iter'
        if(head iter'==0) then do --BTreeNode
            let ([size,  lPrevNode, lNextNode,first_child], oc_iter)=LR.rzip (replicate 4 LR.readInt32) [] iter
            let (other_children, _)=LR.rzip (replicate (fromIntegral size) (\it->
                    let (v_k, it_cd)=splitAt (keySize capacity) it
                        (v_c, rest)=LR.readInt32(it_cd)
                    in ((v_k, v_c), rest))) [] oc_iter
            let (v_keys, v_children)=unzip other_children
            return $ BTreeNode page v_keys (map fromIntegral (first_child:v_children)) parent (wrapMaybe 0 (fromIntegral lPrevNode)) (wrapMaybe 0 (fromIntegral lNextNode))
        else do --BTreeLeaf
            let ([size, prev, next], data_iter)=LR.rzip (replicate 3 LR.readInt32) [] iter
            let val_data=take (fromIntegral size) $ chunksOf (recordSize capacity) data_iter
            return $ BTreeLeaf page val_data (wrapMaybe 0 (fromIntegral prev)) (wrapMaybe 0 (fromIntegral next)) parent
    closeIndex :: TableIndex->IO()
    closeIndex index=do
        saveIndexMetadata index
        FS.closeDataFile $ file index
    lazyConcat2 :: IO [a]->IO [a]->IO [a]
    lazyConcat2 a b=unsafeInterleaveIO $ do
        a'<-a
        if(null a') then b
        else do
            let (x:xs)=a'
            lazy_val<-lazyConcat2 (return xs) b
            return (x:lazy_val)
    lazyConcat :: [IO [a]]->IO [a]
    lazyConcat = foldr lazyConcat2 (return [])


    findLeaf :: TableIndex->IKey->IO BTree
    findLeaf tindex k=g (tree tindex) where
        fext=extractor tindex
        fcmp=comparator tindex
        g node=do
            n<-readBTree tindex node Nothing
            case n of
                BTreeLeaf a b c d e-> return $ BTreeLeaf a b c d e
                BTreeNode index key children _ _ _->
                    let next_node=head $ drop (length $ takeWhile (\k'->fcmp k' k==LT) key) children
                    in g next_node
    iterateBlock :: TableIndex->BTree->IO [BTree]
    iterateBlock tindex tree=unsafeInterleaveIO $ do
        if(isNothing $ nextLeaf tree) then return [tree]
        else do
            let Just ni=nextLeaf tree
            nextt<-unsafeInterleaveIO $ readBTree tindex ni Nothing
            nit<-iterateBlock tindex nextt
            return $ tree:nit
    riterateBlock :: TableIndex->BTree->IO [BTree]
    riterateBlock tindex tree=unsafeInterleaveIO $ do
        if(isNothing $ prevLeaf tree) then return [tree]
        else do
            let Just pi=prevLeaf tree
            prevt<-unsafeInterleaveIO $ readBTree tindex pi Nothing
            pit<-iterateBlock tindex prevt
            return $ tree:pit 
    iterate' :: TableIndex->Int->IO [[Word8]]
    iterate' index pn=do
        btree<-readBTree index pn Nothing
        blocks<-iterateBlock index btree
        return $ concatMap value blocks
    riterate' :: TableIndex->Int->IO [[Word8]]
    riterate' index pn=do
        btree<-readBTree index pn Nothing
        blocks<-riterateBlock index btree
        return $ concatMap (reverse . value) blocks

    
    
    -- iterate :: TableIndex->[Type.TValue]->IO TableIndexCursor
    

    --riterate:: TableIndex->[Type.TValue]->IO TableIndexCursor

    insertOrdered :: (a->a)->(a->a->Ordering)->a->[a]->[a]
    insertOrdered ext cmp x []=[x]
    insertOrdered ext cmp x (y:ys)
        | cmp (ext x) (ext y)==LT = x:y:ys
        | otherwise = y:(insertOrdered ext cmp x ys)

    halfSplit :: [a]->([a],[a])
    halfSplit l=splitAt ((length l) `div` 2) l
    data InsertResult=InsertSuccess Int | InsertRequireSplit IKey Int Int
    isInsertSuccess :: InsertResult->Bool
    isInsertSuccess (InsertSuccess _)=True
    isInsertSuccess (InsertRequireSplit _ _ _)=False
    type IKey=[Word8]
    type IValue=[Word8]
    replaceValue :: (Eq a)=>a->a->[a]->[a]
    replaceValue orig new []=[]
    replaceValue orig new (x:xs)
        | x==orig = new:xs
        | otherwise = x:(replaceValue orig new xs)
    
    leftLeaf :: TableIndex->BTree->IO Int
    leftLeaf tindex (BTreeLeaf a b c d e)=return a
    leftLeaf tindex (BTreeNode index key children _ _ _)=do
        subtree<-readBTree tindex (head children) (Just index)
        leftLeaf tindex subtree
    rightLeaf :: TableIndex->BTree->IO Int
    rightLeaf tindex (BTreeLeaf a b c d e)=return a
    rightLeaf tindex (BTreeNode index key children _ _ _)=do
        subtree<-readBTree tindex (last children) (Just index)
        rightLeaf tindex subtree
    
    insertInto :: TableIndex->(IKey->IValue)->(IKey->IKey->Ordering)->IValue->IO TableIndex
    insertInto tindex fext fcomp item=do
        let root=tree tindex
        broot<-readBTree tindex root Nothing
        result<-insertInto' tindex fext fcomp item broot
        newroot<-if isInsertSuccess result then return root
        else do
            --putStrLn "Split!"
            let (InsertRequireSplit pushup_key left right)=result
            new_chunk<-allocatePage $ usedChunks tindex
            let bnewroot=BTreeNode new_chunk [pushup_key] [left,right] Nothing Nothing Nothing
            --putStrLn $ show bnewroot
            writeBTree tindex bnewroot
            return new_chunk
        --putStrLn "What?"
        broot<-readBTree tindex newroot Nothing
        --putStrLn (show broot)
        firstLeaf<-leftLeaf tindex broot
        lastLeaf<-rightLeaf tindex broot
        let new_index=TableIndex (name tindex) (table tindex) (file tindex) newroot (keys tindex) (capacity tindex) (usedChunks tindex) firstLeaf lastLeaf
        saveIndexMetadata new_index
        return new_index
    insertInto' :: TableIndex->(IKey->IValue)->(IKey->IKey->Ordering)->IValue->BTree->IO InsertResult
    insertInto' tindex fext fcomp item (BTreeLeaf index value prevLeaf nextLeaf p)=do
        if ((length value)+1<=(recordPerLeaf $ capacity tindex)) then do
            let new_leaf=BTreeLeaf index (insertOrdered fext fcomp item value) prevLeaf nextLeaf Nothing
            writeBTree tindex new_leaf
            return $ InsertSuccess index
        else do
            let long_value=insertOrdered fext fcomp item value
            let (lv, rv)=halfSplit long_value
            new_chunk<-allocatePage $ usedChunks tindex
            let left_leaf=BTreeLeaf index lv prevLeaf (Just new_chunk) Nothing
            let right_leaf=BTreeLeaf new_chunk rv (Just index) nextLeaf Nothing
            if(isNothing nextLeaf) then return ()
            else do
                let Just rr=nextLeaf
                (BTreeLeaf rri rrv rrp rrn rrpr)<-readBTree tindex rr Nothing
                let new_rr=BTreeLeaf rri rrv (Just new_chunk) rrn rrpr
                writeBTree tindex new_rr
            writeBTree tindex left_leaf
            writeBTree tindex right_leaf
            return $ InsertRequireSplit (fext item) index new_chunk
    insertInto' tindex fext fcomp item (BTreeNode index key children _ prevNode nextNode)=do
        let pairs=zip key children
        let k=fext item
        let (_, leastKey)=unzip $ dropWhile (\(k', c)->fcomp k' k==LT) pairs
        let pindex=if null leastKey then last children else head leastKey
        nt<-readBTree tindex pindex (Just index)
        result<-insertInto' tindex fext fcomp item nt
        if (isInsertSuccess result) then do
            let (InsertSuccess newpage)=result
            if (newpage==pindex) then return ()
            else do 
                let new_children=replaceValue pindex newpage children
                writeBTree tindex (BTreeNode index key new_children Nothing prevNode nextNode)
            return $ InsertSuccess index
        else 
            let lprefix=length $ takeWhile (\k'->fcomp k' k==LT) key 
                (InsertRequireSplit pushup_key left right)=result 
                new_keys=(take lprefix key)++(pushup_key:drop lprefix key)
                new_children=(take lprefix children)++(left:right:(drop (lprefix+1) children)) in
            if(length key+1<=(keyPerNode $ capacity tindex)) then do
                let new_node=(BTreeNode index new_keys new_children Nothing prevNode nextNode)
                writeBTree tindex new_node
                return (InsertSuccess index)
            else do
                let (left_keys, right_keys)=halfSplit new_keys
                let (left_children, right_children)=splitAt (length left_keys) new_children
                let ln_keys=left_keys
                let ln_children=left_children++[head right_children]
                let rn_keys=tail right_keys
                let rn_children=tail right_children
                let center_key=head right_keys
                new_chunk<-allocatePage $ usedChunks tindex
                let left_node=(BTreeNode index ln_keys ln_children Nothing prevNode (Just new_chunk))
                let right_node=(BTreeNode new_chunk rn_keys rn_children Nothing (Just index) nextNode)
                if(isNothing nextNode) then return ()
                else do
                    let Just rr=nextNode
                    (BTreeNode rri rrk rrc rrpr rrp rrn)<-readBTree tindex rr Nothing
                    let new_rr=BTreeNode rri rrk rrc rrpr (Just new_chunk) rrn
                    writeBTree tindex new_rr
                writeBTree tindex left_node
                writeBTree tindex right_node
                return $ InsertRequireSplit center_key index new_chunk
            
            
    insert' :: ([Word8]->[Word8])->([Word8]->[Word8]->Ordering)->TableIndex->[Word8]->IO TableIndex
    insert' fKeyExtractor fKeyComparator (TableIndex name table file tree keys capacity usedChunks firstLeaf lastLeaf) item=do
        let index= TableIndex name table file tree keys capacity usedChunks firstLeaf lastLeaf
        let key=fKeyExtractor item
        root<-readBTree index tree Nothing
        insertInto index fKeyExtractor fKeyComparator item

    extractor :: TableIndex->IValue->IKey
    extractor tindex=
        let all_domains=Table.domains $ table tindex
            selected_domains=keys tindex
            (names, _)=unzip selected_domains
        in  Table.extractKeyword all_domains names
    comparator :: TableIndex->IKey->IKey->Ordering
    comparator tindex=
        let all_domains=Table.domains $ table tindex
            selected_domains=keys tindex
        in Table.compareKeys (table tindex) selected_domains
    insert :: TableIndex->[Type.TValue]->IO TableIndex
    insert tindex item=
        let fcmp=comparator tindex
            fext=extractor tindex
            Right word=Table.writeRecord (Table.preDomain++(Table.domains $ table tindex)) item
        in insert' fext fcmp tindex word
    -- insert :: Table->TableIndex->[Word8]->IO TableIndex

    data DeleteResult=DeleteFinish | DeleteMerged Int | DeleteJuggled IKey Int | DeleteFailed

    tryToRemove :: (Eq a)=>a->[a]->([a], Bool)
    tryToRemove=rm False where
        rm ext item []=([], ext)
        rm ext item (x:xs)=if item==x then (xs, True) else let (a, b)=rm ext item xs in (x:a, b)
    deleteFrom :: TableIndex->(IValue->IKey)->(IKey->IKey->Ordering)->IValue->BTree->(Bool, Bool)->(IKey, IKey)->IO DeleteResult
    deleteFrom tindex fext fcomp item (BTreeLeaf index value prevLeaf nextLeaf p) (isLeftSibling, isRightSibling) _=do
        let (removed_value, success)=tryToRemove item value
        if(success) then do
            writeBTree tindex (BTreeLeaf index removed_value prevLeaf nextLeaf p)
            let rpl=recordPerLeaf $ capacity tindex
            let half_full=rpl-(div rpl 2)
            if(length removed_value>=half_full) then return DeleteFinish
            else do --Maybe need juggle
                if(not (isLeftSibling || isRightSibling)) then return DeleteFinish
                else do
                    f<-if(isLeftSibling) then do--Try to juggle with left sibling
                            let Just vpL=prevLeaf 
                            another<-readBTree tindex vpL Nothing
                            return (another, (BTreeLeaf index removed_value prevLeaf nextLeaf p))
                        else do
                            let Just vnL=nextLeaf --Only when leaf is left-most in siblings.
                            another<-readBTree tindex vnL Nothing
                            return ((BTreeLeaf index removed_value prevLeaf nextLeaf p), another)
                    let ((BTreeLeaf l_index l_value l_prevLeaf l_nextLeaf _),(BTreeLeaf r_index r_value r_prevLeaf r_nextLeaf _))=f
                    if ((length l_value) + (length r_value))<=rpl then do --Merge into one
                        let new_leaf=BTreeLeaf l_index (l_value++r_value) l_prevLeaf r_nextLeaf Nothing
                        freePage (usedChunks tindex) r_index
                        writeBTree tindex new_leaf
                        if(isJust r_nextLeaf) then do
                            let Just tp=r_nextLeaf
                            (BTreeLeaf t1 t2 t3 t4 t5)<-readBTree tindex tp Nothing
                            writeBTree tindex (BTreeLeaf t1 t2 (Just l_index) t4 t5)
                        else return ()

                        return $ DeleteMerged l_index
                    else do --juggle
                        let (l_nv, r_nv)=halfSplit (l_value++r_value)
                        writeBTree tindex (BTreeLeaf l_index l_nv l_prevLeaf l_nextLeaf Nothing)
                        writeBTree tindex (BTreeLeaf r_index r_nv r_prevLeaf r_nextLeaf Nothing)
                        return $ DeleteJuggled (fext $ head r_nv) l_index
        else return DeleteFailed
    deleteFrom tindex fext fcomp item (BTreeNode index key children _ prevNode nextNode) (isLeftSibling, isRightSibling) (leftHint, rightHint)=do
        let k=fext item
        let lesser_keys=takeWhile (\k'->fcomp k' k==LT) key
        let ge_keys=dropWhile (\k'->fcomp k' k==LT) key
        let target_node=head $ drop (length lesser_keys) children
        tn<-readBTree tindex target_node Nothing
        let sisLeftSibling=not $ null lesser_keys
        let sisRightSibling=not $ null ge_keys
        result<-deleteFrom tindex fext fcomp item tn (sisLeftSibling, sisRightSibling) (if sisLeftSibling then last lesser_keys else [], if sisRightSibling then head ge_keys else [])
        case result of
            DeleteFailed -> return DeleteFailed
            DeleteFinish -> return DeleteFinish
            DeleteJuggled juggleResult leftChild -> do
                let Just juggle_index=elemIndex leftChild children
                let (k1, k2)=splitAt juggle_index key
                let new_node=BTreeNode index (k1++(juggleResult: tail k2)) children Nothing prevNode nextNode
                writeBTree tindex new_node
                return DeleteFinish
            DeleteMerged leftOne -> do
                let Just merged_index=elemIndex leftOne children
                let (k1, k2)=splitAt merged_index key
                let new_keys=k1++(tail k2)
                let (c1, c2)=splitAt (merged_index+1) children
                let new_children=c1++(tail c2)
                if(not (isLeftSibling || isRightSibling)) then do
                    writeBTree tindex $ BTreeNode index new_keys new_children Nothing prevNode nextNode
                    return DeleteFinish
                else do
                    f<-if(isLeftSibling) then do--Try to juggle with left sibling
                            let Just vpL=prevNode
                            another<-readBTree tindex vpL Nothing
                            return (another, (BTreeNode index new_keys new_children Nothing prevNode nextNode), leftHint)
                        else do
                            let Just vnL=nextNode --Only when node is left-most in siblings.
                            another<-readBTree tindex vnL Nothing
                            return ((BTreeNode index new_keys new_children Nothing prevNode nextNode), another, rightHint)
                    let ((BTreeNode l_index l_key l_children _ l_prevNode l_nextNode),(BTreeNode r_index r_key r_children _ r_prevNode r_nextNode), hint)=f
                    let total_keys=l_key++[hint]++r_key
                    let total_children=l_children++r_children
                    if(length total_keys<=(keyPerNode $ capacity tindex)) then do --merge
                        let new_node=BTreeNode l_index total_keys total_children Nothing l_prevNode r_nextNode
                        freePage (usedChunks tindex) r_index
                        writeBTree tindex new_node
                        if(isJust r_nextNode) then do
                            let Just tp=r_nextNode
                            (BTreeNode t1 t2 t3 t4 t5 t6)<-readBTree tindex tp Nothing
                            writeBTree tindex (BTreeNode t1 t2 t3 t4 (Just l_index) t6)
                        else return ()
                        return $ DeleteMerged l_index
                    else do
                        let (l_nkeys, r_nkeys)=halfSplit total_keys
                        let new_left=BTreeNode l_index l_nkeys (take (length l_nkeys+1) total_children) Nothing l_prevNode l_nextNode
                        let new_right=BTreeNode r_index (tail r_nkeys) (drop (length l_nkeys+1) total_children) Nothing r_prevNode r_nextNode
                        writeBTree tindex new_left
                        writeBTree tindex new_right
                        return $ DeleteJuggled (head r_nkeys) l_index

    delete' :: (IValue->IKey)->(IKey->IKey->Ordering)->TableIndex->IValue->IO (Maybe TableIndex)
    delete' fext fcomp tindex item=do
        let TableIndex name table file tree keys capacity usedChunks firstLeaf lastLeaf=tindex
        root<-readBTree tindex tree Nothing
        result<-deleteFrom tindex fext fcomp item root (False, False) ([],[])

        case result of
            DeleteFailed -> return Nothing
            DeleteFinish -> do
                broot<-readBTree tindex tree Nothing
                nfirstLeaf<-leftLeaf tindex broot
                nlastLeaf<-rightLeaf tindex broot
                let newtindex=TableIndex name table file tree keys capacity usedChunks nfirstLeaf nlastLeaf
                saveIndexMetadata newtindex
                return $ Just newtindex
            DeleteMerged newroot -> do
                bnewroot<-readBTree tindex newroot Nothing
                nfirstLeaf<-leftLeaf tindex bnewroot
                nlastLeaf<-rightLeaf tindex bnewroot
                let newtindex= TableIndex name table file newroot keys capacity usedChunks nfirstLeaf nlastLeaf
                saveIndexMetadata newtindex
                return $ Just newtindex
    --instance RecordSource TableIndexCursor where
