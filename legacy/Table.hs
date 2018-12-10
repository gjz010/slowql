module SlowQL.Table where
    import qualified Data.ByteString.Lazy as B
    import qualified Data.ByteString.Lazy.UTF8 as UB
    import qualified Data.ByteArray as BA
    import qualified SlowQL.PageFS as FS
    import SlowQL.DataType
    import Data.Word
    import SlowQL.ListReader
    import Data.Either
    import Data.Char
    import Data.Bits
    import Data.List.Split
    import Control.Monad
    import Data.Maybe
    import Data.List
    import Control.DeepSeq
    --import qualified Data.Set as Set
    import qualified Data.HashMap.Strict as HashMap
    import System.IO.Unsafe (unsafeInterleaveIO)
    type Domains=[TParam]
    type Record=[TValue]
    tableFileMagicNumber :: String
    tableFileMagicNumber="SLOWQLTB"
    
    newtype Bitmap=Bitmap {bitmapContent::[[Bool]]}
    --newtype BitSet=BitSet {bitsetContent::IOBitArray}

    --instance Show BitSet where
    --    show a = "<BitSet>"

    instance Show Bitmap where
        show a = "<Bitmap>"
    --createBitmap :: [FS.DataChunk]->BitSet

    --storeBitmap :: Bitmap->[FS.DataChunk]
    createBitmap :: [FS.DataChunk]->Bitmap
    createBitmap chunks=Bitmap $ map (\chunk->concatMap (expandWord8) (FS.iterate chunk 0) ) chunks

    storeBitmap :: Bitmap->[FS.DataChunk]
    storeBitmap bitmaps=map (\bitmap->BA.pack $ map (buildWord8) (let l=chunksOf 8 bitmap in deepseq l l)) $ bitmapContent bitmaps

    findFirstSlot :: [[Bool]]->Int->Int->Int->Maybe (Int, Int, Int)
    findFirstSlot ((False:xs):cs) index x y=Just (index, x, y)
    findFirstSlot ((True:xs):cs) index x y=findFirstSlot (xs:cs) (index+1) x (y+1)
    findFirstSlot ([]:cs) index x y= findFirstSlot cs index  (x+1) 0
    findFirstSlot [] index _ _=Nothing

    data Table=Table {name :: String, domains:: Domains, file :: FS.DataFile, blockCount :: Int, accumulator::Int, recordSize::Int, recordPerPage::Int, freeChunks :: Bitmap} deriving (Show)
    
    headerDomain :: Domains
    headerDomain=[TVarCharParam (defaultGeneral "table_name") 256, TIntParam (defaultGeneral "accumulator") 0,
     TIntParam (defaultGeneral "blockCount") 0, TIntParam (defaultGeneral "domainCount") 0, TIntParam (defaultGeneral "recordSize") 0, TIntParam (defaultGeneral "recordPerPage") 0]
    metaDomain :: Domains
    metaDomain=[TVarCharParam (defaultGeneral "name") 64, TVarCharParam (defaultGeneral "type") 16, TBoolParam (defaultGeneral "nullable"), TIntParam (defaultGeneral "maxLength") 0]
    
    preDomain :: Domains
    preDomain=[TBoolParam (defaultGeneral "_deleted"), TIntParam (defaultGeneral "_rid") 10]
    parseDomain :: Record->TParam
    parseDomain [ValChar (Just name), ValChar (Just ptype), ValBool (Just nullable), ValInt (Just maxLength)]=
        let g=TGeneralParam name nullable in
        case ptype of
            "varchar" -> TVarCharParam g maxLength
            "int" -> TIntParam g maxLength
            "float" -> TFloatParam g
            "date" -> TDateParam g
            "bool" -> TBoolParam g
    encodeDomainLine ::String->String->Bool->Int->Record
    encodeDomainLine name ptype nullable maxLength=[ValChar (Just name), ValChar (Just ptype), ValBool (Just nullable), ValInt (Just maxLength)]
    encodeDomain :: TParam->Record
    encodeDomain (TVarCharParam (TGeneralParam name nullable) maxLength)=encodeDomainLine name "varchar" nullable maxLength
    encodeDomain (TIntParam (TGeneralParam name nullable) maxLength)=encodeDomainLine name "int" nullable maxLength
    encodeDomain (TFloatParam (TGeneralParam name nullable))=encodeDomainLine name "float" nullable 0
    encodeDomain (TDateParam (TGeneralParam name nullable))=encodeDomainLine name "date" nullable 0
    encodeDomain (TBoolParam (TGeneralParam name nullable))=encodeDomainLine name "bool" nullable 0


    openTable :: String->IO (Maybe Table)
    openTable name = do
        let filename=name++".tbl.dat";
        fexist<-FS.exists filename
        if fexist then do
            file <- FS.openDataFile filename
            chunk <- FS.readPage file 0
            let iter=FS.iterate chunk 0
            let (magic, header)=splitAt 8 iter
            if map (chr.fromIntegral) magic == tableFileMagicNumber then do
                -- print $ readRecord headerDomain header
                let ([ValChar (Just table_name), ValInt (Just accumulator), ValInt (Just blockCount), ValInt (Just domainCount), ValInt (Just recordSize), ValInt (Just recordPerPage)], domains) = readRecord headerDomain header
                let (domainList, freemap)=rzip (replicate domainCount (readRecord metaDomain)) [] domains
                p1<-FS.readPage file 1
                p2<-FS.readPage file 2
                return $ Just $ Table table_name (map parseDomain domainList) file blockCount accumulator recordSize recordPerPage $ createBitmap [p1, p2]
            else return Nothing
        else return Nothing
    writeParam :: TParam->[Word8]
    writeParam param=let Right r=writeRecord metaDomain $encodeDomain param in r

    writeTableMetadata :: Table->IO()
    writeTableMetadata Table{SlowQL.Table.name=name, domains=domains, file=file, blockCount=blockCount, accumulator=accumulator, recordSize=recordSize, recordPerPage=recordPerPage, freeChunks=freeChunks}=do
        
        let metaDomain=concat $ map writeParam domains
        -- For a boolean and an index
        let recordSize=sum (map paramSize domains)+6
        let recordPerPage=quot FS.pageSize recordSize;
        --print ([ValChar (Just name), ValInt (Just accumulator), ValInt (Just blockCount), ValInt (Just (length domains)), ValInt (Just recordSize), ValInt (Just recordPerPage)])
        let Right header=writeRecord headerDomain [ValChar (Just name), ValInt (Just accumulator), ValInt (Just blockCount), 
                                                    ValInt (Just (length domains)), ValInt (Just recordSize), ValInt (Just recordPerPage)]
        --print header
        let (fields, _)=readRecord headerDomain header
        --print fields
        let Just chunk=FS.compact $ B.concat [ UB.fromString tableFileMagicNumber, B.pack header, B.pack metaDomain]
        --print $ B.concat [ UB.fromString tableFileMagicNumber, B.pack header, B.pack metaDomain]
        FS.writePage file 0 chunk
        let [b1, b2]=storeBitmap freeChunks
        FS.writePage file 1 b1
        FS.writePage file 2 b2
    createTable :: String->Domains->IO()
    createTable name domains = do
        let filename=name++".tbl.dat"
        file<-FS.openDataFile filename
        FS.readPage file 0
        FS.readPage file 1
        FS.readPage file 2
        FS.readPage file 3
        let recordSize=sum (map paramSize domains)+6
        let recordPerPage=quot FS.pageSize recordSize;
        let table=Table name domains file 1 0 recordSize recordPerPage (createBitmap [FS.emptyChunk, FS.emptyChunk])
        writeTableMetadata table
        FS.writeBackAll file
        FS.closeDataFile file
        return ()
    closeTable :: Table->IO()
    closeTable t =do
        writeTableMetadata t
        FS.closeDataFile $ file t 
    readRecord :: Domains->[Word8]->([TValue],[Word8])
    readRecord domains l=rzip (map readField domains) [] l

    writeRecord :: Domains->Record->Either [DataWriteError] [Word8]
    writeRecord domains record=let fields=zipWith writeField domains record in 
        if any isLeft fields then
            Left $ map (\(Left err) ->err) $ filter isLeft fields
        else Right $ concatMap (\(Right l)->l) fields


    data Cursor = Cursor Table Int [Record]| EOT deriving (Show)
    createPageCursor :: Table->Int->Int->IO Cursor
    createPageCursor table page offset=do
        if(page>=blockCount table) then 
            return EOT
        else do
            pagedata<-FS.readPageLazy (file table) (3+page) --Lazy Evaluation!
            
            let (records,_)=rzip [readRecord (preDomain++(domains table))|i<-[1..(recordPerPage table)]] [] (FS.iterate pagedata (offset*(recordSize table)) )
            -- print (BA.length pagedata, length records, head records)
            return $ Cursor table page records

    nextCursor :: Cursor->IO (Cursor, Record)
    nextCursor (Cursor table a (x:xs))= do
        if(length xs==0) then do
            newcursor<-createPageCursor table (a+1) 0
            return (newcursor, x)
        else
            return (Cursor table a xs, x)

    rawfoldrQuery :: (Record->b->b)->b->Table->IO b
    rawfoldrQuery f acc table=do
        cursor<-createPageCursor table 0 0
        _rawfoldrQuery f acc cursor
    _rawfoldrQuery :: (Record->b->b)->b->Cursor->IO b
    _rawfoldrQuery _ acc EOT=return acc
    _rawfoldrQuery f acc cursor=unsafeInterleaveIO $ do
                (newcursor, record)<-nextCursor cursor
                rc<- _rawfoldrQuery f acc newcursor
                return $ f record rc

    class RecordSource t where
        foldrQuery :: (Record->b->b)->b->t->IO b
    
    instance RecordSource Cursor where
        foldrQuery f acc cursor=
            let g= \record b->if (let (ValBool (Just val))=head record in val) then
                    f (tail (tail record)) b
                else 
                    b
            in _rawfoldrQuery g acc cursor
    instance RecordSource Table where
        --foldrQuery :: (Record->b->b)->b->Table->IO b
        foldrQuery f acc table=do
            cursor<-createPageCursor table 0 0
            foldrQuery f acc  cursor



    updateMatrix :: [[a]] -> a -> (Int, Int) -> [[a]]
    updateMatrix m x (r,c) =
        take r m ++
        [take c (m !! r) ++ [x] ++ drop (c + 1) (m !! r)] ++
        drop (r + 1) m

    
    
    createRecordValue :: Domains->Record
    createRecordValue=map createValue
    
 
    
    modifyRawRecord :: Table->Int->Int->(Record->Record)->IO (Either [DataWriteError] Record)
    modifyRawRecord table pn en f=do
        let (Table name domains file blockCount accumulator recordSize recordPerPage freeChunks)=table
        block <- FS.readPage file pn
        let (pre, currnxt)=BA.splitAt (en*recordSize) block
        let (curr, nxt)=BA.splitAt (recordSize) currnxt
       
        let (old_record, _)=readRecord (preDomain ++ domains) (BA.unpack curr)
        let result=writeRecord (preDomain ++ domains) (f old_record)
        if isLeft result then do
            let (Left error)=result
            return $ Left error
        else do
            let (Right ncurr)=result
            -- print (pn, ncurr)
            FS.writePage file pn $ BA.concat [pre, BA.pack ncurr, nxt]
            return $ Right (f old_record)
    findFirstEmptyItem :: Table->Int->IO Int
    findFirstEmptyItem table pn=do
        block <- FS.readPage (file table) pn
        let (Just index)=findIndex not (map (\el->((BA.index block (el*recordSize table))/=0)) [0..((recordPerPage table)-1)])
        return index
    insert :: Table->Record->IO (Either [DataWriteError] Table)
    insert table record = do
        let (Table name domains file blockCount accumulator recordSize recordPerPage freeChunks)=table
        let Just (page, r, c)=findFirstSlot (bitmapContent freeChunks) 3 0 0
        -- print (table, page)
        en<-findFirstEmptyItem table page
        let new_ri=accumulator+1
        -- print (record, page, r, c, en)
        result<-modifyRawRecord table page en (const ([ValBool (Just True), ValInt (Just accumulator)]++record))
        if isLeft result then do
            let (Left error)=result
            return $ Left error
        else do
            let new_mat=Bitmap $ updateMatrix (bitmapContent freeChunks) True (r, c)
            return $ Right $ Table name domains file (if en==recordPerPage-1 then blockCount+1 else blockCount) new_ri recordSize recordPerPage (if en==recordPerPage-1 then new_mat else freeChunks)
    remove :: Table->(Int, Int)->IO (Maybe [DataWriteError])
    remove table (r, c)=do 
        let (Table name domains file blockCount accumulator recordSize recordPerPage freeChunks)=table
        result<-modifyRawRecord table (r+3) c (const ([ValBool (Just False), ValInt (Just 0)]++(createRecordValue domains)))
        if isLeft result then do 
            let (Left error)=result
            return $ Just error
        else return Nothing

    getDomainByName :: Table->String->TParam
    getDomainByName table name= let domain_map=map (\v->(SlowQL.DataType.name $ general v, v)) (domains table) 
                                    in lookupInList name domain_map
    
    extractKey :: Domains->[String]->[Word8]->(Domains, Record)
    extractKey domains names rawrecord=
        let all_domains=preDomain++domains
            (fields, _)=readRecord all_domains rawrecord
            dv_pair=zip (map (SlowQL.DataType.name . general) all_domains) (zip all_domains fields)
            dv_map=HashMap.fromList dv_pair
        in unzip $ map (\n->let (Just v)=HashMap.lookup n dv_map in v) names
    lookupInList :: Eq a=>a->[(a,b)]->b
    lookupInList x ((x', y):xs)=if x==x' then y else lookupInList x xs
    extractKeyword :: Domains->[String]->[Word8]->[Word8]
    extractKeyword a b w=
        let sizes=map paramSize a
            offset=scanl1 (+) sizes
            ranges=zipWith (\s o->(o-s, s)) sizes offset
            dv_pair=zip (map (SlowQL.DataType.name . general) a) ranges
            dv_map=HashMap.fromList dv_pair
            real_ranges=map (\n->lookupInList n dv_pair) b
            tr (d, t) w'=take t $ drop d w'
        in concatMap (`tr` w) real_ranges


    compareKeys :: Table->[(String, Bool)]->[Word8]->[Word8]->Ordering
    compareKeys table name_dir a b=
        let (names, dirs)=unzip name_dir
            domains=map (getDomainByName table) names
            (a', _)=readRecord domains a
            (b', _)=readRecord domains b
            (a'', b'')=unzip $ zipWith3 (\v1 v2 asc->if asc then (v2, v1) else (v1, v2)) a' b' dirs
        in compare a'' b''