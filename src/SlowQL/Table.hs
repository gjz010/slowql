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
    type Domains=[TParam]
    type Record=[TValue]
    tableFileMagicNumber :: String
    tableFileMagicNumber="SLOWQLTB"
    
    newtype Bitmap=Bitmap {bitmapContent::[[Bool]]}

    instance Show Bitmap where
        show a = "<Bitmap>"
    createBitmap :: [FS.DataChunk]->Bitmap
    createBitmap chunks=Bitmap $ map (\chunk->concatMap (expandWord8) (FS.iterate chunk 0) ) chunks

    storeBitmap :: Bitmap->[FS.DataChunk]
    storeBitmap bitmaps=map (\bitmap->BA.pack $ map (buildWord8) (chunksOf 8 bitmap)) $ bitmapContent bitmaps

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
                print $ readRecord headerDomain header
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
        let Right header=writeRecord headerDomain [ValChar (Just name), ValInt (Just accumulator), ValInt (Just blockCount), 
                                                    ValInt (Just (length domains)), ValInt (Just recordSize), ValInt (Just recordPerPage)]
        let Just chunk=FS.compact $ B.concat [ UB.fromString tableFileMagicNumber, B.pack header, B.pack metaDomain]
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
    closeTable Table{file=f} =FS.closeDataFile f 
    readRecord :: Domains->[Word8]->([TValue],[Word8])
    readRecord domains l=rzip (map readField domains) [] l

    writeRecord :: Domains->Record->Either [DataWriteError] [Word8]
    writeRecord domains record=let fields=zipWith writeField domains record in 
        if any isLeft fields then
            Left $ map (\(Left err) ->err) $ filter isLeft fields
        else Right $ concatMap (\(Right l)->l) fields


    data Cursor = Cursor Table Int [Record]| EOT
    createPageCursor :: Table->Int->Int->IO Cursor
    createPageCursor table page offset=do
        if(page>=blockCount table) then 
            return EOT
        else do
            pagedata<-FS.readPageLazy (file table) (3+page) --Lazy Evaluation!
            let (records,_)=rzip [readRecord (preDomain++(domains table))|i<-[1..(recordPerPage table)]] [] (FS.iterate pagedata (offset*(recordSize table)) )
            return $ Cursor table page records

    nextCursor :: Cursor->IO (Cursor, Record)
    nextCursor (Cursor table a (x:xs))= do
        if(length xs==0) then do
            newcursor<-createPageCursor table (a+1) 0
            return (newcursor, x)
        else
            return (Cursor table a xs, x)

    foldrQuery :: (Record->b->b)->b->Table->IO b
    foldrQuery f acc table=do
        cursor<-createPageCursor table 0 0
        _foldrQuery f acc cursor
    _foldrQuery :: (Record->b->b)->b->Cursor->IO b
    _foldrQuery _ acc EOT=return acc
    _foldrQuery f acc cursor=do
                (newcursor, record)<-nextCursor cursor
                rc<-_foldrQuery f acc newcursor
                return $ f record rc


    updateMatrix :: [[a]] -> a -> (Int, Int) -> [[a]]
    updateMatrix m x (r,c) =
        take r m ++
        [take c (m !! r) ++ [x] ++ drop (c + 1) (m !! r)] ++
        drop (r + 1) m

    
    createRecordValue :: Domains->Record
    createRecordValue=map createValue
    modifyRawRecord :: Table->Int->Int->(Record->Record)->IO Record
    modifyRawRecord (Table name domains file blockCount accumulator recordSize recordPerPage freeChunks) pn en f=do
        return (f $ createRecordValue domains)
    insert :: Table->Record->IO Table
    insert table record = do
        let (Table name domains file blockCount accumulator recordSize recordPerPage freeChunks)=table
        let Just (page, r, c)=findFirstSlot (bitmapContent freeChunks) 3 0 0
        page_data<-FS.readPage file page
        let new_ri=accumulator+1
        modifyRawRecord table r c (const ([ValBool (Just True), ValInt (Just new_ri)]++record))
        let new_mat=Bitmap $ updateMatrix (bitmapContent freeChunks) True (r, c)
        return $ Table name domains file blockCount (accumulator+1) recordSize recordPerPage (if c==recordPerPage-1 then new_mat else freeChunks)
    remove :: Table->(Int, Int)->IO ()
    remove table (r, c)=do 
        let (Table name domains file blockCount accumulator recordSize recordPerPage freeChunks)=table
        modifyRawRecord table r c (const ([ValBool (Just False), ValInt (Just 0)]++(createRecordValue domains)))
        return ()