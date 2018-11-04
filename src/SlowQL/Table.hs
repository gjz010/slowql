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
    type Domains=[TParam]
    type Record=[TValue]
    tableFileMagicNumber :: String
    tableFileMagicNumber="SLOWQLTB"
    
    type Bitmap=[[Bool]]

    createBitmap :: [FS.DataChunk]->Bitmap
    createBitmap chunks=map (\chunk->concatMap (expandWord8) (FS.iterate chunk 0) ) chunks

    storeBitmap :: Bitmap->[FS.DataChunk]
    storeBitmap bitmaps=map (\bitmap->BA.pack $ map (buildWord8) (chunksOf 8 bitmap)) bitmaps

    data Table=Table {name :: String, domains:: Domains, file :: FS.DataFile, blockCount :: Int, 
    accumulator::Int, recordSize::Int, recordPerPage::Int, freeChunks :: Bitmap} deriving (Show)
    
    headerDomain :: Domains
    headerDomain=[TVarCharParam (defaultGeneral "table_name") 256, TIntParam (defaultGeneral "accumulator") 0,
     TIntParam (defaultGeneral "blockCount") 0, TIntParam (defaultGeneral "domainCount") 0, TIntParam (defaultGeneral "recordSize") 0, TIntParam (defaultGeneral "recordPerPage") 0]
    metaDomain :: Domains
    metaDomain=[TVarCharParam (defaultGeneral "name") 64, TVarCharParam (defaultGeneral "type") 16, TBoolParam (defaultGeneral "nullable"), TIntParam (defaultGeneral "maxLength") 0]
    
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
        let recordSize=sum (map paramSize domains)+6
        let recordPerPage=quot FS.pageSize recordSize;
        let table=Table name domains file 0 0 recordSize recordPerPage (createBitmap [FS.emptyChunk, FS.emptyChunk])
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

    