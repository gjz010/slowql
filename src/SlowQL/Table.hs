module SlowQL.Table where
    import qualified Data.ByteString as B
    import qualified SlowQL.PageFS as FS
    import SlowQL.DataType
    import Data.Word
    import SlowQL.ListReader
    import Data.Either
    import Data.Char
    type Domains=[TParam]
    type Record=[TValue]
    tableFileMagicNumber :: String
    tableFileMagicNumber="SLOWQLTB"
    data Table=Table {name :: String, domains:: Domains, file :: FS.DataFile}
    
    openTable :: String->IO (Maybe Table)
    openTable name = do
        let filename=name++".tbl.dat";
        fexist<-FS.exists filename
        if fexist then do
            file <- FS.openDataFile filename
            chunk <- FS.readPage file 0
            let iter=FS.iterate chunk 0
            let (magic, header)=splitAt 8 iter
            if(map (chr.fromIntegral) magic == tableFileMagicNumber) then
                return Nothing
            else return Nothing
        else return Nothing
    readRecord :: Domains->[Word8]->([TValue],[Word8])
    readRecord domains l=rzip (map readField domains) [] l

    writeRecord :: Domains->Record->Either [DataWriteError] [Word8]
    writeRecord domains record=let fields=zipWith writeField domains record in 
        if any isLeft fields then
            Left $ map (\(Left err) ->err) $ filter isLeft fields
        else Right $ concatMap (\(Right l)->l) fields