{-# LANGUAGE DeriveGeneric #-}
module SlowQL.Manage.Database where
    import qualified SlowQL.Manage.Table as T
    import qualified Data.ByteString as BS
    import qualified Data.ByteString.Lazy as BL
    import qualified SlowQL.Record.DataType as DT
    import qualified Data.ByteString.Char8 as BC
    import Data.Binary.Put
    import Data.Binary.Get
    import System.IO
    import System.Directory
    import SlowQL.Utils
    import Data.IORef
    import qualified Data.Map.Strict as Map
    type ByteString=BS.ByteString
    data DatabaseDef=DatabaseDef {name :: ByteString, tables :: [T.TableDef]} deriving (Show)
    data Database=Database {def :: IORef DatabaseDef, live_tables :: IORef (Map.Map ByteString T.Table)}

    flushDatabaseDef :: Database->IO()
    flushDatabaseDef db=do
        def<-readIORef $ def db
        BL.writeFile ("slowql-data/"++(BC.unpack $ name def)++"/.slowql") $ runPut $ serialize def
    openTable :: ByteString->T.TableDef->IO (ByteString, T.Table)
    openTable base df=do
        let path="slowql-data/"++(BC.unpack base)++"/"++(BC.unpack $ T.name df)++".table"
        t<-T.open path df
        return $ (T.name df, t)
    openAllTables :: DatabaseDef->IO (Map.Map ByteString T.Table)
    openAllTables df=do
        l<-mapM (openTable $ name df) $ tables df
        return $ Map.fromList l
    serialize :: DatabaseDef->Put
    serialize def=do
        DT.writeString $ name def
        putWord32le $ fromIntegral $ length $ tables def
        mapM_ T.serializeTableDef $ tables def

    deserialize :: Get DatabaseDef
    deserialize=do
        name<-DT.readString
        tables<-DT.getList T.deserializeTableDef
        return $ DatabaseDef name tables

    listDatabases :: IO [String]
    listDatabases=listDirectory "slowql-data"
    
    open :: String->IO (Either String Database)
    open name=do
        e<-doesDirectoryExist ("slowql-data/"++name)
        if e
            then do
                str<-BL.readFile $ "slowql-data/"++name++"/.slowql"
                let df=runGet deserialize str
                lt<-openAllTables df
                r1<-newIORef df
                r2<-newIORef lt
                return $ Right $ Database r1 r2
            else return $ Left "Database does not exist."
    close :: Database->IO ()
    close db=do
        lt<-readIORef $ live_tables db
        mapM_ T.close $ Map.elems lt
        d<-readIORef $ def db
        BL.writeFile ("slowql-data/"++(BC.unpack $ name d)++"/.slowql") $runPut $ serialize d
    create :: String->IO (Maybe String)
    create db=do
        e<-doesDirectoryExist ("slowql-data/"++db)
        if e then return $ Just "Database already exists."
        else do
                createDirectory $ "slowql-data/"++db
                let def=DatabaseDef (BC.pack db) []
                BL.writeFile ("slowql-data/"++db++"/.slowql") $runPut $ serialize def
                return Nothing

    drop :: String->IO(Maybe String)
    drop db=do
        e<-doesDirectoryExist ("slowql-data/"++db)
        if e
            then do
                removeDirectoryRecursive $ "slowql-data/"++db
                return Nothing
            else return $ Just "Database does not exist."
    --data Database=Database {file :: Han}
    --createDatabase :: String->IO ()
    --createDatabase=do
