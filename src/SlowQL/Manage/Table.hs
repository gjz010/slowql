{-# LANGUAGE DeriveGeneric#-}
module SlowQL.Manage.Table where
    import qualified SlowQL.Record.DataType as DT
    import qualified Data.ByteString as BS
    import qualified Data.ByteString.Lazy as BL
    import qualified Data.ByteString.Char8 as BC
    import Data.Aeson
    import GHC.Generics
    import Data.Binary.Put
    import Data.Binary.Get
    import Conduit
    import Data.Conduit
    import Control.DeepSeq
    import Data.Either
    import SlowQL.Utils
    import Data.Maybe
    import Data.IORef
    import Data.Map
    import qualified SlowQL.Data.LinearTable as LT
    import qualified SlowQL.Data.BTreeTable as BT
    type ByteString=BS.ByteString
    
    data TableDef=TableDef {name :: !ByteString, domains :: !DT.Domains, primary :: !Int, fkey :: ![(Int, ByteString, Int)]} deriving (Show) --Not mutable, since no alter table allowed.
    data Table=Table {def :: !TableDef, raw :: !LT.LinearTable}
    instance Show Table where
        show Table{def=d}=show d
    open :: String->TableDef->IO Table
    open path def=do
        lt<-LT.open path
        return $ Table def lt

    --createIndex :: Table->String->Int->IO()
    --createIndex table path cid=do
        

    --dropIndex :: Table->Int->IO()
    --dropIndex table cid=do

    --enumerateRange :: Table->Int->Maybe (DT.TValue, Bool)->Maybe (DT.TValue, Bool)->ConduitT () DT.Record IO ()
    --enumerateRange :: 
    close :: Table->IO ()
    close table=do
        LT.close $ raw table

    create :: String->String->DT.Domains->Int->[(Int, ByteString, Int)]->IO TableDef
    create path name domains primary fkey=do
        LT.initialize path (DT.calculateSize domains)
        return $ TableDef (BC.pack name) domains primary fkey
    
    serializeTableDef :: TableDef->Put
    serializeTableDef def=do
        DT.writeString $ name def
        putWord32le $ fromIntegral $ primary def
        DT.serializeDomains $ domains def
        putWord32le $ fromIntegral $ length $ fkey def
        mapM_ (\(a, b, c)->putWord32le (fromIntegral a) >>= const (DT.writeString b) >>= const (putWord32le $ fromIntegral c)) $ fkey def

    deserializeTableDef :: Get TableDef
    deserializeTableDef=do
        name<-DT.readString
        prim<-getWord32le
        domains<-DT.deserializeDomains
        fkey<-DT.getList $ do
                a<-getWord32le
                b<-DT.readString
                c<-getWord32le
                return $ (fromIntegral a,b,fromIntegral c)
        return $ TableDef name domains (fromIntegral prim) fkey
    
    enumerateAll :: Table->ConduitT () DT.Record IO ()
    enumerateAll table=(LT.enumerate $ raw table) .| (mapC $ DT.extractAllFields (domains $ def table))
    enumerateAllWithIdx :: Table->ConduitT () (Int, DT.Record) IO ()
    enumerateAllWithIdx table=(LT.enumerateWithIdx $ raw table) .| (mapC $ fmap $ DT.extractAllFields (domains $ def table))
    enumerate :: Table->[String]->ConduitT () DT.Record IO ()
    enumerate table dmn=
        let source=LT.enumerate $ raw table
            extractor=mapC (DT.extractFields (domains $ def table) dmn)
        in source .| extractor

    testInsert :: Table->DT.Record->Bool
    testInsert table rcd=isRight $ DT.putRecord (domains $ def table) rcd
    insert :: Table->DT.Record->((ByteString, Int)-> IO ())->IO ()
    insert table rcd hook =do
        let put=DT.putRecord (domains $ def table) rcd
        if isLeft put
            then let Left err=put in print err
            else let Right p=put in do
                        let bs=runPut' p
                        LT.insert (runPut' p) (raw table) hook
    
    update :: Table->(DT.Record->Maybe DT.Record)->(Int->ByteString->ByteString->IO ())->IO Int
    update table updator hook=do
        let fext=DT.extractAllFields (domains $ def table)
        let raw_updator bs=do
                let rc=fext bs
                let nrcd=updator rc
                if(isJust nrcd)
                    then do
                        let Just rcd=nrcd
                        let put=DT.putRecord (domains $ def table) rcd
                        if isLeft put
                            then let Left err=put in print err >>= (const $ return Nothing)
                            else let Right p=put in return $ Just (runPut' p)
                    else return Nothing
        LT.update raw_updator (raw table) hook

    delete :: Table->(DT.Record->Bool)->((ByteString, Int)->IO ())->IO Int
    delete table filter hook =do
        let fext=DT.extractAllFields (domains $ def table)
        let raw_filter=return .filter.fext
        LT.delete raw_filter (raw table) hook
        