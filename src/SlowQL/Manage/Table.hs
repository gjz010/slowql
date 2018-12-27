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
    import qualified SlowQL.Data.LinearTable as LT
    type ByteString=BS.ByteString
    
    data TableDef=TableDef {name :: !ByteString, domains :: !DT.Domains} deriving (Show) --Not mutable, since no alter table allowed.
    data Table=Table {def :: !TableDef, raw :: !LT.LinearTable}

    open :: String->TableDef->IO Table
    open path def=do
        lt<-LT.open path
        return $ Table def lt
    close :: Table->IO ()
    close table=do
        LT.close $ raw table

    create :: String->String->DT.Domains->IO TableDef
    create path name domains=do
        LT.initialize path 10
        return $ TableDef (BC.pack name) domains
    
    serializeTableDef :: TableDef->Put
    serializeTableDef def=do
        DT.writeString $ name def
        DT.serializeDomains $ domains def

    deserializeTableDef :: Get TableDef
    deserializeTableDef=do
        name<-DT.readString
        domains<-DT.deserializeDomains
        return $ TableDef name domains
    
    enumerateAll :: Table->ConduitT () DT.Record IO ()
    enumerateAll table=(LT.enumerate $ raw table) .| (mapC $ DT.extractAllFields (domains $ def table))
    enumerate :: Table->[String]->ConduitT () DT.Record IO ()
    enumerate table dmn=
        let source=LT.enumerate $ raw table
            extractor=mapC (DT.extractFields (domains $ def table) dmn)
        in source .| extractor

    insert :: Table->DT.Record->IO ()
    insert table rcd=do
        let put=DT.putRecord (domains $ def table) rcd
        if isLeft put
            then let Left err=put in print err
            else let Right p=put in LT.insert (runPut' p) $ raw table
    
    