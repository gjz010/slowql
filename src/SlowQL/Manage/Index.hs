{-# LANGUAGE DeriveGeneric#-}
module SlowQL.Manage.Index where
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
    import qualified Data.Map as Map
    import Data.List
    import qualified SlowQL.Data.LinearTable as LT
    import qualified SlowQL.Data.BTreeTable as BT
    import qualified SlowQL.Manage.Table as T
    import qualified Data.Array.IArray as Arr
    import Debug.Trace
    type ByteString=BS.ByteString
    
    data IndexDef=IndexDef {name :: !ByteString, target :: !Int} deriving (Show, Eq, Ord) --Not mutable, since no alter table allowed.
    data Index=Index {def :: !IndexDef, raw :: !BT.BTreeTable, hint :: BT.KeyHint, param :: DT.TParam, tdef :: !T.TableDef}
    instance Show Index where
        show Index{def=d} = show d
    -- Sort based on pair (target, _rid)
    open :: String->IndexDef->T.TableDef->IO Index
    open path def tdef=do
        bt<-BT.open path
        let par=(Arr.!) (DT.domains $ T.domains tdef) (target def)
        let all_params=map DT.paramSize $ Arr.elems $ DT.domains $ T.domains tdef
        let offsets=scanl (+) 0 all_params

        let fext bs=BS.concat [BS.take (DT.paramSize par) $ BS.drop (head $ drop (target def) offsets) bs,BS.take (DT.paramSize DT.ridParam) bs]
        let fcmp a b=
                let domains=DT.Domains $ DT.buildArray' [par, DT.ridParam]
                    r1=runGet (DT.getRecord domains) $ BL.fromStrict a
                    r2=runGet (DT.getRecord domains) $ BL.fromStrict b
                in compare r1 r2
        return $ Index def bt (fext, fcmp) par tdef 
    
    --createIndex :: Table->String->Int->IO()
    --createIndex table path cid=do
        

    --dropIndex :: Table->Int->IO()
    --dropIndex table cid=do

    --enumerateRange :: Table->Int->Maybe (DT.TValue, Bool)->Maybe (DT.TValue, Bool)->ConduitT () DT.Record IO ()
    --enumerateRange :: 
    close :: Index->IO ()
    close index=do
        BT.close $ raw index

    create :: String->T.TableDef->Int->IO IndexDef
    create path def column=do
        let record_size=(DT.calculateSize $ T.domains def)+4
        let key_size=DT.paramSize ((Arr.!) (DT.domains $ T.domains def) column)+(DT.paramSize DT.ridParam)
        BT.initialize path record_size key_size
        return $ IndexDef (T.name def) column
    
    serializeIndexDef :: IndexDef->Put
    serializeIndexDef def=do
        DT.writeString $ name def
        putWord32le $ fromIntegral $ target def

    deserializeIndexDef :: Get IndexDef
    deserializeIndexDef=do
        name<-DT.readString
        target<-getWord32le
        return $ IndexDef name $ fromIntegral target
    showLeft :: (Show a)=>Either a b->String
    showLeft (Left a)=show a
    showLeft (Right b)="[Right]"
    resolveBound :: DT.TParam->Bool->Maybe (DT.TValue, Bool)->Maybe (BS.ByteString, Bool)
    resolveBound param lower bound=do
        (val, inclusive)<-bound
        let use_pos_infinity=(lower && (not inclusive)) || ((not lower) && inclusive)
        let put=do
                let Right p= DT.writeField param val
                let Right q=DT.writeField DT.ridParam $ DT.ValChar $ Just $ if use_pos_infinity then uuidPosInfinity else uuidNegInfinity
                p
                q
        return $ (runPut' put, True)
    enumerateRange :: Index->Maybe (DT.TValue, Bool)->Maybe (DT.TValue, Bool)->ConduitT () DT.Record IO ()
    enumerateRange index lower upper=
        (BT.enumerateRange (raw index) (hint index) (resolveBound (param index) True lower) (resolveBound (param index) False upper)) .| (mapC $ DT.extractAllFields (T.domains $ tdef index))
    checkUnique :: Index->DT.TValue->IO Bool
    checkUnique index v=runConduit $ enumerateRange index (Just (v, True)) (Just (v, True)) .| nullC
    {-insert :: Index->DT.Record->Int->IO ()
    insert index rcd idx=do
        let put=DT.putRecord (T.domains $ tdef index) rcd
        if isLeft put
            then let Left err=put in print err
            else let (Right p) = put
                     putAll = do
                                 p
                                 putWord32le $ fromIntegral idx
                     literal=runPut' putAll
                 in BT.insert (raw index) (hint index) literal
    delete :: Index->DT.Record->Int->IO ()
    delete index rcd =do
        let put=DT.putRecord (T.domains $ tdef index) rcd
        if isLeft put
            then let Left err=put in print err
            else let Right p=put 
                     putAll=do
                            p
                            putWord32le $ fromIntegral idx
                     literal=runPut' putAll
                in BT.delete (raw index) (hint index) literal
    -}
    merge :: (BS.ByteString, Int)->BS.ByteString
    merge (str, idx)=runPut' $ putByteString str >>= (const $ putWord32le $ fromIntegral idx) 
    hookInsert :: Index->(BS.ByteString, Int)->IO ()
    hookInsert index (bs, idx)=let total=merge (bs, idx) in BT.insert (raw index) (hint index) total
    hookDelete :: Index->(BS.ByteString, Int)->IO ()
    hookDelete index (bs, idx)=let total=merge (bs, idx) in BT.delete (raw index) (hint index) total >>= const (return ())
    hookUpdate :: Index->Int->BS.ByteString->BS.ByteString->IO ()
    hookUpdate index idx olds news=do
        hookDelete index (olds, idx)
        hookInsert index (news, idx)
    seed :: Index->T.Table->IO ()
    seed index table=
        let do_insert (a,b)= do
                liftIO $ BT.insert (raw index) (hint index) (merge (b,a))
                return ()
        in runConduit $ (LT.enumerateWithIdx $ T.raw table) .| awaitForever do_insert