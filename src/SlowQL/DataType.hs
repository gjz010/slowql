module SlowQL.DataType where
    import Data.Word
    import Data.Maybe
    import Data.Either
    import Data.Time
    import qualified SlowQL.PageFS as PageFS
    import SlowQL.ListReader
    import qualified Data.ByteString.Lazy.UTF8 as UB
    import qualified Data.ByteString.Lazy as B
    data DataWriteError = DataWriteError ErrorType TParam TValue deriving (Show)
    data ErrorType = NullValue | ForeignKeyViolated | StringTooLong deriving (Show, Enum)
    
    data TGeneralParam=TGeneralParam {name :: String, nullable :: Bool} deriving(Show)
    defaultGeneral name=TGeneralParam {name=name, nullable=True}
    data TParam = TVarCharParam {general :: TGeneralParam, max_length :: Int} 
                 | TIntParam {general :: TGeneralParam, max_length::Int}
                 | TFloatParam {general :: TGeneralParam}
                 | TDateParam {general :: TGeneralParam}
                 | TBoolParam {general ::TGeneralParam}
                 deriving (Show)

    data TValue = ValChar (Maybe String) | ValInt (Maybe Int) | ValFloat (Maybe Float) | ValDate (Maybe UTCTime) |ValBool (Maybe Bool) deriving (Show, Eq)

    paramSize :: TParam->Int
    paramSize TVarCharParam{max_length=max_length}=1+4+max_length
    paramSize TIntParam{}=1+4
    paramSize TFloatParam{}=1+4
    paramSize TDateParam{}=1+8
    paramSize TBoolParam{}=1

    readField :: TParam->[Word8]->(TValue, [Word8])
    readField TVarCharParam{max_length=maxlen} iter=let (isnull, r1)=readBool iter
                                    in if isnull then (ValChar Nothing, r1)
                                        else let (strsize, str)=readInt32 r1
                                                 (strd, r2)=splitAt (fromIntegral maxlen) str
                                                 read_strd=take (fromIntegral strsize) strd
                                        in (ValChar (Just $ UB.toString (B.pack read_strd)), r2)
    readField TIntParam{} iter=let (isnull, r1)=readBool iter in
                                 if isnull then (ValInt Nothing, r1)
                                 else let (a, b)=readInt32 r1 in (ValInt $ Just (fromIntegral a), b)
    readField TFloatParam{} iter=let (isnull, r1)=readBool iter in
                                 if isnull then (ValFloat Nothing, r1)
                                 else let (a, b)=readFloat r1 in (ValFloat $ Just a, b)
    readField TBoolParam{} iter=let (val:r1)=iter    
                                in (pval val, r1)
                                where 
                                    pval 2= ValBool $ Just True
                                    pval 0= ValBool $ Just False
                                    pval 1= ValBool Nothing
                             
    readField TDateParam{} iter=(ValDate $ Just (UTCTime (ModifiedJulianDay 0) (secondsToDiffTime 0)), iter) --TODO

    writeField :: TParam->TValue->Either DataWriteError [Word8]
    writeField param value = doWriteField param value (\ etype -> Left $ DataWriteError etype param value)
    tryEmitNullFlag :: Bool->(ErrorType->Either DataWriteError [Word8])->Either DataWriteError [Word8]
    tryEmitNullFlag nullable throw=if nullable then Right [1] else throw NullValue 
    doWriteField :: TParam->TValue->(ErrorType->Either DataWriteError [Word8])->Either DataWriteError [Word8]
    doWriteField TVarCharParam{general=TGeneralParam {nullable=nullable}} (ValChar Nothing) throw=tryEmitNullFlag nullable throw
    doWriteField TIntParam{general=TGeneralParam {nullable=nullable}} (ValInt Nothing) throw=tryEmitNullFlag nullable throw
    doWriteField TFloatParam{general=TGeneralParam {nullable=nullable}} (ValFloat Nothing) throw=tryEmitNullFlag nullable throw
    doWriteField TDateParam{general=TGeneralParam {nullable=nullable}} (ValDate Nothing) throw=tryEmitNullFlag nullable throw
    doWriteField TBoolParam{general=TGeneralParam {nullable=nullable}} (ValBool Nothing) throw=tryEmitNullFlag nullable throw

    doWriteField TVarCharParam{max_length=mlen} (ValChar (Just val) ) throw=let bs=UB.fromString val
                                                                                bslen=B.length bs
                                                                         in if fromIntegral bslen>mlen then throw StringTooLong
                                                                         else let paddinglen=mlen-fromIntegral bslen
                                                                                  padding=B.pack [0| i<-[1..paddinglen]]
                                                                                  in Right $ B.unpack $ B.concat [B.singleton 0,B.pack $ writeInt32 $ fromIntegral bslen,bs, padding]

    doWriteField TIntParam{} (ValInt (Just val) ) throw=Right $ B.unpack $ B.concat[B.singleton 0, B.pack $ writeInt32 $ fromIntegral val]
    doWriteField TFloatParam{} (ValFloat (Just val) ) throw=Right $ B.unpack $ B.concat[B.singleton 0, B.pack $ writeFloat val]
    doWriteField TBoolParam{} (ValBool (Just val) ) throw=Right $ B.unpack $ B.concat[B.singleton $ if val then 2 else 0]
    --doWriteField TFloatParam{} (ValFloat (Just val) ) throw=Right $ B.unpack $ B.concat[B.singleton 0, B.pack $ writeFloat val]
    --doWriteField TIntParam{general=TGeneralParam {nullable=nullable}} (ValInt Nothing)=
    
