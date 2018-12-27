{-# LANGUAGE OverloadedStrings, DeriveGeneric#-}
module SlowQL.Record.DataType where
    import Data.Word
    import Data.Maybe
    import Data.Either
    import Data.Time
    import Data.Ord
    import Data.List
    import qualified Debug.Trace as Trace
    import qualified SlowQL.PageFS as PageFS
    --import SlowQL.ListReader
    import Data.Array.IArray
    import Data.Binary.Get
    import Data.Binary.Put
    import Control.Monad
    import Data.Binary.IEEE754
    import GHC.Generics
    import qualified Data.ByteString as BS
    import qualified Data.ByteString.Lazy
    import qualified Data.ByteString.Char8 as BC
    import Control.DeepSeq
    type ByteString=BS.ByteString
    data DataWriteError = DataWriteError ErrorType TParam TValue deriving (Show)
    data ErrorType = NullValue | ForeignKeyViolated | StringTooLong | TypeMismatch deriving (Show, Enum)
    
    data TGeneralParam=TGeneralParam {name :: ByteString, nullable :: Bool} deriving(Show, Generic)
    defaultGeneral name=TGeneralParam {name=name, nullable=True}
    data TParam = TVarCharParam {general :: TGeneralParam, max_length :: Int} 
                 | TIntParam {general :: TGeneralParam, max_length::Int}
                 | TFloatParam {general :: TGeneralParam}
                 | TDateParam {general :: TGeneralParam}
                 | TBoolParam {general ::TGeneralParam}
                 deriving (Show, Generic)

    data TValue = ValChar (Maybe ByteString) | ValInt (Maybe Int) | ValFloat (Maybe Float) | ValDate (Maybe UTCTime) |ValBool (Maybe Bool) | ValNull deriving (Show, Generic)
    instance NFData TParam 
    instance NFData TValue
    instance NFData TGeneralParam
    instance Eq TValue where
        ValChar a == ValChar b = a==b
        ValInt a == ValInt b = a==b
        ValFloat a == ValFloat b = a==b
        ValDate a == ValDate b = a==b
        ValBool a == ValBool b = a==b
        _ == _ = error "Bad Eql!"
    instance Ord TValue where
        (ValChar Nothing) <= (ValChar _)=True
        (ValChar (Just a)) <= (ValChar (Just b))=a<=b
        (ValInt Nothing) <= (ValInt _)=True
        (ValInt (Just a)) <= (ValInt (Just b))=a<=b
        (ValFloat Nothing) <= (ValFloat _)=True
        (ValFloat (Just a)) <= (ValFloat (Just b))=a<=b
        (ValDate Nothing) <= (ValDate _)=True
        (ValDate (Just a)) <= (ValDate (Just b))=a<=b
        (ValBool Nothing) <= (ValBool _)=True
        (ValBool (Just a)) <= (ValBool (Just b))=a<=b
        _ <= _ = error "Bad Comparison!"

    compatiblePP :: TParam->TParam->Bool
    compatiblePP TVarCharParam{} TVarCharParam{}=True
    compatiblePP TIntParam{} TIntParam{}=True
    compatiblePP TFloatParam{} TFloatParam{}=True
    compatiblePP TDateParam{} TDateParam{}=True
    compatiblePP TBoolParam{} TBoolParam{}=True
    compatiblePP _ _=False
    compatiblePV :: TParam->TValue->Bool
    compatiblePV TVarCharParam{} ValChar{}=True
    compatiblePV TIntParam{} ValInt{}=True
    compatiblePV TFloatParam{} ValFloat{}=True
    compatiblePV TDateParam{} ValDate{}=True
    compatiblePV TBoolParam{} ValBool{}=True
    compatiblePV _ _=False
    isNull :: TValue->Bool
    isNull (ValChar a)=isNothing a
    isNull (ValInt a)=isNothing a
    isNull (ValFloat a)=isNothing a
    isNull (ValDate a)=isNothing a
    isNull (ValBool a)=isNothing a
    newtype Domains=Domains {domains :: Array Int TParam} deriving (Show)
    newtype Record=Record {fields :: Array Int TValue} deriving (Show, Eq, Ord)

    metaDomain :: Domains
    metaDomain=Domains $ buildArray [TVarCharParam (defaultGeneral "name") 64, 
        TVarCharParam (defaultGeneral "type") 16, 
        TBoolParam (defaultGeneral "nullable"), 
        TIntParam (defaultGeneral "maxLength") 0]
    justRight :: Either a b->b
    justRight (Right b)=b
    serializeDomains :: Domains->Put
    second (a,b)=b
    serializeDomains dom=(putWord32le $ fromIntegral (1+(second $ bounds $ domains dom)))>>= (const $ mapM_ serializeParam $ elems $ domains dom)
    serializeParam :: TParam->Put
    serializeParam param=justRight $ putRecord metaDomain $ Record $ encodeDomain param
        where 
            encodeDomainLine a b c d=buildArray [ValChar $ Just a, ValChar $ Just b, ValBool $ Just c,  ValInt $ Just d] :: Array Int TValue
            encodeDomain (TVarCharParam (TGeneralParam name nullable) maxLength)=encodeDomainLine name "varchar" nullable maxLength
            encodeDomain (TIntParam (TGeneralParam name nullable) maxLength)=encodeDomainLine name "int" nullable maxLength
            encodeDomain (TFloatParam (TGeneralParam name nullable))=encodeDomainLine name "float" nullable 0
            encodeDomain (TDateParam (TGeneralParam name nullable))=encodeDomainLine name "date" nullable 0
            encodeDomain (TBoolParam (TGeneralParam name nullable))=encodeDomainLine name "bool" nullable 0
    deserializeParam :: Get TParam
    deserializeParam=do
        r<-getRecord metaDomain
        let parseDomain [ValChar (Just name), ValChar (Just ptype), ValBool (Just nullable), ValInt (Just maxLength)]=
                let g=TGeneralParam name nullable in
                case ptype of
                    "varchar" -> TVarCharParam g maxLength
                    "int" -> TIntParam g maxLength
                    "float" -> TFloatParam g
                    "date" -> TDateParam g
                    "bool" -> TBoolParam g
                    otherwise -> Trace.trace (show r) undefined
            in return $ parseDomain $ elems $ fields r
    deserializeDomains :: Get Domains
    deserializeDomains=fmap (\l->Domains $ buildArray l) (getList deserializeParam)
    paramSize :: TParam->Int
    paramSize TVarCharParam{max_length=max_length}=1+4+max_length
    paramSize TIntParam{}=1+4
    paramSize TFloatParam{}=1+4
    paramSize TDateParam{}=1+8
    paramSize TBoolParam{}=1

    createValue :: TParam->TValue
    createValue TVarCharParam{max_length=max_length}=ValChar(Just "")
    createValue TIntParam{}=ValInt(Just 0)
    createValue TFloatParam{}=ValFloat(Just 0.0)
    createValue TDateParam{}=ValDate(Just (UTCTime (ModifiedJulianDay 0) (secondsToDiffTime 0)))
    createValue TBoolParam{}=ValBool(Just False)
    readBool :: Get Bool
    readBool=do
        b<-getWord8
        return (b/=0)
    readInt32 :: Get Int
    readInt32=do
        w<-getWord32le
        return $ fromIntegral w
    readFloat :: Get Float
    readFloat=do
        w<-getWord32le
        return $ wordToFloat w
    getList :: Get a->Get [a]
    getList f=do
        l<-getWord32le
        replicateM (fromIntegral l) f
    checkNull :: a->Get a->Get a
    checkNull vnil m=do
        isnull<-readBool
        if isnull
            then return vnil
            else m
    readString :: Get ByteString
    readString=getWord32le >>= (getByteString. fromIntegral)
    writeString :: ByteString->Put
    writeString bs=do
        putWord32le $ fromIntegral $ BS.length bs
        putByteString bs
    readField :: TParam->Get TValue
    readField TVarCharParam{max_length=maxlen}=checkNull (ValChar Nothing) $ do
                strsize<-readInt32
                str<-getByteString maxlen
                return $ ValChar $ Just (BS.take strsize str)
    readField TIntParam{}=checkNull (ValInt Nothing) $ fmap (ValInt . Just) readInt32
    readField TBoolParam{}=do
        v<-getWord8
        return $ case v of
                    2 -> ValBool $ Just True
                    0 -> ValBool $ Just False
                    1 -> ValBool Nothing 
    readField TFloatParam{}=checkNull (ValFloat Nothing) $ fmap (ValFloat . Just) readFloat
                             
    readField TDateParam{}=return $ ValDate $ Just (UTCTime (ModifiedJulianDay 0) (secondsToDiffTime 0)) --TODO

    writeField :: TParam->TValue->Either DataWriteError Put
    writeField param value = doWriteField param value (\ etype -> Left $ DataWriteError etype param value)
    tryEmitNullFlag :: Bool->(ErrorType->Either DataWriteError Put)->Either DataWriteError Put
    tryEmitNullFlag nullable throw=if nullable then Right $ putWord8 1 else throw NullValue
    doWriteField :: TParam->TValue->(ErrorType->Either DataWriteError Put)->Either DataWriteError Put
    doWriteField TVarCharParam{general=TGeneralParam {nullable=nullable}} (ValChar Nothing) throw=tryEmitNullFlag nullable throw
    doWriteField TIntParam{general=TGeneralParam {nullable=nullable}} (ValInt Nothing) throw=tryEmitNullFlag nullable throw
    doWriteField TFloatParam{general=TGeneralParam {nullable=nullable}} (ValFloat Nothing) throw=tryEmitNullFlag nullable throw
    doWriteField TDateParam{general=TGeneralParam {nullable=nullable}} (ValDate Nothing) throw=tryEmitNullFlag nullable throw
    doWriteField TBoolParam{general=TGeneralParam {nullable=nullable}} (ValBool Nothing) throw=tryEmitNullFlag nullable throw

    doWriteField TVarCharParam{max_length=mlen} (ValChar (Just val) ) throw=if  fromIntegral (BS.length val)>mlen
        then throw StringTooLong
        else Right $ do
                putWord8 0
                putWord32le $ fromIntegral $ BS.length val
                putByteString val
                let paddinglen=mlen-(fromIntegral $BS.length val)
                putByteString (BS.pack [0|i<-[1..paddinglen]])
        
 
    doWriteField TIntParam{} (ValInt (Just val) ) throw=Right $ do
        putWord8 0
        putWord32le $ fromIntegral val
        
    doWriteField TFloatParam{} (ValFloat (Just val) ) throw=Right $ do
        putWord8 0
        putWord32le $ floatToWord val
    doWriteField TBoolParam{} (ValBool (Just val) ) throw=Right $ putWord8 (if val then 2 else 0)
    doWriteField a b throw=throw TypeMismatch

    putRecord :: Domains->Record->Either DataWriteError Put
    putRecord dom record=foldl' go (Right $ return ()) $ zip (elems $ domains dom) (elems $ fields record)
        where go (Left x) _ = Left x
              go (Right op) nop =let rop=uncurry writeField nop in if isLeft rop then rop else Right (op >>= (const $ justRight rop))
    buildArray :: (IArray a e)=>[e]->a Int e
    buildArray l=array (0, (length l)-1) $ zip [0..] l
    buildArray' :: [e]->Array Int e
    buildArray'=buildArray
    getRecord :: Domains->Get Record
    getRecord dom=do
        let arr=domains dom
        l<-mapM readField (elems arr) 
        return $ Record $ buildArray l

    extractAllFields :: Domains->ByteString->Record
    extractAllFields d s=runGet (getRecord d) $ Data.ByteString.Lazy.fromStrict s
    extractFields :: Domains->[String]->ByteString->Record
    extractFields pdomains selector=
        let Domains domains=pdomains
            offsets=buildArray' $ scanl (+) 0 $ map paramSize $ elems domains
            findfield str=findIndex (\d->str==(name $ general d)) $ elems domains
            field_hint=buildArray' $ map (\(Just i)->(readField (domains!i), offsets!i)) $ map findfield $ map BC.pack selector
            parse_field bs (get, offset)=runGet get $ Data.ByteString.Lazy.fromStrict (BS.drop offset bs)
        in (\bs->Record $ amap (parse_field bs) field_hint)
    

                

    --doWriteField TFloatParam{} (ValFloat (Just val) ) throw=Right $ B.unpack $ B.concat[B.singleton 0, B.pack $ writeFloat val]
    --doWriteField TIntParam{general=TGeneralParam {nullable=nullable}} (ValInt Nothing)=
    
