{-# LANGUAGE OverloadedStrings, DeriveGeneric#-}
module SlowQL.Record.DataType where
    import Data.Word
    import Data.Maybe
    import Data.Either
    import Data.Time
    import Data.Ord
    import Data.List
    import qualified SlowQL.PageFS as PageFS
    --import SlowQL.ListReader
    import Data.Array.IArray
    import Data.Binary.Get
    import Data.Binary.Put
    import Control.Monad
    import Data.Binary.IEEE754
    import GHC.Generics
    import qualified Data.ByteString as BS
    type ByteString=BS.ByteString
    data DataWriteError = DataWriteError ErrorType TParam TValue deriving (Show)
    data ErrorType = NullValue | ForeignKeyViolated | StringTooLong | TypeMismatch deriving (Show, Enum)
    
    data TGeneralParam=TGeneralParam {name :: ByteString, nullable :: Bool} deriving(Show)
    defaultGeneral name=TGeneralParam {name=name, nullable=True}
    data TParam = TVarCharParam {general :: TGeneralParam, max_length :: Int} 
                 | TIntParam {general :: TGeneralParam, max_length::Int}
                 | TFloatParam {general :: TGeneralParam}
                 | TDateParam {general :: TGeneralParam}
                 | TBoolParam {general ::TGeneralParam}
                 deriving (Show)

    data TValue = ValChar (Maybe ByteString) | ValInt (Maybe Int) | ValFloat (Maybe Float) | ValDate (Maybe UTCTime) |ValBool (Maybe Bool) deriving (Show, Eq, Ord)
    

    newtype Domains=Domains {domains :: Array Int TParam} deriving (Show)
    newtype Record=Record {fields :: Array Int TValue} deriving (Show, Eq, Ord)

    metaDomain :: Domains
    metaDomain=Domains $ array (0, 3) $ zip [ 0..] [TVarCharParam (defaultGeneral "name") 64, 
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
            encodeDomainLine a b c d=array (0, 3) [(0, ValChar $ Just a), (1, ValChar $ Just b), (2, ValBool $ Just c), (3, ValInt $ Just d)] :: Array Int TValue
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
                str<-getByteString strsize
                return $ ValChar $ Just str
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
    getRecord :: Domains->Get Record
    getRecord dom=do
        let arr=domains dom
        l<-mapM readField (elems arr) 
        return $ Record $ buildArray l
    --doWriteField TFloatParam{} (ValFloat (Just val) ) throw=Right $ B.unpack $ B.concat[B.singleton 0, B.pack $ writeFloat val]
    --doWriteField TIntParam{general=TGeneralParam {nullable=nullable}} (ValInt Nothing)=
    
