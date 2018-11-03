module SlowQL.DataType where
    import Data.Word
    import Data.Maybe
    import Data.Either
    import Data.Time
    import qualified SlowQL.PageFS as PageFS
    data DataWriteError = DataWriteError ErrorType TParam TValue
    data ErrorType = NullValue | ForeignKeyViolated deriving (Show, Enum)
    
    data TGeneralParam=TGeneralParam {name :: String, nullable :: Bool, primary :: Bool} deriving(Show)
    data TParam = TVarCharParam {general :: TGeneralParam, max_length :: Int} 
                 | TIntParam {general :: TGeneralParam}
                 | TFloatParam {general :: TGeneralParam}
                 | TDateParam {general :: TGeneralParam}
                 deriving (Show)

    data TValue = ValChar (Maybe String) | ValInt (Maybe Int) | ValFloat (Maybe Float) | ValDate (Maybe UTCTime)  deriving (Show, Eq)

    readField :: TParam->[Word8]->(TValue, [Word8])
    readField TVarCharParam{} iter=(ValChar $ Just "Test", iter)
    readField TIntParam{} iter=(ValInt $ Just 12345, iter)
    readField TFloatParam{} iter=(ValFloat $ Just 1.414, iter)
    readField TDateParam{} iter=(ValDate $ Just (UTCTime (ModifiedJulianDay 0) (secondsToDiffTime 0)), iter)

    writeField :: TParam->TValue->Either DataWriteError [Word8]
    writeField param value = doWriteField param value (\ etype -> Left $ DataWriteError etype param value)
    doWriteField :: TParam->TValue->(ErrorType->Either DataWriteError [Word8])->Either DataWriteError [Word8]
    doWriteField TVarCharParam{general=general} (ValChar string) throw =throw NullValue 

