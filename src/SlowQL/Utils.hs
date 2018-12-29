{-# LANGUAGE OverloadedStrings #-}
module SlowQL.Utils where
    import Data.Maybe
    import Foreign.Storable
    import Data.Word
    import Foreign.Ptr
    import Data.Binary.Put
    import qualified Data.ByteString as BS
    import qualified Data.ByteString.Char8 as BC
    import qualified Data.ByteString.Lazy as BSL
    import Data.List (unfoldr)
    import qualified Data.Set as Set
    import Control.Exception 
    import Data.Time
    import Data.Binary.Get
    import Data.UUID.V4
    import Control.Monad
    import Data.UUID.V5
    import qualified Data.UUID as UUID
    import qualified System.Random.MWC as MWC
    hasDuplicates :: (Ord a) => [a] -> Bool
    hasDuplicates list = length list /= length set
                    where set = Set.fromList list
    extractMaybe :: a->Maybe a->a
    extractMaybe _ (Just b)=b
    extractMaybe a Nothing=a
    wrapMaybe :: (Eq a)=>a->a->Maybe a
    wrapMaybe d a = if a==d then Nothing else Just a

    writeInt:: Ptr a->Int->Int->IO ()
    writeInt p o v=pokeElemOff (castPtr (p `plusPtr` o) :: Ptr Word32) 0 (fromIntegral v)
    readInt::Ptr a->Int->IO Int
    readInt p o=do
        v<-peekElemOff (castPtr (p `plusPtr` o) :: Ptr Word32) 0
        return (fromIntegral v)
    writeByte :: Ptr a->Int->Word8->IO()
    writeByte p o v=pokeElemOff (castPtr (p `plusPtr`o) :: Ptr Word8) 0 v
    readByte :: Ptr a->Int->IO Word8
    readByte p o=peekElemOff (castPtr (p `plusPtr` o) :: Ptr Word8) 0
    runPut' :: Put->BS.ByteString
    runPut' =BSL.toStrict . runPut

    justWhen :: (a -> Bool) -> (a -> b) -> (a -> Maybe b)
    justWhen f g a = if f a then Just (g a) else Nothing
    nothingWhen :: (a -> Bool) -> (a -> b) -> (a -> Maybe b)
    nothingWhen f = justWhen (not . f)
    chunksOfBS :: Int -> BS.ByteString -> [BS.ByteString]
    chunksOfBS x = Data.List.unfoldr (nothingWhen BS.null (BS.splitAt x))
    rangeBS :: Int->Int->BS.ByteString->BS.ByteString
    rangeBS off len=(BS.take len).(BS.drop off)

    checkWhen :: Bool->String->IO ()->IO ()
    checkWhen pred msg op=
        if pred
            then op
            else putStrLn msg

    assert :: Bool->String->a->a
    assert False reason _=error reason
    assert True _ val=val

    assertIO :: Bool->String->IO ()
    assertIO False reason=evaluate $ error reason
    assertIO True _ = return ()

    parseDate :: String->Maybe Day
    parseDate=parseTimeM True defaultTimeLocale "%Y/%m/%d"
    parseDateBS :: BS.ByteString->Maybe Day
    parseDateBS=parseDate . BC.unpack
    getDate :: Get Day
    getDate=do
        a<-getWord32le
        b<-getWord32le
        c<-getWord32le
        return $ fromGregorian (fromIntegral a) (fromIntegral b) (fromIntegral c)
    putDate :: Day->Put
    putDate date=do
        let (a, b, c)=toGregorian date
        putWord32le $ fromIntegral a
        putWord32le $ fromIntegral b
        putWord32le $ fromIntegral c

    generateUUID :: IO BS.ByteString
    generateUUID=do
        uuid<-nextRandom
        return $ BC.pack $ UUID.toString uuid --we use ascii-form here to make it readable
    generateBulkUUID :: Int->IO [BS.ByteString]
    generateBulkUUID count=do
            seed<-nextRandom
            r<-MWC.createSystemRandom
            let gen=replicateM 64 $ (MWC.uniform r :: IO Word8)
            seeds<-replicateM count $ gen
            return $ map (BC.pack . UUID.toString . generateNamed seed) $ seeds
    uuidNegInfinity :: BS.ByteString
    uuidNegInfinity=BS.pack $ replicate 40 (fromIntegral 0)
    uuidPosInfinity :: BS.ByteString
    uuidPosInfinity=BS.pack $ replicate 40 (fromIntegral 255)
    