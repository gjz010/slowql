module SlowQL.Utils where
    import Data.Maybe
    import Foreign.Storable
    import Data.Word
    import Foreign.Ptr
    import Data.Binary.Put
    import Data.ByteString as BS
    import Data.ByteString.Lazy as BSL
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

    runPut' :: Put->BS.ByteString
    runPut' =BSL.toStrict . runPut
