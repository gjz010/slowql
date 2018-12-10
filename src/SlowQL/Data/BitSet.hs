module SlowQL.Data.BitSet where
    import Data.Word
    import Data.Bits
    import Data.Maybe
    import Foreign.Storable
    import Foreign.Ptr
    flipFirstZero :: Word8->(Word8, Maybe Int)
    flipFirstZero w
        | w .&. 1 ==0 = (w .|. 1, Just 0)
        | w .&. 2 == 0 = (w.|.2, Just 1)
        | w .&. 4 == 0 = (w.|.4, Just 2)
        | w .&. 8 == 0 = (w.|.8, Just 3)
        | w .&. 16 == 0 = (w.|.16, Just 4)
        | w .&. 32 == 0 = (w.|.32, Just 5)
        | w .&. 64 == 0 = (w.|.64, Just 6)
        | w .&. 128 == 0 = (w.|.128, Just 7)
        | otherwise = (w, Nothing)
    alloc :: Int->Ptr a->IO Int
    alloc maxsize p=
            loop 0
        where
            loop n=
                if n==maxsize
                    then return $ error "BitSet Exhausted!"
                    else 
                        do
                            b<-peekElemOff (castPtr p :: Ptr Word8) n
                            let ( newword, pos)=flipFirstZero b
                            if isJust pos
                                then do
                                    let Just jp=pos
                                    pokeElemOff (castPtr p ::Ptr Word8) n newword
                                    return (jp+n*8)
                                else loop (n+1)


    free :: Int->Ptr a->IO ()
    free pos p=do
        let (q, r)=(pos `quot` 8, pos `rem` 8)
        b<-peekElemOff (castPtr p::Ptr Word8) q
        pokeElemOff (castPtr p::Ptr Word8) q (clearBit b r)