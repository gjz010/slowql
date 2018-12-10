module SlowQL.BitSet where
    import qualified Data.ByteArray as BA
    import Foreign.Storable
    import Foreign.Ptr
    import Data.Bits
    import Data.Word
    data BitSet=BitSet Int BA.Bytes
    zero :: Int->BitSet
    zero size=BitSet size $ BA.zero size
    size :: BitSet->Int
    size (BitSet s _)=s
    
    poffset :: Int->(Int, Int)
    poffset idx=(idx `shiftR` 3, idx .&. 7)
    getBit::Word8->Int->Bool
    getBit w i=((w `shiftR` i) .&. 1)==1

    --index :: BitSet->Int->IO Bool
    --index bitset n=do
    --    let (a, b)=poffset n
    --    return $ testBit 

