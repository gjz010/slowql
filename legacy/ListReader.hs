module SlowQL.ListReader where
    import Data.Word
    import qualified Data.ByteString as B
    import Data.Int
    import Data.Bits
    import Data.Binary.IEEE754
    import Data.List
    import Control.DeepSeq
    -- applying list of functions to peek something from list [a] 
    -- Note that this is a foldr-styled expression that supports lazy evaluation
    -- ListReader=[a]->(b, [a])

    rzipB :: [B.ByteString->(b, B.ByteString)]->[b]->B.ByteString->([b], B.ByteString)
    rzipB [] bs as=(bs, as)
    rzipB (f:fs) bs as=let (b, al)=f as
                           (bl, all)=rzipB fs bs al
                        in (b:bl, all)
    
    rzip :: [[a]->(b, [a])]->[b]->[a]->([b], [a])
    rzip [] bs as=(bs, as)
    rzip (f:fs) bs as=let (b, al)=f as
                          (bl, all)=rzip fs bs al
                      in (b:bl, all)
    
    castToInt32 :: [Word8]->Int32
    castToInt32 [a,b,c,d]=((fromIntegral a) `shiftL` 0) .|. ((fromIntegral b) `shiftL` 8) .|. ((fromIntegral c) `shiftL` 16) .|. ((fromIntegral d) `shiftL` 24) :: Int32
    readInt32 :: [Word8]->(Int32, [Word8])
    readInt32 (a:b:c:d:rest)=(castToInt32 [a,b,c,d], rest)
    
    castToBool :: Word8->Bool
    castToBool w=w/=0
    readBool :: [Word8]->(Bool, [Word8])
    readBool list=(castToBool $ head list, tail list)
    
    castToFloat :: [Word8]->Float
    castToFloat = wordToFloat.fromIntegral.castToInt32
    readFloat :: [Word8]->(Float, [Word8])
    readFloat (a:b:c:d:rest) = (castToFloat [a,b,c,d], rest)

    
    writeInt32 :: Int32->[Word8]
    writeInt32 i=map (\o->fromIntegral (i `shiftR` o)) [0,8,16,24] 
    
    writeFloat :: Float->[Word8]
    writeFloat =writeInt32.fromIntegral.floatToWord

    expandWord8 :: Word8->[Bool]
    expandWord8 word=[((word `shiftR` i) .&. 1) /=0|i<-[0..7]]

    buildWord8 :: [Bool]->Word8
    buildWord8 [a,b,c,d,e,f,g,h]=foldl' (.|.) 0 (zipWith shiftL (map (\t->if t then 1 else 0) [a,b,c,d,e,f,g,h]) [0..7]) 

    split' :: NFData a=>Int->[a]->([a], [a])
    split' n l=let a=take n l
                   b=drop n l
               in seq a $ seq b (a, b)
    chunksOf' :: NFData a=>Int->[a]->[[a]]
    chunksOf' n []=[]
    chunksOf' n l=let (a,b)=split' n l in a:(chunksOf' n b)
    