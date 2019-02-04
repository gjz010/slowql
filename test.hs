import Control.Exception
data MyException = ThisException | ThatException
    deriving Show
instance Exception MyException
p=do
    throw ThisException
    print "hahaha"

binsearch :: (Monad m)=>(Int->m Ordering)->Int->Int->m Int
binsearch funct{- compare to-insert key with the existing key -} l r=
    let go l r=if l==r 
                    then return l
                    else do
                        let mid=(l+r) `quot` 2 -- l<=mid<mid+1<=r
                        ret<-funct mid
                        if(ret==GT || ret==EQ)
                            then go (mid+1) r
                            else go l mid
    in go l (r+1)



