import Control.Exception
data MyException = ThisException | ThatException
    deriving Show
instance Exception MyException
p=do
    throw ThisException
    print "hahaha"