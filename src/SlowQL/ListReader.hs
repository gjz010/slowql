module SlowQL.ListReader where
    import Data.Word
    import qualified Data.ByteString as B
    -- applying list of functions to peek something from list [a] 
    -- Note that this is a foldr-styled expression that supports lazy evaluation
    -- ListReader=[a]->(b, [a])
    rzip :: [[a]->(b, [a])]->[b]->[a]->([b], [a])
    rzip [] bs as=(bs, as)
    rzip (f:fs) bs as=let (b, al)=f as
                          (bl, all)=rzip fs bs al
                      in (b:bl, all)
    
    readInt4 :: [a]->(Int, [a])
    readInt4 (a:b:c:d:xs)=(0, xs)
