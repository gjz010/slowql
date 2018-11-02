module Main where
    import Control.Monad.State
    import qualified SlowQL.PageFS
    import qualified Data.ByteString as B
    main :: IO()
    main = do 
        putStrLn "ABC"
        putStrLn "DEF"
        -- table <- SlowQL.PageFS.openTableFile "table.tbl"
        test_pagefs
        return ()

    test_pagefs :: IO()
    test_pagefs = do
        table <- SlowQL.PageFS.openTableFile "table.tbl"
        let arr1=B.pack [48|a<-[1..8192]]
        SlowQL.PageFS.writePage table 0 arr1
        let arr2=B.pack [49|a<-[1..8192]]
        SlowQL.PageFS.writePage table 10 arr2
        SlowQL.PageFS.writeBackAll table
        SlowQL.PageFS.closeTableFile table
        putStrLn "Done"