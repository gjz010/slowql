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
        table <- SlowQL.PageFS.openDataFile "table.tbl"
        let arr1=B.pack [50|a<-[1..8192]]
        SlowQL.PageFS.writePage table 0 arr1
        let arr2=B.pack [52|a<-[1..8192]]
        size<-SlowQL.PageFS.getFileSize table
        print size
        SlowQL.PageFS.writePage table 11 arr2
        size<-SlowQL.PageFS.getFileSize table
        print size
        SlowQL.PageFS.writeBackAll table
        size<-SlowQL.PageFS.getFileSize table
        print size
        page<-SlowQL.PageFS.readPage table 100
        size<-SlowQL.PageFS.getFileSize table
        print size
        print $ B.length page
        SlowQL.PageFS.closeDataFile table
        putStrLn "Done"