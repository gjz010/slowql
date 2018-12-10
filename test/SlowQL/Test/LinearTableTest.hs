{-# LANGUAGE OverloadedStrings #-}
module SlowQL.Test.LinearTableTest where
    import qualified SlowQL.PageFS as FS
    import qualified SlowQL.Data.LinearTable as T
    import qualified Data.ByteString as BS
    import Conduit
    type ByteString=BS.ByteString
    filename="test-lineartable.tmp"
    test :: IO()
    test=do
        putStrLn "Testing SlowQL.Data.LinearTable"
        T.initialize filename 11
        table<-T.open filename
        print table
        T.printStats table

        mapM_  (const $ T.insert "YajueSenpai" table) [1..1000000]
        --T.update (\x->return (if x== "yajue" then Nothing else (Just "wslnm"))) table
        --T.insert "hahah" table
        --T.insert "yajuu" table
        T.printStats table
        runConduit $ (T.enumerate table).| takeC 100 .| printC
        T.close table