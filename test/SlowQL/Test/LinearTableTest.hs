{-# LANGUAGE OverloadedStrings #-}
module SlowQL.Test.LinearTableTest where
    import qualified SlowQL.PageFS as FS
    import qualified SlowQL.Data.LinearTable as T
    import qualified Data.ByteString as BS
    import Conduit
    import qualified SlowQL.Conduit.Combinator as CB
    type ByteString=BS.ByteString
    filename="test-lineartable.tmp"
    test :: IO()
    test=do
        putStrLn "Testing SlowQL.Data.LinearTable"
        T.initialize filename 11
        table<-T.open filename
        print table
        T.printStats table
        T.insert "YajueSenpai" table T.unitaryId
        T.insert "11451481893" table T.unitaryId
        T.insert "DeepFantasy" table T.unitaryId
        T.printStats table
        let take3=(T.enumerate table).| takeC 3
        l<-runConduit $ (CB.cartesian take3 take3 ) .| filterC (\(a,b)->a/=b) .|sinkList
        print l
        T.close table

        -- mapM_  (const $ T.insert "YajueSenpai" table) [1..1000000]
        --T.update (\x->return (if x== "yajue" then Nothing else (Just "wslnm"))) table
        --T.insert "hahah" table
        --T.insert "yajuu" table