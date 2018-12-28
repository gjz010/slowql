{-# LANGUAGE OverloadedStrings #-}
module SlowQL.Test.BTreeTableTest where
    import qualified SlowQL.PageFS as FS
    import qualified SlowQL.Data.LinearTable as T
    import qualified SlowQL.Data.BTreeTable as BT
    import qualified Data.ByteString as BS
    import qualified Data.ByteString.Char8 as BC
    import Conduit
    import qualified SlowQL.Conduit.Combinator as CB
    test :: IO()
    test=do
        putStrLn "Test SlowQL.Test.BTreeTableTest"
        putStrLn "Creating BTreeTable"
        BT.initialize "test-btree.tmp" 10 5
        bt<-BT.open "test-btree.tmp"
        BT.printBT bt
        let fext=BS.take 5
        let fcmp=compare
        let hint=(fext, fcmp)
        BT.insert bt hint "YajueSenpi"
        BT.insert bt hint "YajueYajue"
        BT.insert bt hint "TadnoDogod"
        mapM_ (BT.insert bt hint) $ map (BC.pack .(flip (++) "YAJUU"). show) [10001..15000]
        mapM_ (BT.insert bt hint) $ map (BC.pack .(flip (++) "YAJUU"). show) [16001..20000]
        BT.printBT bt
        BT.close bt
        bt<-BT.open "test-btree.tmp"
        let names2=map (BC.pack .(flip (++) "SENPI"). show) [15001..16000]
        mapM_ (BT.insert bt hint) names2
        --ret<-runConduit $ BT.enumerateRange bt hint (Just ("15000",True)) (Just ("16000",False)) .| sinkList
        --print ret
        ret<-runConduit $ BT.renumerateRange bt hint (Just ("15000",True)) (Just ("16000",False)) .| sinkList
        print ret
        BT.printBT bt
        
        BT.close bt