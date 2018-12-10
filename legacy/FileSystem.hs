module SlowQL.FileSystem where
    import qualified Foreign.Marshal.Alloc as FAlloc
    import qualified Foreign.Marshal.Array as FArray
    import qualified Foreign.Storable as FStore
    import Foreign.Ptr
    import Data.Word
    import Data.IORef
    data Page=Page {raw :: !Ptr Word8, rdirty :: !IORef Bool}
    

    withPage :: (Ptr Word8->IO Bool)->Page->IO ()
    withPage f page=do
        modified<-f $ raw page
        modifyIORef' (rdirty page) (||modified)
    dirty :: Page->IO Bool
    dirty=readIORef . rdirty
    