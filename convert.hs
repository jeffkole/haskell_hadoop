{-# LANGUAGE TypeSynonymInstances,
             FlexibleInstances,
             UndecidableInstances,
             OverlappingInstances #-}

class (Read a) => HRead a where
    hread :: String -> a
    hread = read

-- Essentially redefines `Read` for a String to be the identity function
-- Thank you StackOverflow: http://stackoverflow.com/a/3740984
instance HRead String where
    hread = id

instance (Read a) => HRead a

class (Show a) => HShow a where
    hshow :: a -> String
    hshow = show

-- Essentially redefines `Show` for a String to be the identity function
-- Thank you StackOverflow: http://stackoverflow.com/a/3740984
instance HShow String where
    hshow = id

instance (Show a) => HShow a

convertInput :: (HRead k, HRead v) => String -> (k, v)
convertInput s =
    (hread $ takeWhile (/= '\t') s,
     hread $ tail $ dropWhile (/= '\t') s)

convertOutput :: (HShow k, HShow v) => (k, v) -> String
convertOutput (k, v) = "Key: " ++ (hshow k) ++ ", Value: " ++ (hshow v)

intIn = convertInput :: String -> (Int, String)
intOut = convertOutput :: (Int, String) -> String
-- intOut (k, v) = "Key: " ++ (hshow k) ++ ", Value: " ++ (hshow v) ++ ", Total: " ++ (hshow (k+v))

main = do
    interact ((unlines . (map intOut)) . ((map intIn) . lines))
