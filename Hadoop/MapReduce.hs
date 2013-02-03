
-- Hadoop Streaming MapReduce interface for Haskell
-- (c) 2010 Paul Butler <paulgb @ gmail>
-- 
-- You may use this module under the zlib/libpng license.
-- (http://www.opensource.org/licenses/zlib-license.php)
--
-- See README.md, LICENSE, and /examples for more information.
-- 
-- Happy MapReducing!


module Hadoop.MapReduce (Mapper, Reducer) where

import System (getArgs)
import Data.List (groupBy)

-- Mapper and Reducer correspond to the type signatures required
-- for the "map" and "reduce" functions of the MapReduce job.
-- Note that these do NOT correspond to the type signatures
-- of haskell's `map` and `reduce` functions.

type Mapper k v = String -> [(k, v)]
type Reducer ki vi ko vo = ki -> [vi] -> [(ko, vo)]

-- Interactor is a type signature of the function required
-- by the haskell `interact` function.
type Interactor = String -> String

-- Field separator. Hadoop uses the tab character ('\t')
-- by default, but comma (',') and space (' ') are also
-- common. Ideally there should be a way to change the
-- separator without modifying the source.
separator = '\t'

usage_message =
    "Notice: This is a streaming MapReduce program.\n" ++
    "It must be called with one of the following arguments:\n" ++
    "    -m     Run the Mapper portion of the MapReduce job\n" ++
    "    -r     Run the Reducer portion of the MapReduce job\n"

-- Convert key value pairs from the output of a Mapper to String values that are
-- expected output by the Hadoop streaming system.
convertMapOutput :: (Show k, Show v) => [(k, v)] -> [String]
convertMapOutput = map toKeyValue
    where toKeyValue kv = (show $ fst kv) ++ [separator] ++ (show $ snd kv)

-- Given a map function, `mapper` returns a function that
-- can be passed to haskell's `interact` function.
mapInteractor :: (Show k, Show v) => Mapper k v -> Interactor
mapInteractor mapper =
    unlines . (concatMap (convertMapOutput . mapper)) . lines


-- Convert a line of input from a Hadoop streaming process to the key and value
-- that are expected as input to a Reducer
convertReduceInput :: String -> (String, String)
convertReduceInput s =
    (takeWhile (/= separator) s, tail $ dropWhile (/= separator) s)

{-
reduceInteractor ::
    (Read ki, Read vi, Show ko, Show vo) => Reducer ki vi ko vo -> Interactor
reduceInteractor reducer input =
 -}




{-
-- For the reduce step, each line consists of a key and
-- (optionally) a value. The function `key` extracts
-- the key from a line.
key :: String -> String
key = takeWhile (/= separator)

-- Like `key`, `value` extracts the value from a line.
value :: String -> String
value = (drop 1) . dropWhile (/= separator)

-- Given two lines, `compareKeys` returns True if the lines
-- have the same key.
compareKeys :: String -> String -> Bool
compareKeys a b = (key a) == (key b)

-- Given a reduce function, `reducer` returns a function
-- that can be passed to haskell's `interact` function.
reducer :: Reduce -> Interactor
reducer mrReduce input =
    let groups = groupBy compareKeys $ lines input :: [[String]] in
    unlines $ concatMap
        (\g -> mrReduce (key $ head g) (map value g :: [String])) groups

-- Main function for a map reduce program
mrMain :: Map -> Reduce -> IO () 
mrMain mrMap mrReduce = do
    args <- getArgs
    case args of
        ["-m"] -> interact (mapper mrMap)
        ["-r"] -> interact (reducer mrReduce)
        _ -> putStrLn usage_message

-- Main function for a map-only job
mapMain :: Map -> IO ()
mapMain mrMap = do
    args <- getArgs
    case args of
        ["-m"] -> interact (mapper mrMap)
        ["-r"] -> interact id
        _ -> putStrLn usage_message

-- Main function for a reduce-only job
reduceMain :: Reduce -> IO ()
reduceMain mrReduce = do
    args <- getArgs
    case args of
        ["-m"] -> interact id
        ["-r"] -> interact (reducer mrReduce)
        _ -> putStrLn usage_message
-}
