
-- Hadoop Streaming MapReduce interface for Haskell
-- (c) 2010 Paul Butler <paulgb @ gmail>
-- 
-- You may use this module under the zlib/libpng license.
-- (http://www.opensource.org/licenses/zlib-license.php)
--
-- See README.md, LICENSE, and /examples for more information.
-- 
-- Happy MapReducing!


module Hadoop.MapReduce (mrMain, Mapper, Reducer) where

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

-- Convert key value pairs from the output of a Mapper or a Reducer
-- to String values that are expected output by the Hadoop streaming system.
convertOutput :: (Show k, Show v) => [(k, v)] -> [String]
convertOutput = map toKeyValue
    where toKeyValue kv = (show $ fst kv) ++ [separator] ++ (show $ snd kv)

-- Given a map function, `mapper` returns a function that
-- can be passed to haskell's `interact` function.
mapInteractor :: (Show k, Show v) => Mapper k v -> Interactor
mapInteractor mapper =
    unlines . (concatMap (convertOutput . mapper)) . lines


-- For the reduce step, each line consists of a key and
-- (optionally) a value. The function `key` extracts
-- the key from a line.
key :: (Read k) => String -> k
key = read . takeWhile (/= separator)

-- Like `key`, `value` extracts the value from a line.
value :: (Read v) => String -> v
value = read . (drop 1) . dropWhile (/= separator)

-- Convert a line of input from a Hadoop streaming process to the key and value
-- that are expected as input to a Reducer
convertReduceInput :: (Read k, Read v) => String -> (k, v)
convertReduceInput s = (key s, value s)

compareKeys :: (Eq k) => (k, v) -> (k, v) -> Bool
compareKeys a b = (fst a) == (fst b)

reduceInteractor ::
    (Read ki, Read vi, Eq ki, Show ko, Show vo) =>
    Reducer ki vi ko vo -> Interactor
reduceInteractor reducer input =
    let keyValues = ((map convertReduceInput) . lines) input in
    let groups = groupBy compareKeys keyValues in
    let gathered = map (\g -> ((fst . head) g, map snd g)) groups in
    let reducedKeyValues = concatMap (\g -> reducer (fst g) (snd g)) gathered in
    unlines $ convertOutput reducedKeyValues


-- Main function for a map reduce program
mrMain :: (Show k, Read k, Eq k,
           Show v, Read v,
           Show ko, Show vo) =>
    Mapper k v -> Reducer k v ko vo -> IO ()
mrMain mapper reducer = do
    args <- getArgs
    case args of
        ["-m"] -> interact (mapInteractor mapper)
        ["-r"] -> interact (reduceInteractor reducer)
        _ -> putStrLn usage_message

{-
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
