
-- Count the frequency of words in a text document. Words
-- are any non-space characters separated by spaces.
-- Outputs, with one line per word, each word followed by
-- the number of times it occurs.

module Main where

import Hadoop.MapReduce (mrMain, Mapper, Reducer)

wfMap :: (Num t) => Mapper String t
wfMap input = map (\w -> (w, 1)) (words input)

wfReduce :: (Num t) => Reducer String t String t
wfReduce key values = [(key, sum values)]

main = mrMain wfMap wfReduce
