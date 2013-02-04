
-- Alternate word frequency example. Functionally identical
-- to WordFrequency.hs, but can be used as a combiner.

module Main where

import Hadoop.MapReduce (mrMain, Mapper, Reducer)

wfMap :: Mapper String Int
wfMap record = [(word, 1) | word <- words record]

wfReduce :: Reducer String Int String Int
wfReduce key values = [(key, sum values)]

main = mrMain wfMap wfReduce
