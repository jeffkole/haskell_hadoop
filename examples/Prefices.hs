

module Main where

import Hadoop.MapReduce (mrMain, Mapper, Reducer)
import Data.List (inits, sort, nub)

pMap :: Mapper String String
pMap line = [(prefix, line) | prefix <- tail $ inits line]
pMap line = [prx ++ " " ++ line | prx <- tail $ inits line]

pReduce :: Reduce String String String String
pReduce prx wrds = [(prx, unwords $ sort $ nub wrds)]

main = mrMain pMap pReduce

