
-- Takes a directed graph and reverses every edge in it.
-- The directed graph is expected to be described as follows.
-- Each line corresponds to one node in the graph. The line
-- begins with the name of the node followed by a space, and
-- is followed by the names of adjacent nodes, separated by
-- spaces. The output graph is in the same format.

module Main where

import Hadoop.MapReduce (mapMain, Mapper, Reducer)

-- input line is [origin, destA, destB, destC,...]
-- emit [(destA, origin), (destB, origin), (destC, origin),...]
wfMap :: Mapper String String
wfMap line =
    let nodes = words line in
    let origin = head nodes in
    do
        adjacentNode <- tail nodes
        return $ (adjacentNode, origin)

{-
wfReduce :: Reducer String String String String
wfReduce key values =
    return $ key ++ " " ++ (unwords values)
    -}

main = mapMain wfMap -- wfReduce

