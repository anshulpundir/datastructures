# datastructures

trie.h: A trie or a prefix tree with a character key and generic value.

expire_map.h
Allows adding of a <key, value> pair and the lookup/removal of a key
just like a regular map with an added feature of removal of an entry after a
timout, if not explicitly removed. The access to the map is thread-safe.
Insert, lookup and remove are O(1) operations.

The removal of timed-out entries happens in the background
on separate thread. This take O(log(n)) per entry.
