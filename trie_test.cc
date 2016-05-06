/*
 * trie_test.cc
 *
 *  Created on: Aug 10, 2015
 *      Author: anshulp
 */
#include <stdio.h>

#include <thread>

#include "trie.h"

/*!
 * \brief Test trie to do parallel inserts.
 * Non-deterministic so nothing to test except
 * insert without panic.
 */
void concurrency_test() {
	printf("\nstarting concurrency test.\n");

	Trie<uint64_t> t;

	thread t1( [&t] () {
		string s = "ABCDEFGHIJKLMNO";

		for (int i = 1;i <= 25;i++) {
			random_shuffle(s.begin(), s.end());
			t.insert(&s[0], i);
		}

		// Insert some deterministic values
		t.insert("blah", 10000);
		t.insert("bloo", 10001);
	});

	thread t2( [&t] () {
		string s = "PQRSTUVWXYZ123";

		for (int i = 1;i <= 15;i++) {

			random_shuffle(s.begin(), s.end());
			t.insert(&s[0], i);
		}

		// Insert some deterministic values
		t.insert("avacado", 10003);
		t.insert("oranges", 10005);
	});

	thread t3( [&t] () {
		string s = "ABCDEFQRSTUS7980";

		for (int i = 16;i <= 30;i++) {
			random_shuffle(s.begin(), s.end());
			t.insert(&s[0], i);
		}

		// Insert some deterministic values
		t.insert("bloom", 10007);
		t.insert("filters", 10008);
	});

	t1.join();
	t2.join();
	t3.join();

	uint64_t val = 0;
	assert(t.lookup("avacado", &val) && val == 10003);
	assert(t.lookup("blah", &val) && val == 10000);
	assert(t.lookup("filters", &val) && val == 10008);

	printf("\ndone concurrency test.\n");
}

/*!
 * \brief Test the trie a combination of supported ops (inserts + overwrites).
 */
void simple_test() {
	printf("\nstarting simple test.\n");

	Trie<uint64_t> t;

	char key1[] = "foo";
	char key2[] = "faa";
	char key3[] = "fem";

	uint64_t v1 = 1111;
	uint64_t v2 = 2222;
	uint64_t v3 = 3333;

	t.insert(key1, v1);
	t.insert(key2, v2);
	t.insert(key3, v3);

	set<uint64_t> vals;
	char k[] = "f";
	t.prefix_match(k, &vals);

	assert(vals.size() == 3);
	(vals.find(v1) != vals.end());
	(vals.find(v2) != vals.end());
	(vals.find(v3) != vals.end());

	vals.clear();

	uint64_t v4 = 4444;
	uint64_t v5 = 5555;
	uint64_t v6 = 6666;

	t.insert(key1, v4);
	t.insert(key2, v5);
	t.insert(key3, v6);

	t.prefix_match("f", &vals);

	assert(vals.size() == 3);
	(vals.find(v4) != vals.end());
	(vals.find(v5) != vals.end());
	(vals.find(v6) != vals.end());

	printf("\ndone simple test.\n");
}

int main() {
	Trie<uint64_t> t;

	simple_test();
	concurrency_test();
}
