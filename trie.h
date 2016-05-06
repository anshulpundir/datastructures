/*
 * trie.h
 *
 *  Created on: Aug 10, 2015
 *      Author: anshulp
 */

#ifndef TRIE_H_
#define TRIE_H_

#include <stdio.h>
#include <assert.h>

#include <vector>
#include <set>
#include <deque>
#include <unordered_map>
#include <algorithm>

#include <mutex>

using namespace std;

/*!
 * \class Trie.
 *
 * Uses an std::unordered_map at each level to
 * index the subtree for the next level.
 *
 * Provides fine-grained concurrency, where each node in the trie
 * can be locked and operated upon in parallel with other nodes.
 *
 * The key is a character array and value is templatized.
 *
 * **NOTE: For simplicity, assumes that a node is immutable and hence does
 * not support deletions for now.
 *
 */
template <class T>
class Trie {
public:
	Trie() {}

	~Trie() {delete root_;}

	/*!
	 * \brief Insert a given key-value pair.
	 * If the key already exists,
	 * its value is overwritten with the new value.
	 */
	void insert(char *key, T value) {
		insert(root_, key, value);
	}

	/*!
	 * \brief Search for an exact match
	 * for the given key.
	 */
	bool lookup(char *key, T *value) {
		return lookup(root_, key, value);
	}

	/*!
	 * \brief Search for values that prefix match
	 * the given key.
	 */
	void prefix_match(char *key, set<T> *values) {
		prefix_match(root_, key, values);
	}

private:

	/*!
	 * \class Trie node.
	 */
	struct Node {
		Node(char key) : key_(key) {}
		Node() {};
	    ~Node() {
	    	for_each(nodes_.begin(), nodes_.end(),
	    			[] (pair<char, Node*> p) { delete p.second; });
	    	nodes_.clear();
	    }

	    void lock() {
	    	lock_.lock();
	    }

	    void unlock() {
	    	lock_.unlock();
	    }

	    /*!
	     * \brief Node key.
	     */
	    char key_ = 0;

	    /*!
	     * \brief Node value.
	     */
	    T value_;

	    /*!
	     * \brief Index for the next level subtrees.
	     */
	    unordered_map<char, Node *> nodes_;

	    /*!
	     * \brief Indicates that a value exists for this node.
	     */
	    bool terminus_ = false;

	    /*!
	     * \brief Mutual exclusion for the node.
	     */
	    std::mutex lock_;
	};

	/*!
	 * \brief Insert a given key-value pair to the tree
	 * starting at the given root.
	 */
	void insert(Node *root, char *key, T value) {
		assert(root);

		unique_lock<mutex> lock(root->lock_);

		auto it = root->nodes_.find(key[0]);
		Node *n = nullptr;

		// Add the node for character at key[0] if it doesn't exist.
		if (it == root->nodes_.end()) {
			n = new Node(key[0]);
			root->nodes_[key[0]] = n;
		} else {
			n = it->second;
		}

		/* If this is the last character in the key,
		 * insert the value and mark it as terminus.
		 */
		if (key[1] == '\0') {
			n->value_ = value;
			n->terminus_ = true;

			return;
		}

		lock.unlock();

		insert(n, ++key, value);
	}

	/*!
	 * \brief Search for an exact match
	 * for the given key.
	 */
	bool lookup(Node *root, char *key, T *value) {

		unique_lock<mutex> lock(root->lock_);

		/* If this node is a terminus and
		 * if all the characters in the key
		 * have been exhausted.
		 */
		if (key[0] == '\0' && root->terminus_) {
			*value = root->value_;
			return true;
		} else if (key[0] == '\0')
			return false;

		// Search on the next level.
		auto it = root->nodes_.find(key[0]);

		if (it != root->nodes_.end()) {
			lock.unlock();
			return lookup(it->second, ++key, value);
		}

		return false;
	}

	/*!
	 * \brief Default number of values to get when prefix matching.
	 */
	const int prefix_match_max_values_default_ = 10;

	/*!
	 * brief Return the node for the given key.
	 */
	Node *node(Node *root, char *key) {
		if (key[0] == '\0') {
			return root;
		}

		unique_lock<mutex> lock(root->lock_);

		auto it = root->nodes_.find(key[0]);

		if (it != root->nodes_.end()) {
			lock.unlock();
			return node(it->second, ++key);
		}

		return nullptr;
	}

	/*!
	 * \brief Search for values that prefix match
	 * the given key starting at the given root.
	 *
	 * Does a BFS starting at the given root.
	 */
	void prefix_match(Node *root, char *key, set<T> *values) {
		Node *n = node(root, key);

		if (!n)
			return;

		deque<Node *> nodes;
		nodes.push_back(n);

		/*
		 * Iterate over the set of nodes and the children.
		 */
		while (!nodes.empty()) {

			n = nodes.front();
			nodes.pop_front();

			unique_lock<mutex> lock(n->lock_);

			if (n->terminus_) {
				values->insert(n->value_);

				if (values->size() == prefix_match_max_values_default_) {
					return;
				}
			}

			for_each(n->nodes_.begin(), n->nodes_.end(),
					[&nodes] (pair<char, Node *> cn) {
				nodes.push_back(cn.second);
			});
		}
	}

	Node *root_ = new Node();
};

#endif /* TRIE_H_ */
