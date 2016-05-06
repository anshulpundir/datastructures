/*
 * expire_map.h
 *
 *  Created on: Jul 27, 2015
 *      Author: Anshul Pundir
 */
#include <stdio.h>
#include <iostream>
#include <assert.h>

// STL
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>

// Synch. headers
#include <mutex>
#include <condition_variable>
#include <thread>

// Misc
#include <chrono>

using namespace std;
using namespace std::chrono;

#define DEBUG 0

/*!
 * \brief Get current time in millis.
 */
uint64_t get_time_ms() {
	system_clock::time_point now = system_clock::now();
	auto duration = now.time_since_epoch();
	return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

/*!
 * \class Expire Map class.
 *
 * Allows adding of a <key, value> pair and the lookup/removal of a key
 * just like a regular map with an added feature of removal of an entry after a
 * timout, if not explicitly removed. The access to the map is thread-safe.
 * Insert, lookup and remove are O(1) (near constant time,
 * see README for gotchas) operations.
 *
 * The removal of timed-out entries happens in the background
 * on separate thread. This take O(log(n)) per entry.
 */
template <typename K, typename V>
class expire_map {
public:
	expire_map() :
		reclaimer_(thread([this] { this->reclaim(); } )),
		append_timeouts_(new vector<timeout_data>()) {}

	~expire_map() {
		cleanup();
		reclaimer_.join();

		delete append_timeouts_;
		append_timeouts_ = nullptr;
	}

	/*!
	 * brief Gets the value associated with the passed in key.
	 * Returns true if the key is present in the map,
	 * with the value copied into val.
	 * False otherwise.
	 */
	bool get(K key, V *val) {
		unique_lock<mutex> lock(lock_);

		auto it = lookup_map_.find(key);

		if (it == lookup_map_.end()) {
			return false;
		}
		else {
			// Return not found if timed-out.
			if (it->second.timeout_ <= get_time_ms()) {
				return false;
			} else {
				*val = it->second.value_;
				return true;
			}
		}
	}

	/*!
	 * \brief Adds the passed in key and value to the map.
	 * The added key is expired from the map after time equal to
	 * the passed-in timeout has expired and is essentially not
	 * available after that point.
	 * If the key already exists, its overwritten with the new value/timeout.
	 */
	void put(K key, V val, uint64_t ms) {

		unique_lock<mutex> lock(lock_);

		// Calculate timeout in ms.
		auto epoch_ms = get_time_ms() + ms;

		// Remove the current entry for this key from the map, if one exists.
		auto it = lookup_map_.find(key);
		if (it != lookup_map_.end()) {
			remove_internal(key);
		}

		/* Insert timeout in append list.
		 * The last param is set to false to indicate that this is a
		 * regular timeout entry.
		 */
		append_timeouts_->push_back(timeout_data(key, epoch_ms, false));

		// Insert K, V for lookup.
		lookup_map_.insert(std::pair<uint64_t, map_data>(key,
				map_data(val, epoch_ms)));

		if (sorted_timeouts_.empty() || epoch_ms <= min_timeout()) {
			cv_.notify_all();
		}
	}

	/*!
	 * \brief Remove the entry for the given key from the map,
	 * if present.
	 */
	void remove(K key) {
		unique_lock<mutex> lock(lock_);
		remove_internal(key);
	}

	/*!
	 * \brief Return the size in terms of the number of entries.
	 */
	size_t size() {
		unique_lock<mutex> lock(lock_);
		return lookup_map_.size();
	}

	bool empty() {
		return size() == 0;
	}

private:
	/*!
	 * \brief Max number of eligible entries to be reclaimed in one go.
	 * Bounds the reclaimer thread lock contention.
	 */
	const int MAX_RECLAIM_ENTRIES = 10;

	/*!
	 * \class Value type for the lookup-map.
	 * Stores the value and the timeout for the key it corresponds to.
	 */
	struct map_data {
		map_data(V value, uint64_t timeout) :
			value_(value), timeout_(timeout) {}

		V value_;
		uint64_t timeout_ = 0;
	};

	/*!
	 * \class Data structure to store the key and its corresponding
	 * timeout in the append_timeout_ list.
	 * Also stores a bool set to true to indicate whether this is entry
	 * is an overwrite of a previously created timeout (in which case
	 * the original entry's flag value would be false).
	 */
	struct timeout_data {
		timeout_data(K key, uint64_t timeout, bool remove) :
			key_(key), timeout_(timeout), overwrite_(remove) {}

		K key_;
		uint64_t timeout_ = 0;

		/*!
		 * brief Flag to indicate whether this is entry
		 * is an overwrite of a previously created timeout entry.
		 * True if its an overwrite. False otherwise.
		 *
		 * If the reclaimation logic encounters an entry with the overwrite
		 * set to true, it verifyeis that there is a corresponding timeout entry
		 * with the same key already in the sorted_timeouts_ map
		 */
		bool overwrite_ = false;
	};

	/*!
	 * \brief Append only list of timeouts.
	 * Timeouts for new key/value pairs being inserted into
	 * the map go here.
	 */
	vector<timeout_data> *append_timeouts_ = nullptr;

	/*!
	 * \brief Keys indexed and sorted by timeouts
	 * ((k, v) => (timeout, unordered_set<key>)).
	 *
	 * The value is an unordered_set of keys, each of which,
	 * potentially has the same timeout value (i.e. millisecond
	 * granularity not good enough).
	 */
	map<uint64_t, unordered_set<K>> sorted_timeouts_;

	/*!
	 * \brief Values+timeouts indexed by keys used to
	 * service gets.
	 */
	unordered_map<K, map_data> lookup_map_;

	/*!
	 * \brief Reclaimer thread.
	 */
	thread reclaimer_;

	/*!
	 * \brief CV to synchronize the reclaimer.
	 */
	std::condition_variable cv_;

	/*!
	 * \brief Global lock.
	 */
	std::mutex lock_;

	/*!
	 * \brief Shutdown flag.
	 * Indicates shutdown to the reclaimer thread.
	 */
	bool shutdown_ = false;

	/*!
	 * brief Returns the lowest timeout value from the map.
	 * Make sure the map is not empty before calling this function.
	 */
	uint64_t min_timeout() const {
		assert(!sorted_timeouts_.empty());

		return sorted_timeouts_.begin()->first;
	}

	/*!
	 * brief Removes the given key from the map, if present.
	 */
	void remove_internal(K key) {
		auto it = lookup_map_.find(key);

		if (it != lookup_map_.end()) {

			/* Insert a removal entry to remove the original entry
			 * from sorted_timeouts_.
			 */
			append_timeouts_->push_back(timeout_data(key,
					it->second.timeout_, true));

			// Remove entry from the lookup map.
			lookup_map_.erase(it);
		}
	}

	/*!
	 * \brief Populates the append-only list of timeouts sorted by timeouts
	 * into sorted_timeouts_.
	 *
	 * If the timeout is an overwrite, then
	 * the matching original timeout is found and removed and
	 * the overwrite is discarded.
	 * If the timeout is not an overwrite, then a new timeout is inserted
	 * into the sorted_timeouts_ map to be processed by the reclaimer.
	 */
	void populate_sorted_timeouts() {

		for_each(append_timeouts_->begin(),
				append_timeouts_->end(),
				[this] (timeout_data data) {

				auto it = sorted_timeouts_.find(data.timeout_);

				if (it != sorted_timeouts_.end()) {

					/* If the second timeout entry entry is an overwrite,
					 * there should be an entry for the original timeout.
					 */
					if (data.overwrite_) {
						auto it2 = it->second.find(data.key_);
						assert(it2 != it->second.end());

						// Remove the original timeout entry.
						it->second.erase(data.key_);

						if (it->second.empty())
							sorted_timeouts_.erase(it);
					} else {
						auto ret = it->second.insert(data.key_);

						// verify that the element was inserted.
						assert(ret.second);
					}

				} else {
					sorted_timeouts_.insert(
							std::pair<uint64_t, unordered_set<K>>(data.timeout_,
									unordered_set<K> ({ data.key_ })));
				}
		});

		append_timeouts_->clear();
	}

	/*!
	 * \brief Reclaim routine.
	 * Copies the append-only list of timeouts into timeouts_,
	 * sorted by the timeout. Then, scans the lowest timeout first
	 * to identify eligible entries.
	 *
	 * Tries to reclaim MAX_RECLAIM_ENTRIES at a time before relinquishing
	 * the lock.
	 *
	 * Sleeps if no eligible entries are found for reclaim:
	 * 	1. If an entry is found for reclaim, but it timeout is into the future,
	 * 		then sleeps for the amount of time required to get to the timeout.
	 * 	2. If no entries are found, then sleeps awaiting insertion into the map.
	 *
	 * Woken up if an entry is added with a timeout lower than the
	 * current min timout.
	 */
	void reclaim() {

		while (1) {

			unique_lock<mutex> lock(lock_);

			// Sort out the timeouts.
			populate_sorted_timeouts();

			// Wait if sorted_timeouts_ is empty.
			if (sorted_timeouts_.empty()) {

				// If shutting down, return and exit.
				if (shutdown_)
					return;
#if DEBUG
				printf("\nNo more entries. Reclaimer sleeping ...\n");
#endif
				cv_.wait(lock);
				continue;
			}

			auto curr_ms = get_time_ms();
			auto min_to = min_timeout();

			/* Wait the amount of time it takes to reach
			 * the minimum timeout.
			 */
			if (min_to > curr_ms) {
				std::chrono::system_clock::time_point now1 =
						std::chrono::system_clock::now();

				auto diff = min_to - curr_ms;

#if DEBUG
				printf("\nno values eligible for reclaim. sleeping "
						"for %llu ms at: %llu\n", diff, curr_ms);
#endif

				cv_.wait_for(lock, std::chrono::milliseconds(diff));
				continue;
			}

			// Try to reclaim MAX_RECLAIM_ENTRIES timed-out entries.
			int num_reclaimed = 0;
			auto it = sorted_timeouts_.begin();
			while (it != sorted_timeouts_.end() &&
					num_reclaimed < MAX_RECLAIM_ENTRIES) {
				if (it->first <= curr_ms) {

					for_each(it->second.begin(), it->second.end(),
							[this] (K key) {
								int result = lookup_map_.erase(key);
								assert(result == 1);
					});

					sorted_timeouts_.erase(it++);
				} else
					break;

				++num_reclaimed;
			}
		}
#if DEBUG
		printf("\nreclaimer exiting.\n");
#endif
	}

	/*!
	 * \brief Sets the shutdown flag which causes
	 * the reclaimer thread to exit.
	 */
	void cleanup() {
		shutdown_ = true;

		unique_lock<mutex> lock(lock_);
		cv_.notify_all();
	}
};
