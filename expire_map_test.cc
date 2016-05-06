/*
 * expire_map_test.cc
 *
 * expire_map unit-tests.
 *
 *  Created on: Jul 27, 2015
 *      Author: Anshul Pundir
 */
#include "expire_map.h"

constexpr uint64_t ms = 1;
constexpr uint64_t sec = (ms * 1000);

/*!
 * \brief Test expire_map to do parallel puts, removes and overwrites.
 * Nothing deterministic here so just verify that the map is empty in the end.
 */
void concurrency_test() {
	printf("\nstarting concurrency test.\n");

	expire_map<uint64_t, uint64_t> *m = new expire_map<uint64_t, uint64_t>();

	thread t1( [m] () {
		for (int i = 1;i <= 25;i++) {
			m->put(i, i, i + sec * 2);
		}
	});

	//std::this_thread::sleep_for(std::chrono::milliseconds(2000));

	thread t2( [m] () {
		for (int i = 1;i <= 15;i++) {
			m->remove(i);
		}
	});

	//std::this_thread::sleep_for(std::chrono::milliseconds(2000));

	thread t3( [m] () {
		for (int i = 16;i <= 30;i++) {
			m->put(i, i, i + sec);
		}
	});

	t1.join();
	t2.join();
	t3.join();

	// Wait a certain amount more than the largest timeout.
	std::this_thread::sleep_for(std::chrono::milliseconds(4 * sec));
	assert(m->empty());

	printf("\ndone concurrency test.\n");

	delete m;
}

/*!
 * \brief Test expire_map a combination of supported ops.
 */
void simple_test() {
	printf("\nstarting simple test.\n");

	expire_map<uint64_t, uint64_t> *m = new expire_map<uint64_t, uint64_t>();

	/*
	 * Simple get/put:
	 */
	uint64_t k = 1;
	uint64_t v = 1234;
	uint64_t timeout = sec;
	uint64_t read_val = 0;

	// Query empty map.
	assert(!m->get(k, &read_val));

	// Insert single value.
	m->put(k, v, timeout);

	// Query value.
	bool ret = m->get(k, &read_val);
	assert(ret);
	assert(read_val == v);

	// Query after timeout.
	std::this_thread::sleep_for(std::chrono::milliseconds(timeout));
	assert(!m->get(k, &read_val));

	/*
	 * Overwrite:
	 */
	m->put(k, v, timeout);

	// Overwrite with shorter timeout.
	m->put(k, v, 100 * ms);

	// Verify entry removed after shorter timeout.
	std::this_thread::sleep_for(std::chrono::milliseconds(100 * ms));
	assert(!m->get(k, &read_val));

	/*
	 * Remove:
	 */
	m->put(k, v, timeout);
	m->remove(k);
	assert(!m->get(k, &read_val));

	/*
	 * Multiple inserts with different timeouts.
	 */
	m->put(1, 1, sec);
	m->put(2, 2, 100 * ms);
	m->put(3, 3, 2 * sec);

	std::this_thread::sleep_for(std::chrono::milliseconds(100 * ms));
	assert(!m->get(2, &read_val));
	std::this_thread::sleep_for(std::chrono::milliseconds(sec));
	assert(!m->get(1, &read_val));
	std::this_thread::sleep_for(std::chrono::milliseconds(2 * sec));
	assert(!m->get(3, &read_val));
	assert(m->empty());

	/*
	 * Multiple overwrites:
	 */
	m->put(1, 1, 100 * sec);
	m->put(1, 1, 50 * sec);
	m->put(1, 1, 25 * sec);
	m->put(1, 1, 12 * sec);
	m->put(1, 1, 6 * sec);
	m->put(1, 1, 3 * sec);
	m->put(1, 1, sec);

	std::this_thread::sleep_for(std::chrono::milliseconds(sec));
	assert(!m->get(1, &read_val));

	/*
	 * Multiple removes/overwrites.
	 */
	m->put(1, 1, 100 * sec);
	m->remove(1);
	m->put(1, 1, 25 * sec);
	m->put(1, 1, 12 * sec);
	m->remove(1);
	m->put(1, 1, 6 * sec);
	m->put(1, 1, 3 * sec);
	m->remove(1);
	m->put(1, 1, sec);
	m->remove(1);

	std::this_thread::sleep_for(std::chrono::milliseconds(sec));
	assert(!m->get(1, &read_val));

	delete m;
	printf("\ndone simple test.\n");
}

int main() {

	while (1) {
		char cc = 0;
		printf("\nexpire_map tester. Choose an option:\n "
				"1. Enter 1 to run tests \n "
				"2. Enter 2 for interactive mode\n ");
		cin >> cc;

		if (cc == '1') {
			simple_test();
			concurrency_test();

			printf("\nall tests passed\n");
		} else if (cc == '2') {

			expire_map<uint64_t, uint64_t> *m =
					new expire_map<uint64_t, uint64_t>();

			while(1) {
				char c = 0;
				printf("enter a command (p: put, g: get, r: remove, "
						"q: quit to main menu)");
				cin >> c;

				if (c == 'Q' || c == 'q') {
					printf("\nexiting interactive mode..\n");
					break;
				} else if (c == 'p' || c == 'P') {
					uint64_t key = 0;
					uint64_t value = 0;
					uint64_t to_ms = 0;
					printf("\nenter key (uint64_t), value (uint64_t), timeout (uint64_t) \n");
					scanf("%llu %llu %llu",
							&key,
							&value,
							&to_ms);
					m->put(key, value, to_ms);
				} else if (c == 'g' || c == 'G') {
					uint64_t key = 0;
					cout << "\nenter key (uint64_t) to get: ";
					cin>>key;

					uint64_t val = 0;
					bool exists = m->get(key, &val);

					if (exists)
						printf("\nValue for key %llu is %llu\n", key, val);
					else
						printf("\nValue for key does not exist.\n");
				} else if (c == 'r' || c == 'R') {
					uint64_t key = 0;
					cout << "\nenter key (uint64_t) to remove: ";
					cin>>key;
					m->remove(key);
				} else {
					printf("\ninvalid command.\n");
				}
			}

			delete m;
		} else if (cc == 'q' || cc == 'Q') {
			printf(" exiting...\n");
			break;
		} else {
			printf("\n invalid option.\n");
		}

	}
}
