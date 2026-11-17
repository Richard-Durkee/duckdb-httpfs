#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

#include <chrono>

#ifndef __EMSCRIPTEN__
#include <curl/curl.h>
#endif

namespace duckdb {
class Logger;

struct HTTPMinTCPMSS {
	//! Throws an InvalidInputException unless 'http_util_name' is the curl client, which alone applies
	//! http_min_tcp_mss, and throws a NotImplementedException on Windows
	static void CheckClient(const string &http_util_name);
};

#ifndef __EMSCRIPTEN__
//! Makes a curl handle skip server addresses whose negotiated TCP maximum segment size (MSS) is below a minimum.
//!
//! curl calls the socket options callback for a new socket before it connects, so the callback connects the socket
//! itself, reads the MSS the two ends agreed on, and has curl go on with the next address when it is too small. This
//! replaces curl's own connect for new connections: it does not race IPv6 against IPv4 and the connect cannot be
//! interrupted. Connections curl reuses are not checked, and with a proxy the address is the proxy's.
//!
//! Only the connections of a transfer's first request are checked. A request that follows a redirect connects to
//! the first address that works: the earlier request was already sent, so the transfer cannot be started over when
//! every address of the new host is skipped.
class CURLSocketFilter {
public:
	static constexpr idx_t NO_MINIMUM = 0;

	//! Installs the callbacks on 'curl' the first time and sets the minimum. Throws on Windows unless it is 0.
	void Configure(CURL *curl_p, idx_t min_mss, idx_t connect_timeout_seconds, shared_ptr<Logger> logger_p);
	//! Runs the transfer. If every address of the first request was skipped nothing was sent, so it runs the transfer
	//! once more and keeps the first address that connects. Skipped and kept addresses are logged afterwards.
	CURLcode Perform();

private:
	struct Event {
		string type;
		string host;
		string payload;
	};
	friend struct CURLSocketCallbacks;

	CURL *curl = nullptr;
	idx_t min_mss = NO_MINIMUM;
	bool installed = false;
	int connect_timeout_ms = 0;
	//! When the connects of the current request must be done, as curl's connect timeout counts all of its addresses
	std::chrono::steady_clock::time_point connect_deadline;
	//! The raw bytes of the address curl is about to connect to, kept by the open callback for the options callback
	string address;
	//! The addresses skipped and whether one was kept since the transfer started
	idx_t skipped = 0;
	bool kept = false;
	//! Keep the first address that connects, whatever its MSS
	bool keep_any = false;
	vector<Event> events;
	shared_ptr<Logger> logger;
};
#endif

} // namespace duckdb
