#include "http/http_tcp_mss.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "http/httpfs_client.hpp"

#ifndef __EMSCRIPTEN__
#ifndef _WIN32
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#endif
#endif

namespace duckdb {

void HTTPMinTCPMSS::CheckClient(const string &http_util_name) {
	if (http_util_name != "HTTPFS-Curl") {
		throw InvalidInputException("http_min_tcp_mss is only applied by the curl HTTP client, but the current client "
		                            "is '%s'; set httpfs_client_implementation = 'curl' or set http_min_tcp_mss to 0",
		                            http_util_name);
	}
#ifdef _WIN32
	throw NotImplementedException("http_min_tcp_mss is not supported on Windows");
#endif
}

#ifndef __EMSCRIPTEN__

//! A connect that is given more time than this is not worth waiting for, and keeps the milliseconds in an int
static constexpr idx_t MAX_CONNECT_TIMEOUT_SECONDS = 3600;
//! What curl uses when its connect timeout is 0
static constexpr idx_t DEFAULT_CONNECT_TIMEOUT_SECONDS = 300;

#ifndef _WIN32

//! The callbacks curl calls, which reach the filter's state
struct CURLSocketCallbacks {
	static void Record(CURLSocketFilter &filter, const char *type, const string &host, const string &payload) {
		if (!filter.logger) {
			return;
		}
		// the callbacks are called from C, so an allocation failure must not escape them
		try {
			filter.events.push_back({type, host, payload});
		} catch (...) { // NOLINT(bugprone-empty-catch): dropping a log line is better than failing the connection
		}
	}

	static string AddressToString(const string &address) {
		char text[INET6_ADDRSTRLEN] = {};
		auto family = reinterpret_cast<const sockaddr *>(address.data())->sa_family;
		const void *source = nullptr;
		if (family == AF_INET) {
			source = &reinterpret_cast<const sockaddr_in *>(address.data())->sin_addr;
		} else if (family == AF_INET6) {
			source = &reinterpret_cast<const sockaddr_in6 *>(address.data())->sin6_addr;
		}
		if (!source || !inet_ntop(family, source, text, sizeof(text))) {
			return "unknown";
		}
		return text;
	}

	//! Connects the socket to the saved address within the connect timeout, whether or not curl has made it
	//! non-blocking yet. Leaves errno as it was when the connect succeeds.
	static bool Connect(const CURLSocketFilter &filter, curl_socket_t fd) {
		const int saved_errno = errno;
		const int flags = fcntl(fd, F_GETFL, 0);
		if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
			return false;
		}
		bool connected = connect(fd, reinterpret_cast<const sockaddr *>(filter.address.data()),
		                         NumericCast<socklen_t>(filter.address.size())) == 0;
		// an interrupted connect carries on in the background, like one that is still in progress
		if (!connected && (errno == EINPROGRESS || errno == EINTR)) {
			pollfd wait_for_connect {fd, POLLOUT, 0};
			int ready;
			do {
				const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
				    filter.connect_deadline - std::chrono::steady_clock::now());
				ready = poll(&wait_for_connect, 1, remaining.count() > 0 ? NumericCast<int>(remaining.count()) : 0);
			} while (ready < 0 && errno == EINTR);
			int error = 0;
			socklen_t length = sizeof(error);
			connected = ready > 0 && getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length) == 0 && error == 0;
		}
		fcntl(fd, F_SETFL, flags);
		if (connected) {
			errno = saved_errno;
		}
		return connected;
	}

	static curl_socket_t Open(void *data, curlsocktype purpose, curl_sockaddr *address) {
		auto &filter = *static_cast<CURLSocketFilter *>(data);
		filter.address.clear();
		if (purpose == CURLSOCKTYPE_IPCXN) {
			try {
				filter.address.assign(reinterpret_cast<const char *>(&address->addr), address->addrlen);
			} catch (...) { // NOLINT(bugprone-empty-catch): without the address the options callback keeps the socket
			}
		}
		const auto fd = socket(address->family, address->socktype, address->protocol);
		if (fd != CURL_SOCKET_BAD) {
			// like curl's own sockets, a socket opened here must not leak into child processes
			fcntl(fd, F_SETFD, FD_CLOEXEC);
		}
		return fd;
	}

	//! Called by curl for every new socket, after Open and before it connects
	static int Options(void *data, curl_socket_t fd, curlsocktype purpose) {
		auto &filter = *static_cast<CURLSocketFilter *>(data);
		if (purpose != CURLSOCKTYPE_IPCXN || filter.min_mss == CURLSocketFilter::NO_MINIMUM || filter.address.empty()) {
			return CURL_SOCKOPT_OK;
		}
		if (!Connect(filter, fd)) {
			return CURL_SOCKOPT_ERROR;
		}
		int mss = 0;
		socklen_t length = sizeof(mss);
		// a segment size that cannot be read does not make the address worse
		const bool too_small = getsockopt(fd, IPPROTO_TCP, TCP_MAXSEG, &mss, &length) == 0 && mss > 0 &&
		                       NumericCast<idx_t>(mss) < filter.min_mss;
		const auto payload = "mss=" + std::to_string(mss) + " min=" + std::to_string(filter.min_mss);
		// a request after a redirect cannot be started over, so it takes the first address that connects
		long redirects = 0; // NOLINT(google-runtime-int): required by curl_easy_getinfo
		curl_easy_getinfo(filter.curl, CURLINFO_REDIRECT_COUNT, &redirects);
		if (too_small && !filter.keep_any && redirects == 0) {
			filter.skipped++;
			Record(filter, "min_tcp_mss_skipped", AddressToString(filter.address), payload);
			return CURL_SOCKOPT_ERROR;
		}
		if (too_small) {
			Record(filter, "min_tcp_mss_kept_below_minimum", AddressToString(filter.address), payload);
		}
		filter.kept = true;
		// the next connection of the transfer, for a redirect, gets a connect timeout of its own
		filter.connect_deadline =
		    std::chrono::steady_clock::now() + std::chrono::milliseconds(filter.connect_timeout_ms);
		return CURL_SOCKOPT_ALREADY_CONNECTED;
	}
};

#endif

void CURLSocketFilter::Configure(CURL *curl_p, idx_t min_mss_p, idx_t connect_timeout_seconds,
                                 shared_ptr<Logger> logger_p) {
#ifdef _WIN32
	if (min_mss_p != NO_MINIMUM) {
		throw NotImplementedException("http_min_tcp_mss is not supported on Windows");
	}
#else
	curl = curl_p;
	if (!installed && min_mss_p != NO_MINIMUM) {
		// the callbacks stay installed, and do nothing while the minimum is 0
		auto result = curl_easy_setopt(curl, CURLOPT_OPENSOCKETFUNCTION, CURLSocketCallbacks::Open);
		if (result == CURLE_OK) {
			result = curl_easy_setopt(curl, CURLOPT_OPENSOCKETDATA, this);
		}
		if (result == CURLE_OK) {
			result = curl_easy_setopt(curl, CURLOPT_SOCKOPTFUNCTION, CURLSocketCallbacks::Options);
		}
		if (result == CURLE_OK) {
			result = curl_easy_setopt(curl, CURLOPT_SOCKOPTDATA, this);
		}
		if (result != CURLE_OK) {
			throw IOException("Failed to set the curl socket callbacks: %s", curl_easy_strerror(result));
		}
		installed = true;
	}
	min_mss = min_mss_p;
	// like curl's own connect timeout, 0 stands for its default
	const auto seconds = connect_timeout_seconds == 0 ? DEFAULT_CONNECT_TIMEOUT_SECONDS : connect_timeout_seconds;
	connect_timeout_ms = NumericCast<int>(MinValue<idx_t>(seconds, MAX_CONNECT_TIMEOUT_SECONDS) * 1000);
	logger = std::move(logger_p);
#endif
}

CURLcode CURLSocketFilter::Perform() {
	if (min_mss == NO_MINIMUM) {
		return curl_easy_perform(curl);
	}
	skipped = 0;
	kept = false;
	keep_any = false;
	events.clear();
	connect_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(connect_timeout_ms);
	auto result = curl_easy_perform(curl);
	if (result != CURLE_OK && skipped > 0 && !kept) {
		// Every address was skipped before anything was sent. A small segment size is better than no connection, so
		// go on with the first address that connects.
		keep_any = true;
		connect_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(connect_timeout_ms);
		result = curl_easy_perform(curl);
		keep_any = false;
	}
	if (logger && !events.empty() && logger->ShouldLog(HTTPFSInfoLogType::NAME, HTTPFSInfoLogType::LEVEL)) {
		for (auto &event : events) {
			logger->WriteLog(HTTPFSInfoLogType::NAME, HTTPFSInfoLogType::LEVEL,
			                 HTTPFSInfoLogType::ConstructLogMessage(event.type, event.host, event.payload));
		}
	}
	events.clear();
	return result;
}

#endif

} // namespace duckdb
