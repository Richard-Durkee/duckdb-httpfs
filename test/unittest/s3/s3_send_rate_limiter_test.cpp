#include "catch.hpp"

#include "s3/mock_s3_server.hpp"
#include "s3/s3_request.hpp"
#include "s3/s3_send_rate_limiter.hpp"
#include "s3/s3_test_helper.hpp"

#include "duckdb.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/time_point.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/http/http_retry_budget.hpp"
#include "duckdb/main/http/http_util.hpp"

#include <chrono>
#include <cmath>
#include <iostream>
#include <thread>

namespace duckdb {

// Most tests start the limiter a second before their first request, so that its rates count over whole seconds.
static constexpr int64_t WHOLE_SECONDS = -1000;

static double FillRate(S3SendRateLimiter &limiter) {
	return limiter.FillRate();
}

static S3SentRequest Send(S3SendRateLimiter &limiter, int64_t now_ms) {
	S3SentRequest sent;
	limiter.TryAcquire(now_ms, sent);
	return sent;
}

// Turns a new limiter on at `now_ms`: two requests sent at once, one accepted and one refused. The refused rate is 2
// a second, so the fill rate is 2 * BETA = 1.4, and the refused request's retry takes the turn at `now_ms`.
static void TurnOn(S3SendRateLimiter &limiter, int64_t now_ms) {
	auto accepted = Send(limiter, now_ms);
	auto refused = Send(limiter, now_ms);
	limiter.OnResponse(accepted, false, now_ms);
	limiter.OnResponse(refused, true, now_ms);
}

static const auto TURN_ON_INTERVAL_MS = 1000 / (2 * S3SendRateLimiter::BETA);

TEST_CASE("S3 send rate limiter is off until the first throttle", "[httpfs][s3][rate_limit]") {
	S3SendRateLimiter limiter(WHOLE_SECONDS);
	idx_t waits = 0;
	S3SentRequest sent;
	for (int64_t now = 0; now < 1000; now++) {
		waits += limiter.TryAcquire(now, sent) != 0;
		waits += limiter.TryAcquire(now, sent) != 0;
	}
	REQUIRE(waits == 0);
	REQUIRE(std::isinf(FillRate(limiter)));
	REQUIRE(sent.rate == Approx(2000));
}

TEST_CASE("S3 send rate limiter takes a stale clock reading as the latest one", "[httpfs][s3][rate_limit]") {
	S3SendRateLimiter limiter(WHOLE_SECONDS);
	for (idx_t i = 0; i < 10; i++) {
		Send(limiter, 2000);
	}
	// A thread that read the clock before another took the lock does not move the counters back a second
	REQUIRE(Send(limiter, 1999).rate == Approx(11));
	REQUIRE(Send(limiter, 2500).rate == Approx(12));
}

TEST_CASE("S3 send rate limiter cuts at most once a second", "[httpfs][s3][rate_limit]") {
	S3SendRateLimiter limiter(WHOLE_SECONDS);
	// 20 requests in flight: 10 are accepted, then one is refused.
	vector<S3SentRequest> in_flight;
	for (int64_t now = 0; now < 20; now++) {
		in_flight.push_back(Send(limiter, now));
	}
	for (idx_t i = 0; i < 10; i++) {
		limiter.OnResponse(in_flight[i], false, 20);
	}
	limiter.OnResponse(in_flight[10], true, 20);
	auto cut = FillRate(limiter);
	REQUIRE(cut == Approx(20 * S3SendRateLimiter::BETA));
	// Another refusal within the second does not cut again, one a second later does
	limiter.OnResponse(in_flight[11], true, 600);
	REQUIRE(FillRate(limiter) == cut);
	auto late = Send(limiter, 1100);
	limiter.OnResponse(late, true, 1101);
	REQUIRE(FillRate(limiter) < cut);
}

TEST_CASE("S3 send rate limiter measures a new limiter's rates over the time it has run", "[httpfs][s3][rate_limit]") {
	S3SendRateLimiter limiter(0);
	// 200 requests in the first 200 ms, 100 accepted, then one refused: S3 refused about 1000 a second, not 200.
	vector<S3SentRequest> in_flight;
	for (int64_t now = 0; now < 200; now++) {
		in_flight.push_back(Send(limiter, now));
	}
	for (idx_t i = 0; i < 100; i++) {
		limiter.OnResponse(in_flight[i], false, 200);
	}
	limiter.OnResponse(in_flight[100], true, 200);
	REQUIRE(FillRate(limiter) == Approx(1000 * S3SendRateLimiter::BETA).epsilon(0.02));
	// The first few requests count over at least 100 ms
	S3SendRateLimiter early(0);
	REQUIRE(Send(early, 1).rate == Approx(10));
}

TEST_CASE("S3 send rate limiter does not cut while S3 accepts nothing", "[httpfs][s3][rate_limit]") {
	S3SendRateLimiter limiter(WHOLE_SECONDS);
	// One hot key, the first request after an idle spell, or a bucket that refuses everything: no rate is accepted.
	auto first = Send(limiter, 0);
	Send(limiter, 0);
	limiter.OnResponse(first, true, 10);
	REQUIRE(std::isinf(FillRate(limiter)));
	// Nor, once on, after a second in which S3 accepted nothing.
	TurnOn(limiter, 1000);
	auto rate = FillRate(limiter);
	auto refused = Send(limiter, 3500);
	limiter.OnResponse(refused, true, 3501);
	REQUIRE(FillRate(limiter) == rate);
}

TEST_CASE("S3 send rate limiter does not cut below what S3 accepted", "[httpfs][s3][rate_limit]") {
	S3SendRateLimiter limiter(WHOLE_SECONDS);
	// 100 requests in a second, 90 of them accepted, then one refused: S3 takes 90 a second.
	for (int64_t now = 0; now < 1000; now += 10) {
		auto sent = Send(limiter, now);
		if (now < 900) {
			limiter.OnResponse(sent, false, now);
		}
	}
	auto refused = Send(limiter, 1000);
	limiter.OnResponse(refused, true, 1005);
	REQUIRE(FillRate(limiter) == Approx(90));
}

TEST_CASE("S3 send rate limiter takes the rate a slow request was sent at", "[httpfs][s3][rate_limit]") {
	S3SendRateLimiter limiter(WHOLE_SECONDS);
	S3SentRequest refused;
	for (int64_t now = 0; now < 4000; now += 250) {
		refused = Send(limiter, now);
		if (now < 3750) {
			limiter.OnResponse(refused, false, now);
		}
	}
	REQUIRE(refused.rate == Approx(5));
	auto accepted = Send(limiter, 5500);
	limiter.OnResponse(accepted, false, 5500);
	// Refused 2 s after it was sent, when one request was sent in the last second: it was sent at 5 a second.
	limiter.OnResponse(refused, true, 6000);
	REQUIRE(FillRate(limiter) == Approx(5 * S3SendRateLimiter::BETA));
}

TEST_CASE("S3 send rate limiter grows back along the cubic curve", "[httpfs][s3][rate_limit]") {
	S3SendRateLimiter limiter(WHOLE_SECONDS);
	S3SentRequest refused;
	for (int64_t now = 0; now < 1000; now += 5) {
		auto sent = Send(limiter, now);
		if (now == 990) {
			refused = sent;
		} else if (now < 250) {
			limiter.OnResponse(sent, false, now);
		}
	}
	limiter.OnResponse(refused, true, 999);
	auto cut = FillRate(limiter);
	REQUIRE(cut == Approx(200 * S3SendRateLimiter::BETA));

	SECTION("but not past twice what is actually sent") {
		// One request a second for half a minute: the cubic curve is far above the limit by then.
		for (int64_t now = 2000; now < 32000; now += 1000) {
			auto sent = Send(limiter, now);
			limiter.OnResponse(sent, false, now);
		}
		REQUIRE(FillRate(limiter) == cut);
	}

	SECTION("climbs back past the refused rate while requests succeed") {
		// A sender that always has a request ready, answered at once
		bool never_dropped = true;
		S3SentRequest sent;
		for (int64_t now = 1000; now < 31000; now++) {
			auto before = FillRate(limiter);
			if (limiter.TryAcquire(now, sent) == 0) {
				limiter.OnResponse(sent, false, now);
			}
			never_dropped = never_dropped && FillRate(limiter) >= before;
		}
		REQUIRE(never_dropped);
		REQUIRE(FillRate(limiter) > cut / S3SendRateLimiter::BETA);
		// The sender sends at most one request a millisecond: the rate stops at twice that.
		REQUIRE(FillRate(limiter) == Approx(2 * 1000).epsilon(0.01));
	}
}

TEST_CASE("S3 send rate limiter gives waiting requests turns in order", "[httpfs][s3][rate_limit]") {
	S3SendRateLimiter limiter(WHOLE_SECONDS);
	TurnOn(limiter, 0);
	S3SentRequest first, second;
	// The next turns, after the refused request's retry, are TURN_ON_INTERVAL_MS (714.3 ms) apart
	REQUIRE(limiter.TryAcquire(0, first) == 715);
	REQUIRE(limiter.TryAcquire(0, second) == 1429);
	// Polling does not take another turn
	REQUIRE(limiter.TryAcquire(500, first) == 215);
	REQUIRE(limiter.TryAcquire(715, first) == 0);
	REQUIRE(limiter.TryAcquire(1428, second) == 1);
	REQUIRE(limiter.TryAcquire(1429, second) == 0);
}

TEST_CASE("S3 send rate limiter sends a request at the longest wait without pushing the queue back",
          "[httpfs][s3][rate_limit]") {
	S3SendRateLimiter limiter(WHOLE_SECONDS);
	TurnOn(limiter, 0);
	const auto max_wait_ms = int64_t(HTTPRetryBudget::MAX_BACKOFF_MS);
	// The queue is full after max_wait_ms / TURN_ON_INTERVAL_MS (28) turns
	idx_t at_longest_wait = 0;
	for (idx_t i = 0; i < 40; i++) {
		S3SentRequest sent;
		auto wait_ms = limiter.TryAcquire(0, sent);
		REQUIRE(wait_ms <= max_wait_ms);
		at_longest_wait += wait_ms == max_wait_ms;
	}
	REQUIRE(at_longest_wait >= 11);
	// One more turn a second later comes after the 28th, not after the 40th
	S3SentRequest later;
	REQUIRE(limiter.TryAcquire(1000, later) < max_wait_ms);
}

TEST_CASE("S3 send rate limiter gives a refused request's retry the turn it takes when refused",
          "[httpfs][s3][rate_limit]") {
	S3SendRateLimiter limiter(WHOLE_SECONDS);
	TurnOn(limiter, 0);
	S3SentRequest sent;
	REQUIRE(limiter.TryAcquire(0, sent) == 715);
	REQUIRE(limiter.TryAcquire(715, sent) == 0);
	// Refused at 800, it takes the next turn (2 * TURN_ON_INTERVAL_MS): core's back-off counts towards the wait
	limiter.OnResponse(sent, true, 800);
	REQUIRE(limiter.TryAcquire(1000, sent) == 429);
	REQUIRE(limiter.TryAcquire(1500, sent) == 0);
}

TEST_CASE("S3 send rate limiter undoes a cut that did not lower the share of throttled requests",
          "[httpfs][s3][rate_limit]") {
	S3SendRateLimiter limiter(WHOLE_SECONDS);
	// Turned on at 20 a second: the fill rate is 14
	vector<S3SentRequest> in_flight;
	for (int64_t now = 0; now < 20; now++) {
		in_flight.push_back(Send(limiter, now));
	}
	for (idx_t i = 0; i < 10; i++) {
		limiter.OnResponse(in_flight[i], false, 20);
	}
	limiter.OnResponse(in_flight[10], true, 20);
	REQUIRE(FillRate(limiter) == Approx(14));
	// Cut again a second later, from the 18 a second sent then: 1 in 11 responses was a throttle
	limiter.OnResponse(in_flight[11], true, 1100);
	REQUIRE(FillRate(limiter) == Approx(18 * S3SendRateLimiter::BETA));
	auto accepted = Send(limiter, 1200);
	limiter.OnResponse(accepted, false, 1200);
	auto refused = Send(limiter, 1300);
	limiter.OnResponse(refused, true, 1300);
	// A second later S3 refuses 2 of 3 at a lower rate: the cut is undone, rather than cut again to 9.3
	refused = Send(limiter, 2200);
	limiter.OnResponse(refused, true, 2200);
	REQUIRE(FillRate(limiter) == Approx(14));
}

TEST_CASE("S3 send rate limiter starts over when idle", "[httpfs][s3][rate_limit]") {
	S3SendRateLimiter limiter(0);
	TurnOn(limiter, 0);
	const auto idle_ms = HTTPRetryQuota::IDLE_RESET_MS;
	// Polling keeps it in use
	S3SentRequest sent;
	limiter.TryAcquire(idle_ms - 1, sent);
	limiter.TryAcquire(idle_ms + 10, sent);
	REQUIRE(!std::isinf(FillRate(limiter)));
	REQUIRE(limiter.TryAcquire(2 * idle_ms + 20, sent) == 0);
	REQUIRE(std::isinf(FillRate(limiter)));
}

TEST_CASE("S3 send rate limiters make room by dropping the one idle for longest", "[httpfs][s3][rate_limit]") {
	S3SendRateLimiters limiters;
	auto a = limiters.Get("a", 0);
	REQUIRE(limiters.Get("a", 0) == a);
	auto b = limiters.Get("b", 0);
	for (idx_t i = 2; i < HTTPUtil::MAX_RETRY_QUOTAS; i++) {
		limiters.Get(StringUtil::Format("p%llu", i), 1000 + int64_t(i));
	}
	Send(*b, 4000);
	limiters.Get("new", 5000);
	REQUIRE(limiters.Get("b", 5000) == b);
	REQUIRE(limiters.Get("a", 5000) != a);
}

TEST_CASE("S3 send rate limiters are kept per bucket, for reads and for writes", "[httpfs][s3][rate_limit]") {
	const string endpoint = "s3.amazonaws.com";
	auto key = [&](S3RequestOperation operation) {
		return S3RequestUtil::SendRateKey(endpoint, "bucket", operation);
	};
	REQUIRE(key(S3RequestOperation::GET_OBJECT) != key(S3RequestOperation::PUT_OBJECT));
	REQUIRE(key(S3RequestOperation::HEAD_OBJECT) == key(S3RequestOperation::GET_OBJECT));
	// Listings are left to the glob code, which adapts its own concurrency to throttling.
	REQUIRE(key(S3RequestOperation::LIST_OBJECTS).empty());
}

TEST_CASE("S3 send rate ignores a request that did not get through", "[httpfs][s3][rate_limit]") {
	const auto now = TimePoint::GetTickMs();
	auto limiter = make_shared_ptr<S3SendRateLimiter>(now - 30000 + WHOLE_SECONDS);
	// Turned on 30 s ago: the cubic curve is far above the fill rate by now.
	TurnOn(*limiter, now - 30000);
	S3SendRate send_rate(limiter, S3RequestOperation::PUT_OBJECT, weak_ptr<ClientContext>());
	send_rate.BeforeSend();
	HTTPResponse failed(HTTPStatusCode::OK_200);
	failed.request_error = "Connection reset";
	send_rate.OnResponse(failed);
	REQUIRE(FillRate(*limiter) == Approx(2 * S3SendRateLimiter::BETA));
	send_rate.OnResponse(HTTPResponse(HTTPStatusCode::OK_200));
	REQUIRE(FillRate(*limiter) > 2 * S3SendRateLimiter::BETA);
}

TEST_CASE("S3 send rate stops waiting for a turn when the query is interrupted", "[httpfs][s3][rate_limit]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto now = TimePoint::GetTickMs();
	auto limiter = make_shared_ptr<S3SendRateLimiter>(now);
	TurnOn(*limiter, now);
	// Ten requests wait ahead: the next turn is 11 * TURN_ON_INTERVAL_MS (7.9 s) away
	for (idx_t i = 0; i < 10; i++) {
		S3SentRequest waiting;
		limiter->TryAcquire(now, waiting);
	}
	S3SendRate send_rate(limiter, S3RequestOperation::PUT_OBJECT, con.context);
	con.Interrupt();
	auto started = std::chrono::steady_clock::now();
	REQUIRE_THROWS_AS(send_rate.BeforeSend(), InterruptException);
	REQUIRE(std::chrono::steady_clock::now() - started < std::chrono::seconds(5));
}

TEST_CASE("S3 adaptive send rate can only be set globally", "[httpfs][s3][rate_limit]") {
	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::LoadExtension(db);
	auto session = con.Query("SET SESSION s3_adaptive_send_rate=true");
	REQUIRE(session->HasError());
	REQUIRE(StringUtil::Contains(session->GetError(), "can only be set globally"));
	S3TestHelper::RequireQueryOk(con, "SET s3_adaptive_send_rate=true");
	Connection other(db);
	auto result = other.Query("SELECT current_setting('s3_adaptive_send_rate')");
	REQUIRE(result->Collection().GetValue(0, 0) == Value::BOOLEAN(true));
}

struct ParallelWriteResult {
	idx_t slowdowns = 0;
	idx_t failed_writes = 0;
	//! The fill rate of the bucket's write limiter at the end
	double fill_rate = 0;
	//! How long the writes took
	double seconds = 0;
};

// `threads` threads each write `writes_per_thread` small objects (one PUT each) to a mock bucket that answers 503
// SlowDown above `max_writes_per_second`, as S3 does.
static ParallelWriteResult RunParallelWrites(bool adaptive_send_rate, idx_t threads, idx_t writes_per_thread,
                                             idx_t max_writes_per_second, idx_t response_delay_ms = 0) {
	MockS3ServerConfig config;
	config.write_sink.bucket = "sink-bucket";
	config.write_sink.max_writes_per_second = max_writes_per_second;
	config.write_sink.response_delay_ms = response_delay_ms;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::LoadExtension(db);
	S3TestHelper::RequireQueryOk(con, "SET httpfs_client_implementation='httplib'");
	S3TestHelper::RequireQueryOk(con, "SET httpfs_enable_credential_refresh=false");
	S3TestHelper::RequireQueryOk(con, "SET GLOBAL http_retry_wait_ms=20");
	S3TestHelper::RequireQueryOk(con, "SET GLOBAL http_retry_backoff=2");
	S3TestHelper::RequireQueryOk(
	    con, StringUtil::Format("SET GLOBAL s3_adaptive_send_rate=%s", adaptive_send_rate ? "true" : "false"));
	S3TestHelper::RequireQueryOk(con, StringUtil::Format(R"(
CREATE SECRET sink (
	TYPE S3,
	KEY_ID 'SINK_KEY',
	SECRET 'SINK_SECRET',
	REGION 'us-east-1',
	ENDPOINT '%s',
	USE_SSL false,
	URL_STYLE 'path'
))",
	                                                     server.Endpoint()));

	atomic<idx_t> failed_writes {0};
	auto start = std::chrono::steady_clock::now();
	vector<std::thread> workers;
	for (idx_t thread_index = 0; thread_index < threads; thread_index++) {
		workers.emplace_back([&, thread_index]() {
			Connection thread_con(db);
			// S3 writes run inside a transaction, as a query's do.
			thread_con.BeginTransaction();
			auto &fs = FileSystem::GetFileSystem(*thread_con.context);
			for (idx_t i = 0; i < writes_per_thread; i++) {
				auto path = StringUtil::Format("s3://sink-bucket/scratch/t%llu/part-%llu.bin", thread_index, i);
				try {
					auto handle =
					    fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
					string payload = "payload";
					handle->Write(QueryContext(*thread_con.context), data_ptr_cast(payload.data()), payload.size());
					handle->Close();
				} catch (std::exception &) {
					failed_writes++;
				}
			}
			thread_con.Commit();
		});
	}
	for (auto &worker : workers) {
		worker.join();
	}
	ParallelWriteResult result;
	result.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
	result.slowdowns = server.WriteSinkSlowDowns();
	result.failed_writes = failed_writes.load();
	REQUIRE(server.WriteSinkWrites() == threads * writes_per_thread - result.failed_writes);
	auto &http_util = DBConfig::GetConfig(*db.instance).GetHTTPUtil().Cast<HTTPFSUtil>();
	auto key = S3RequestUtil::SendRateKey(server.Endpoint(), "sink-bucket", S3RequestOperation::PUT_OBJECT);
	result.fill_rate = http_util.SendRateLimiters().Get(key, TimePoint::GetTickMs())->FillRate();
	return result;
}

TEST_CASE("S3 adaptive send rate turns on for a bucket that throttles writes", "[httpfs][s3][rate_limit][upload]") {
	auto result = RunParallelWrites(true, 4, 10, 20);
	REQUIRE(result.slowdowns > 0);
	// The cut starts from the rate the requests were sent at, which only their turns count: without the turns it would
	// give the lowest rate, 0.5 a second.
	REQUIRE(!std::isinf(result.fill_rate));
	REQUIRE(result.fill_rate > 1);
}

TEST_CASE("S3 adaptive send rate keeps parallel writes under a throttled bucket's rate",
          "[httpfs][s3][rate_limit][upload][.]") {
	auto without = RunParallelWrites(false, 8, 25, 50);
	auto with = RunParallelWrites(true, 8, 25, 50);
	INFO("without: " << without.slowdowns << " SlowDowns, " << without.failed_writes << " failed writes; with: "
	                 << with.slowdowns << " SlowDowns, " << with.failed_writes << " failed writes");
	// Without the limit every thread keeps sending at the refused rate and retries into the same throttle.
	REQUIRE(without.slowdowns > 0);
	// With it the process settles near the rate the bucket takes: far fewer refusals, and every write gets through.
	REQUIRE(with.failed_writes == 0);
	REQUIRE(with.slowdowns * 3 < without.slowdowns);
}

// Prints SlowDowns, failed writes and seconds with the setting off and on, over a few runs each, for tuning.
TEST_CASE("S3 adaptive send rate costs little time against a throttled bucket", "[httpfs][s3][rate_limit][.]") {
	for (idx_t run = 0; run < 3; run++) {
		for (bool adaptive : {false, true}) {
			auto result = RunParallelWrites(adaptive, 32, 60, 400, 20);
			std::cout << (adaptive ? "on " : "off") << " slowdowns " << result.slowdowns << " failed "
			          << result.failed_writes << " seconds " << result.seconds << " fill " << result.fill_rate
			          << std::endl;
		}
	}
}

} // namespace duckdb
