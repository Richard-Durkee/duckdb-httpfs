//===----------------------------------------------------------------------===//
//                         DuckDB
//
// s3/s3_send_rate_limiter.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

class ClientContext;
struct HTTPResponse;
enum class S3RequestOperation : uint8_t;

//! One attempt of a request: its turn, and the send rate it was sent at.
struct S3SentRequest {
	bool has_turn = false;
	double turn_ms = 0;
	double rate = 0;
};

//! A client-side AIMD rate limiter for the requests to one S3 bucket, reads or writes, after the AWS SDKs' adaptive
//! retry mode (CUBIC): off until S3 throttles while it accepts other requests, then requests take turns at a fill
//! rate that a throttle cuts by BETA and that grows back along a cubic curve while requests succeed.
//! Deviations: a cut at most once a second and never below what S3 accepted in the last second; a cut that lowered
//! the rate sent but not the share of throttled responses is undone; a refused request's retry takes its turn when it
//! is refused.
//! Times are monotonic milliseconds, passed in so that the policy is deterministic.
class S3SendRateLimiter {
	friend class S3SendRateLimiters;

public:
	//! Multiplicative decrease on a throttle.
	static constexpr double BETA = 0.7;

public:
	explicit S3SendRateLimiter(int64_t now_ms);

	//! 0 when the attempt may be sent at `now_ms`, which counts it as sent; otherwise the milliseconds until its turn.
	//! The first call takes the next turn, or sends at once when the queue is longer than the longest wait.
	int64_t TryAcquire(int64_t now_ms, S3SentRequest &sent);
	//! Records the response to an attempt; a throttled one may cut the rate and takes the turn of its retry.
	void OnResponse(S3SentRequest &sent, bool throttled, int64_t now_ms);
	//! Requests per second; infinite while off.
	double FillRate() const;

private:
	//! Events in the current second, the one before and the one before that.
	struct Counter {
		idx_t seconds[3] = {0, 0, 0};
	};
	struct State {
		explicit State(int64_t now_ms);

		double fill_rate;
		double next_send_ms = 0;
		//! The refused rate (the cubic curve's maximum), and when the rate was last cut or a cut undone.
		double rate_at_cut = 0;
		int64_t last_cut_ms;
		//! The fill rate before the last cut, and the rate sent and the share of throttled responses then, to undo a
		//! cut that lowered the rate sent but not the share of throttled responses.
		double rate_before_cut = 0;
		double sent_rate_at_cut = 0;
		double throttled_share_at_cut = 0;
		bool cut_to_check = false;
		//! When the limiter started, or started over
		int64_t started_ms;
		int64_t second;
		Counter sent;
		Counter accepted;
		Counter throttled;
	};

	//! Takes the clock no further back than it was seen, starts over when idle and moves the counters to its second.
	int64_t Update(int64_t now_ms) DUCKDB_REQUIRES(lock);
	void TakeTurn(S3SentRequest &sent, int64_t now_ms) DUCKDB_REQUIRES(lock);
	void Cut(const S3SentRequest &sent, int64_t now_ms) DUCKDB_REQUIRES(lock);
	//! Events per second over about the last `seconds` (1 or 2), or since the limiter started when that is shorter.
	double Rate(const Counter &counter, int64_t now_ms, idx_t seconds = 1) const DUCKDB_REQUIRES(lock);
	int64_t IdleMs(int64_t now_ms) const;

	mutable annotated_mutex lock;
	int64_t last_used_ms DUCKDB_GUARDED_BY(lock);
	State state DUCKDB_GUARDED_BY(lock);
};

//! The send rate limiters of one HTTP provider, at most as many as core keeps retry quotas; the one idle for longest
//! makes room, and one evicted while in use is then created again.
class S3SendRateLimiters {
public:
	shared_ptr<S3SendRateLimiter> Get(const string &key, int64_t now_ms) DUCKDB_EXCLUDES(lock);

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<S3SendRateLimiter>> limiters DUCKDB_GUARDED_BY(lock);
};

//! The send rate limiter of one S3 request, core's retries of it included, and its current attempt. A copy starts
//! without one, so that parameters copied from a request do not share its attempts.
class S3SendRate {
public:
	S3SendRate() = default;
	S3SendRate(shared_ptr<S3SendRateLimiter> limiter, S3RequestOperation operation, weak_ptr<ClientContext> context);
	S3SendRate(const S3SendRate &) {
	}
	S3SendRate &operator=(const S3SendRate &) {
		*this = S3SendRate();
		return *this;
	}
	S3SendRate(S3SendRate &&) = default;
	S3SendRate &operator=(S3SendRate &&) = default;

public:
	//! Waits for the attempt's turn. The first attempt waits before it takes a connection (`ahead`); core's retries
	//! then wait in HTTPFSUtil::PrepareAttempt, where the first attempt does not wait again.
	void BeforeSend(bool ahead = false);
	void OnResponse(const HTTPResponse &response);

private:
	shared_ptr<S3SendRateLimiter> limiter;
	S3RequestOperation operation;
	weak_ptr<ClientContext> context;
	S3SentRequest sent;
	bool sent_ahead = false;
};

} // namespace duckdb
