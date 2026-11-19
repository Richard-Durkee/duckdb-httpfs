#include "s3/s3_send_rate_limiter.hpp"

#include "duckdb/common/thread.hpp"
#include "duckdb/common/time_point.hpp"
#include "duckdb/main/http/http_retry_budget.hpp"
#include "duckdb/main/http/http_util.hpp"
#include "s3/s3_request.hpp"

#include <cmath>
#include <limits>

namespace duckdb {

//! Scaling of the cubic recovery (the SDKs' value).
static constexpr double SCALE_CONSTANT = 0.4;
//! Lowest fill rate, in requests per second.
static constexpr double MIN_RATE = 0.5;
//! Shortest time between cuts: the send rate is measured over a second.
static constexpr int64_t MIN_CUT_INTERVAL_MS = 1000;
//! Shortest time a rate is measured over, so that the first few requests do not count as a high rate.
static constexpr double MIN_RATE_WINDOW_S = 0.1;
//! Longest wait for a turn, as core's longest back-off; without threads there is no waiting, as there is no back-off.
#ifdef DUCKDB_NO_THREADS
static constexpr int64_t MAX_WAIT_MS = 0;
#else
static constexpr int64_t MAX_WAIT_MS = int64_t(HTTPRetryBudget::MAX_BACKOFF_MS);
#endif
//! A limiter unused for this long starts over, off, as core's retry quotas do.
static constexpr int64_t IDLE_RESET_MS = HTTPRetryQuota::IDLE_RESET_MS;
static constexpr double OFF = std::numeric_limits<double>::infinity();

S3SendRateLimiter::State::State(int64_t now_ms)
    : fill_rate(OFF), last_cut_ms(now_ms - MIN_CUT_INTERVAL_MS), started_ms(now_ms), second(now_ms / 1000) {
}

S3SendRateLimiter::S3SendRateLimiter(int64_t now_ms) : last_used_ms(now_ms), state(now_ms) {
}

int64_t S3SendRateLimiter::Update(int64_t now_ms) {
	now_ms = MaxValue<int64_t>(now_ms, last_used_ms);
	if (now_ms - last_used_ms >= IDLE_RESET_MS) {
		state = State(now_ms);
	}
	last_used_ms = now_ms;
	auto second = now_ms / 1000;
	for (auto shifts = MinValue<int64_t>(second - state.second, 3); shifts > 0; shifts--) {
		for (auto counter : {&state.sent, &state.accepted, &state.throttled}) {
			counter->seconds[2] = counter->seconds[1];
			counter->seconds[1] = counter->seconds[0];
			counter->seconds[0] = 0;
		}
	}
	state.second = second;
	return now_ms;
}

double S3SendRateLimiter::Rate(const Counter &counter, int64_t now_ms, idx_t seconds) const {
	auto fading = 1 - double(now_ms % 1000) / 1000.0;
	auto &count = counter.seconds;
	auto events = seconds == 1 ? double(count[1]) * fading + double(count[0])
	                           : double(count[2]) * fading + double(count[1]) + double(count[0]);
	// A limiter that started less than `seconds` ago counted its events over a shorter time
	auto covered = MinValue<double>(double(seconds), double(now_ms - state.started_ms) / 1000.0);
	return events / MaxValue<double>(covered, MIN_RATE_WINDOW_S);
}

void S3SendRateLimiter::TakeTurn(S3SentRequest &sent, int64_t now_ms) {
	auto turn_ms = MaxValue<double>(double(now_ms), state.next_send_ms);
	if (turn_ms > double(now_ms + MAX_WAIT_MS)) {
		// Sent after the longest wait, without moving the queue
		turn_ms = double(now_ms + MAX_WAIT_MS);
	} else {
		state.next_send_ms = turn_ms + 1000.0 / state.fill_rate;
	}
	sent.has_turn = true;
	sent.turn_ms = turn_ms;
}

int64_t S3SendRateLimiter::TryAcquire(int64_t now_ms, S3SentRequest &sent) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	now_ms = Update(now_ms);
	if (!sent.has_turn) {
		TakeTurn(sent, now_ms);
	}
	if (double(now_ms) < sent.turn_ms) {
		return int64_t(std::ceil(sent.turn_ms - double(now_ms)));
	}
	sent.has_turn = false;
	state.sent.seconds[0]++;
	sent.rate = Rate(state.sent, now_ms);
	return 0;
}

void S3SendRateLimiter::Cut(const S3SentRequest &sent, int64_t now_ms) {
	auto throttled = Rate(state.throttled, now_ms);
	auto share = throttled / (throttled + Rate(state.accepted, now_ms));
	auto rate_now = Rate(state.sent, now_ms);
	if (state.cut_to_check && rate_now < state.sent_rate_at_cut && share >= state.throttled_share_at_cut) {
		// S3 refuses as large a share at a lower rate, so the throttling does not depend on the rate
		state.fill_rate = MaxValue<double>(state.fill_rate, state.rate_before_cut);
		state.cut_to_check = false;
		state.last_cut_ms = now_ms;
		return;
	}
	// Core's back-off lowers the rate sent in the last second, so the refused rate is also at least the rate over two
	// seconds, the rate the request was sent at and the fill rate
	auto refused = MaxValue<double>(sent.rate, MaxValue<double>(rate_now, Rate(state.sent, now_ms, 2)));
	// The first cut is not checked: the share of throttled responses before the first throttle says nothing
	state.cut_to_check = state.fill_rate != OFF;
	if (state.cut_to_check) {
		refused = MaxValue<double>(refused, state.fill_rate);
	}
	auto accepted = MinValue<double>(double(state.accepted.seconds[1]), refused);
	state.rate_before_cut = state.fill_rate;
	state.throttled_share_at_cut = share;
	state.sent_rate_at_cut = rate_now;
	state.rate_at_cut = refused;
	auto cut = MaxValue<double>(MaxValue<double>(MIN_RATE, accepted), refused * BETA);
	state.fill_rate = MinValue<double>(state.fill_rate, cut);
	state.last_cut_ms = now_ms;
}

void S3SendRateLimiter::OnResponse(S3SentRequest &sent, bool throttled, int64_t now_ms) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	now_ms = Update(now_ms);
	if (!throttled) {
		state.accepted.seconds[0]++;
		// W(t) = C * (t - K)^3 + W_max reaches the refused rate again at t = K, but never past twice the send rate
		auto k = std::cbrt(state.rate_at_cut * (1 - BETA) / SCALE_CONSTANT);
		auto t = double(now_ms - state.last_cut_ms) / 1000.0;
		auto cubic = SCALE_CONSTANT * std::pow(t - k, 3) + state.rate_at_cut;
		auto rate_now = Rate(state.sent, now_ms);
		state.fill_rate = MaxValue<double>(state.fill_rate, MinValue<double>(cubic, 2 * rate_now));
		return;
	}
	state.throttled.seconds[0]++;
	// Cut only while S3 accepts other requests: when it refuses all of them, core's back-off applies
	if (now_ms - state.last_cut_ms >= MIN_CUT_INTERVAL_MS && Rate(state.accepted, now_ms) > 0) {
		Cut(sent, now_ms);
	}
	// The retry waits for its turn during core's back-off, not after it
	TakeTurn(sent, now_ms);
}

double S3SendRateLimiter::FillRate() const {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return state.fill_rate;
}

int64_t S3SendRateLimiter::IdleMs(int64_t now_ms) const {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return now_ms - last_used_ms;
}

// Copied from HTTPUtil::GetRetryQuota
shared_ptr<S3SendRateLimiter> S3SendRateLimiters::Get(const string &key, int64_t now_ms) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto entry = limiters.find(key);
	if (entry != limiters.end()) {
		return entry->second;
	}
	if (limiters.size() >= HTTPUtil::MAX_RETRY_QUOTAS) {
		auto victim = limiters.begin();
		for (auto it = limiters.begin(); it != limiters.end(); ++it) {
			if (it->second->IdleMs(now_ms) > victim->second->IdleMs(now_ms)) {
				victim = it;
			}
		}
		limiters.erase(victim);
	}
	auto limiter = make_shared_ptr<S3SendRateLimiter>(now_ms);
	limiters.emplace(key, limiter);
	return limiter;
}

S3SendRate::S3SendRate(shared_ptr<S3SendRateLimiter> limiter_p, S3RequestOperation operation_p,
                       weak_ptr<ClientContext> context_p)
    : limiter(std::move(limiter_p)), operation(operation_p), context(std::move(context_p)) {
}

void S3SendRate::BeforeSend(bool ahead) {
	if (!limiter || sent_ahead) {
		sent_ahead = false;
		return;
	}
	int64_t wait_ms;
	while ((wait_ms = limiter->TryAcquire(TimePoint::GetTickMs(), sent)) > 0) {
		ThreadUtil::SleepMs(idx_t(wait_ms), context.lock().get());
	}
	sent_ahead = ahead;
}

void S3SendRate::OnResponse(const HTTPResponse &response) {
	if (!limiter || response.HasRequestError()) {
		// Without a response nothing is known about S3's rate
		return;
	}
	limiter->OnResponse(sent, S3RequestUtil::IsThrottledResponse(operation, response), TimePoint::GetTickMs());
}

} // namespace duckdb
