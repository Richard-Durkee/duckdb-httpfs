#include "http/http_state.hpp"
#include "duckdb/main/http/http_util.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

bool HTTPStateCounters::IsEmpty() const {
	return head_count == 0 && get_count == 0 && put_count == 0 && post_count == 0 && delete_count == 0 &&
	       options_count == 0 && total_bytes_received == 0 && total_bytes_sent == 0;
}

unique_ptr<CachedFileHandle> CachedFile::TryGetHandle() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (!cached_data) {
		return nullptr;
	}
	return make_uniq<CachedFileHandle>(cached_data);
}

unique_ptr<CachedFileDownload> CachedFile::StartDownload(Allocator &allocator) {
	annotated_unique_lock<annotated_mutex> guard(lock);
	while (downloading) {
		download_complete.wait(guard, [&]() DUCKDB_REQUIRES(lock) { return !downloading; });
	}
	if (cached_data) {
		return nullptr;
	}
	auto result = unique_ptr<CachedFileDownload>(new CachedFileDownload(shared_from_this(), allocator));
	downloading = true;
	return result;
}

void CachedFile::Invalidate() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	cached_data.reset();
}

CachedFileHandle::CachedFileHandle(shared_ptr<CachedFileData> file_p) : file(std::move(file_p)) {
}

const char *CachedFileHandle::GetData() const {
	return const_char_ptr_cast(file->data.get());
}

idx_t CachedFileHandle::GetSize() const {
	return file->metadata.length;
}

const HTTPMetadataCacheEntry &CachedFileHandle::GetMetadata() const {
	return file->metadata;
}

CachedFileDownload::CachedFileDownload(shared_ptr<CachedFile> file_p, Allocator &allocator_p)
    : file(std::move(file_p)), allocator(allocator_p) {
}

CachedFileDownload::~CachedFileDownload() {
	Abort();
}

void CachedFileDownload::ReserveInternal(idx_t capacity) {
	if (capacity <= this->capacity) {
		return;
	}
	auto new_data = allocator.Allocate(capacity);
	if (size > 0) {
		memcpy(new_data.get(), data.get(), size);
	}
	data = std::move(new_data);
	this->capacity = capacity;
}

void CachedFileDownload::Reserve(idx_t capacity) {
	ReserveInternal(capacity);
}

void CachedFileDownload::Append(const_data_ptr_t data, idx_t length) {
	if (length == 0) {
		return;
	}
	if (length > NumericLimits<idx_t>::Maximum() - size) {
		throw OutOfMemoryException("Cached file size exceeds the maximum allocation size");
	}
	const auto required_capacity = size + length;
	if (required_capacity > capacity) {
		auto new_capacity = MaxValue<idx_t>(capacity, length);
		while (new_capacity < required_capacity) {
			if (new_capacity > NumericLimits<idx_t>::Maximum() / 2) {
				new_capacity = required_capacity;
				break;
			}
			new_capacity *= 2;
		}
		ReserveInternal(new_capacity);
	}
	memcpy(this->data.get() + size, data, length);
	size += length;
}

void CachedFileDownload::Reset() {
	data.Reset();
	capacity = 0;
	size = 0;
}

unique_ptr<CachedFileHandle> CachedFileDownload::Finalize(HTTPMetadataCacheEntry metadata) {
	metadata.length = size;
	auto cached_data = make_shared_ptr<CachedFileData>(std::move(data), std::move(metadata));
	annotated_lock_guard<annotated_mutex> guard(file->lock);
	D_ASSERT(file->downloading);
	D_ASSERT(!file->cached_data);
	file->cached_data = cached_data;
	file->downloading = false;
	active = false;
	file->download_complete.notify_all();
	return make_uniq<CachedFileHandle>(std::move(cached_data));
}

void CachedFileDownload::Abort() {
	if (!active) {
		return;
	}
	annotated_lock_guard<annotated_mutex> guard(file->lock);
	D_ASSERT(file->downloading);
	file->downloading = false;
	active = false;
	file->download_complete.notify_all();
}

void HTTPState::Reset() {
	// Reset Counters
	head_count = 0;
	get_count = 0;
	put_count = 0;
	post_count = 0;
	delete_count = 0;
	options_count = 0;
	total_bytes_received = 0;
	total_bytes_sent = 0;

	// Reset per-path state
	annotated_lock_guard<annotated_mutex> lock(file_states_mutex);
	file_states.clear();
}

static const string REQUESTS_METRIC = "httpfs.requests";
static const string RETRIES_METRIC = "httpfs.retries";
static const string REQUEST_TIME_METRIC = "httpfs.request_time";

void HTTPState::RegisterMetrics(ExtensionLoader &loader) {
	loader.RegisterMetric(
	    {REQUESTS_METRIC, "uint64", "HTTP requests sent, every attempt counted and retries included", "requests"});
	loader.RegisterMetric({RETRIES_METRIC, "uint64",
	                       "HTTP requests sent again after an earlier attempt of the same operation, also counted in "
	                       "httpfs.requests",
	                       "requests"});
	loader.RegisterMetric({REQUEST_TIME_METRIC, "double",
	                       "Time spent in HTTP requests, added up over all threads and attempts", "seconds"});
}

void HTTPState::AttachToQuery(ClientContext &context) {
	if (!QueryProfiler::Get(context).IsEnabled()) {
		return;
	}
	shared_ptr<ClientContext> shared_context;
	try {
		shared_context = context.shared_from_this();
	} catch (...) { // NOLINT: a context that is not shared cannot be profiled through us
		return;
	}
	annotated_lock_guard<annotated_mutex> guard(profiled_context_lock);
	profiled_context = std::move(shared_context);
	profiling = true;
}

void HTTPState::DetachFromQuery() {
	annotated_lock_guard<annotated_mutex> guard(profiled_context_lock);
	profiling = false;
	profiled_context.reset();
}

shared_ptr<ClientContext> HTTPState::GetProfiledContext() {
	annotated_lock_guard<annotated_mutex> guard(profiled_context_lock);
	return profiled_context.lock();
}

void HTTPState::RecordRequestMetrics(idx_t elapsed_ns) {
	// A strong reference, so that the context cannot go away while its profiler is in use
	auto context = GetProfiledContext();
	if (!context) {
		return;
	}
	try {
		auto &profiler = QueryProfiler::Get(*context);
		profiler.AddToMetricCounter(REQUESTS_METRIC, 1);
		profiler.AddToMetricTiming(REQUEST_TIME_METRIC, elapsed_ns);
	} catch (...) { // NOLINT: a lost figure must not fail the request
	}
}

void HTTPState::RecordRetryMetric() {
	auto context = GetProfiledContext();
	if (!context) {
		return;
	}
	try {
		QueryProfiler::Get(*context).AddToMetricCounter(RETRIES_METRIC, 1);
	} catch (...) { // NOLINT: a lost figure must not fail the request
	}
}

void HTTPState::RecordRequest(RequestType request_type) {
	switch (request_type) {
	case RequestType::HEAD_REQUEST:
		head_count++;
		break;
	case RequestType::GET_REQUEST:
		get_count++;
		break;
	case RequestType::PUT_REQUEST:
		put_count++;
		break;
	case RequestType::POST_REQUEST:
		post_count++;
		break;
	case RequestType::DELETE_REQUEST:
		delete_count++;
		break;
	case RequestType::OPTIONS_REQUEST:
		options_count++;
		break;
	default:
		throw InternalException("Unsupported HTTP request type");
	}
}

void HTTPState::RecordBytesReceived(idx_t byte_count) {
	total_bytes_received += byte_count;
}

void HTTPState::RecordBytesSent(idx_t byte_count) {
	total_bytes_sent += byte_count;
}

HTTPStateCounters HTTPState::GetCounters() const {
	HTTPStateCounters result;
	result.head_count = head_count.load();
	result.get_count = get_count.load();
	result.put_count = put_count.load();
	result.post_count = post_count.load();
	result.delete_count = delete_count.load();
	result.options_count = options_count.load();
	result.total_bytes_received = total_bytes_received.load();
	result.total_bytes_sent = total_bytes_sent.load();
	return result;
}

bool HTTPState::IsEmpty() const {
	return GetCounters().IsEmpty();
}

shared_ptr<HTTPState> HTTPState::TryGetState(ClientContext &context) {
	auto state = context.registered_state->GetOrCreate<HTTPState>("http_state");
	if (!state->IsProfiling()) {
		state->AttachToQuery(context);
	}
	return state;
}

shared_ptr<HTTPState> HTTPState::TryGetState(optional_ptr<FileOpener> opener) {
	auto client_context = FileOpener::TryGetClientContext(opener);
	if (client_context) {
		return TryGetState(*client_context);
	}
	return nullptr;
}

void HTTPState::WriteProfilingInformation(std::ostream &ss) {
	auto counters = GetCounters();
	string read = "in: " + StringUtil::BytesToHumanReadableString(counters.total_bytes_received);
	string written = "out: " + StringUtil::BytesToHumanReadableString(counters.total_bytes_sent);
	string head = "#HEAD: " + to_string(counters.head_count);
	string get = "#GET: " + to_string(counters.get_count);
	string put = "#PUT: " + to_string(counters.put_count);
	string post = "#POST: " + to_string(counters.post_count);
	string del = "#DELETE: " + to_string(counters.delete_count);
	string options = "#OPTIONS: " + to_string(counters.options_count);

	constexpr idx_t TOTAL_BOX_WIDTH = 39;
	ss << "┌─────────────────────────────────────┐\n";
	ss << "│┌───────────────────────────────────┐│\n";
	ss << "││" + QueryProfiler::DrawPadded("HTTPFS HTTP Stats", TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││                                   ││\n";
	ss << "││" + QueryProfiler::DrawPadded(read, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(written, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(head, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(get, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(put, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(post, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(del, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(options, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "│└───────────────────────────────────┘│\n";
	ss << "└─────────────────────────────────────┘\n";
}

bool HTTPState::RunCredentialRefresh(const std::function<bool()> &callback) {
	annotated_lock_guard<annotated_mutex> guard(credential_refresh_mutex);
	return callback();
}

//! Get per-path state, create if it does not exist
shared_ptr<HTTPFileState> HTTPState::GetFileState(const string &path) {
	annotated_lock_guard<annotated_mutex> lock(file_states_mutex);
	auto &file_state = file_states[path];
	if (!file_state) {
		file_state = make_shared_ptr<HTTPFileState>();
	}
	return file_state;
}

void HTTPState::EraseFileState(const string &path) {
	annotated_lock_guard<annotated_mutex> lock(file_states_mutex);
	file_states.erase(path);
}

} // namespace duckdb
