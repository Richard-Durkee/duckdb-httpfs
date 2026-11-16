#include "s3/s3fs.hpp"

#include "s3/s3_request.hpp"
#include "s3/s3_url.hpp"
#include "s3/s3_xml_response.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//! The largest object one CopyObject copies; a larger one needs UploadPartCopy
static constexpr idx_t MAXIMUM_COPY_OBJECT_SIZE = 5ULL << 30ULL;

void S3FileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	if (!CanHandleFile(target)) {
		throw NotImplementedException("Moving \"%s\" to \"%s\" is not supported: the target is not an S3 path", source,
		                              target);
	}
	auto options = S3AuthResolveOptions::ForOperation(opener);
	FileOpenerInfo source_info = {source};
	auto source_auth = S3AuthResolver::Resolve(opener, source_info, options);
	FileOpenerInfo target_info = {target};
	auto target_auth = S3AuthResolver::Resolve(opener, target_info, options);
	if (!(source_auth == target_auth)) {
		throw NotImplementedException("Moving \"%s\" to \"%s\" server-side is not supported: the two paths resolve to "
		                              "different S3 settings or credentials",
		                              source, target);
	}
	if (source_auth.GetProvider().GetType() == S3ProviderType::GCS) {
		throw NotImplementedException("Moving \"%s\" server-side is not supported on GCS", source);
	}
	if (source_auth.GetRequestOptions().sse_customer_key) {
		throw NotImplementedException("Moving \"%s\" server-side is not supported with SSE-C", source);
	}
	auto parsed_source = S3Url::Parse(source, source_auth);
	auto parsed_target = S3Url::Parse(target, target_auth);
	if (parsed_source.GetObjectVersion().IsSet() || parsed_target.GetObjectVersion().IsSet()) {
		throw NotImplementedException("Moving \"%s\" to \"%s\" is not supported: a path names an object version",
		                              source, target);
	}
	if (parsed_source.GetBucket() == parsed_target.GetBucket() && parsed_source.GetKey() == parsed_target.GetKey()) {
		return;
	}

	// the size decides whether one CopyObject can copy the source, and the ETag makes the copy fail if the source
	// is replaced before it
	auto source_session = S3RequestExecutor::CreateSession(opener, source_info.file, source_auth);
	auto head_result = HeadRequest(*source_session, source);
	auto &head = *head_result.response;
	if (head.HasRequestError()) {
		throw IOException("S3 request checking \"%s\" could not be completed: %s", head_result.context.display_url,
		                  head.GetRequestError());
	}
	if (head.status != HTTPStatusCode::OK_200) {
		throw S3RequestUtil::GetRequestError(head_result.context, head);
	}
	uint64_t source_size;
	if (!head.HasHeader("Content-Length") ||
	    !TryCast::Operation(string_t(head.GetHeaderValue("Content-Length")), source_size, true)) {
		throw IOException("S3 response checking \"%s\" has no valid Content-Length", head_result.context.display_url);
	}
	if (source_size > MAXIMUM_COPY_OBJECT_SIZE) {
		throw NotImplementedException("Moving \"%s\" server-side is not supported: it is larger than one CopyObject "
		                              "copies (%llu bytes)",
		                              source, MAXIMUM_COPY_OBJECT_SIZE);
	}
	S3CopySource copy_source;
	copy_source.source = "/" + S3Url::Encode(parsed_source.GetBucket(), S3URLEncodeMode::PATH) + "/" +
	                     S3Url::Encode(parsed_source.GetKey(), S3URLEncodeMode::PATH);
	if (head.HasHeader("ETag")) {
		copy_source.if_match = head.GetHeaderValue("ETag");
	}

	auto target_session = S3RequestExecutor::CreateSession(opener, target_info.file, target_auth);
	auto copy_result =
	    PutRequest(*target_session, S3RequestOperation::COPY_OBJECT, target, nullptr, 0, S3RequestQuery(), copy_source);
	auto &copy = *copy_result.response;
	auto &target_url = copy_result.context.display_url;
	if (copy.HasRequestError()) {
		throw IOException("S3 copy from \"%s\" to \"%s\" has an unknown outcome because the response was not received; "
		                  "the source was not deleted: %s",
		                  source, target_url, copy.GetRequestError());
	}
	if (copy.status != HTTPStatusCode::OK_200) {
		throw S3RequestUtil::GetRequestError(copy_result.context, copy);
	}
	// CopyObject can fail after S3 sent the 200 status, with the error in the body: only a CopyObjectResult shows
	// that the copy was made
	S3XMLResponse parsed;
	if (!S3XMLResponseParser::TryParse(copy.body, parsed) || parsed.type != S3XMLResponseType::COPY_OBJECT_RESULT) {
		if (parsed.type == S3XMLResponseType::ERROR) {
			throw IOException("S3 copy from \"%s\" to \"%s\" failed: %s%s%s; the source was not deleted", source,
			                  target_url, parsed.error_code.empty() ? "S3 returned an embedded error" : parsed.error_code,
			                  parsed.error_message.empty() ? "" : ": ", parsed.error_message);
		}
		throw IOException("S3 copy from \"%s\" to \"%s\" returned no CopyObjectResult, so its outcome is unknown; the "
		                  "source was not deleted",
		                  source, target_url);
	}
	try {
		RemoveFile(source, opener);
	} catch (std::exception &ex) {
		throw IOException("S3 copy from \"%s\" to \"%s\" succeeded, but deleting the source failed, so both objects "
		                  "exist: %s",
		                  source, target_url, ErrorData(ex).RawMessage());
	}
}

} // namespace duckdb
