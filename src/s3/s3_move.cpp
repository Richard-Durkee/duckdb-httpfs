#include "s3/s3fs.hpp"

#include "s3/s3_request.hpp"
#include "s3/s3_url.hpp"
#include "s3/s3_xml_response.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

S3RequestResult S3FileSystem::CopyObjectRequest(HTTPRequestSession &session, const string &target_url,
                                                const string &copy_source) {
	S3RequestSpec spec {target_url, S3RequestOperation::COPY_OBJECT, {}, "", "", ""};
	spec.copy_source = copy_source;
	const string content_type = "application/octet-stream";
	return S3RequestExecutor::RunSession(GetEncryptionUtil(), session, spec, [&](S3RequestData &request_data) {
		auto &params = request_data.http_params->Cast<HTTPFSParams>();
		return RunPutRequest(request_data.http_url, request_data.headers, params, nullptr, 0, content_type,
		                     [&](BaseRequest &request) {
			                     request.try_request = true;
			                     return S3RequestExecutor::SendSessionRequest(session, request_data, request);
		                     });
	});
}

//! S3 copies at most 5 GiB in one CopyObject; a larger source needs UploadPartCopy
static bool IsCopySourceTooLarge(const S3XMLError &error) {
	return error.code == "InvalidRequest" &&
	       StringUtil::Contains(StringUtil::Lower(error.message), "copy source is larger than the maximum");
}

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
	if (parsed_source.GetObjectVersion().IsSet()) {
		throw NotImplementedException("Moving a version of \"%s\" is not supported", source);
	}
	auto parsed_target = S3Url::Parse(target, target_auth);
	if (parsed_source.GetBucket() == parsed_target.GetBucket() && parsed_source.GetKey() == parsed_target.GetKey()) {
		return;
	}

	auto copy_source = "/" + S3Url::Encode(parsed_source.GetBucket(), S3URLEncodeMode::PATH) + "/" +
	                   S3Url::Encode(parsed_source.GetKey(), S3URLEncodeMode::PATH);
	auto session = S3RequestExecutor::CreateSession(opener, target_info.file, target_auth);
	auto request_result = CopyObjectRequest(*session, target, copy_source);
	auto &response = *request_result.response;
	if (response.HasRequestError()) {
		throw IOException("S3 copy request from \"%s\" to \"%s\" could not be completed: %s", source,
		                  request_result.context.display_url, response.GetRequestError());
	}
	S3XMLError error;
	auto has_error_document = S3XMLResponseParser::TryParseError(response.body, error) && !error.code.empty();
	if (response.status == HTTPStatusCode::BadRequest_400 && has_error_document && IsCopySourceTooLarge(error)) {
		throw NotImplementedException("Moving \"%s\" server-side is not supported: %s", source, error.message);
	}
	if (response.status != HTTPStatusCode::OK_200) {
		throw S3RequestUtil::GetRequestError(request_result.context, response);
	}
	// CopyObject can fail after S3 sent the 200 status, with the error in the body
	if (has_error_document) {
		throw IOException("S3 copy from \"%s\" to \"%s\" failed: %s: %s", source, request_result.context.display_url,
		                  error.code, error.message);
	}
	RemoveFile(source, opener);
}

} // namespace duckdb
