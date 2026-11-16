#include "catch.hpp"

#include "s3/mock_s3_server.hpp"
#include "s3/s3_test_helper.hpp"

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "test_helpers.hpp"

#include <cstdlib>
#include <cstring>

namespace duckdb {

namespace {

static constexpr const char *SOURCE_PATH = "s3://refresh-bucket/source dir/a.bin";
static constexpr const char *ENCODED_COPY_SOURCE = "/refresh-bucket/source%20dir/a.bin";

static void ConfigureMockClient(Connection &con, const string &endpoint) {
	S3TestHelper::RequireQueryOk(con, "SET httpfs_client_implementation='httplib'");
	S3TestHelper::RequireQueryOk(con, "SET httpfs_connection_caching=false");
	S3TestHelper::RequireQueryOk(con, "SET enable_global_s3_configuration=false");
	S3TestHelper::RequireQueryOk(con, StringUtil::Format(R"(
CREATE OR REPLACE SECRET move_secret (
	TYPE S3,
	SCOPE 's3://refresh-bucket',
	KEY_ID 'MOVE_TEST_KEY',
	SECRET 'MOVE_TEST_SECRET',
	REGION 'us-east-1',
	ENDPOINT '%s',
	USE_SSL false,
	URL_STYLE 'path'
))",
	                                                     endpoint));
}

//! File system calls resolve their secret in the connection's transaction
static void BeginTransaction(Connection &con) {
	S3TestHelper::RequireQueryOk(con, "BEGIN");
}

template <class CALLBACK>
static string ExceptionMessage(CALLBACK callback) {
	try {
		callback();
	} catch (std::exception &ex) {
		ErrorData error(ex);
		return error.RawMessage();
	}
	return string();
}

template <class CALLBACK>
static ExceptionType ExceptionKind(CALLBACK callback) {
	try {
		callback();
	} catch (std::exception &ex) {
		return ErrorData(ex).Type();
	}
	return ExceptionType::INVALID;
}

static vector<string> SignedHeaders(const MockS3RequestObservation &observation) {
	static constexpr const char *PREFIX = "SignedHeaders=";
	auto position = observation.authorization.find(PREFIX);
	if (position == string::npos) {
		return {};
	}
	position += strlen(PREFIX);
	auto end = observation.authorization.find(',', position);
	return StringUtil::Split(observation.authorization.substr(position, end - position), ";");
}

static vector<MockS3RequestObservation> Requests(const MockS3Server &server, const string &method) {
	vector<MockS3RequestObservation> result;
	for (auto &observation : server.Observations()) {
		if (observation.method == method) {
			result.push_back(observation);
		}
	}
	return result;
}

static MockS3ServerConfig CopyResponseConfig(int status, const string &body) {
	MockS3ServerConfig config;
	config.upload.object_put.status = status;
	config.http_response.object_put_body = body;
	return config;
}

} // namespace

TEST_CASE("S3 MoveFile sends a signed CopyObject and keeps the source when the copy fails", "[httpfs][s3][move]") {
	// S3 can answer CopyObject with 200 and report the failure in the body
	MockS3Server server(CopyResponseConfig(
	    200, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InternalError</Code><Message>copy failed "
	         "midway</Message></Error>"));
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	ConfigureMockClient(con, server.Endpoint());
	BeginTransaction(con);
	auto &fs = FileSystem::GetFileSystem(*con.context);

	auto message = ExceptionMessage([&]() { fs.MoveFile(SOURCE_PATH, S3TestHelper::S3_PATH); });
	INFO(message);
	REQUIRE(StringUtil::Contains(message, "InternalError: copy failed midway"));

	auto puts = Requests(server, "PUT");
	REQUIRE(puts.size() == 1);
	auto copy_source = MockS3HeaderValues(puts[0], "x-amz-copy-source");
	REQUIRE(copy_source.size() == 1);
	REQUIRE(copy_source[0] == ENCODED_COPY_SOURCE);
	auto signed_headers = SignedHeaders(puts[0]);
	REQUIRE(std::find(signed_headers.begin(), signed_headers.end(), "x-amz-copy-source") != signed_headers.end());
	REQUIRE(puts[0].body_size == 0);
	REQUIRE(Requests(server, "DELETE").empty());
}

TEST_CASE("S3 MoveFile reports a missing source and deletes nothing", "[httpfs][s3][move]") {
	MockS3Server server(CopyResponseConfig(
	    404, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code><Message>The specified key does "
	         "not exist.</Message></Error>"));
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	ConfigureMockClient(con, server.Endpoint());
	BeginTransaction(con);
	auto &fs = FileSystem::GetFileSystem(*con.context);

	auto message = ExceptionMessage([&]() { fs.MoveFile(SOURCE_PATH, S3TestHelper::S3_PATH); });
	INFO(message);
	REQUIRE(StringUtil::Contains(message, "NoSuchKey"));
	REQUIRE(Requests(server, "DELETE").empty());
}

TEST_CASE("S3 MoveFile leaves a source too large for CopyObject to the caller", "[httpfs][s3][move]") {
	MockS3Server server(CopyResponseConfig(
	    400, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidRequest</Code><Message>The specified copy "
	         "source is larger than the maximum allowable size for a copy source: 5368709120</Message></Error>"));
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	ConfigureMockClient(con, server.Endpoint());
	BeginTransaction(con);
	auto &fs = FileSystem::GetFileSystem(*con.context);

	REQUIRE(ExceptionKind([&]() { fs.MoveFile(SOURCE_PATH, S3TestHelper::S3_PATH); }) ==
	        ExceptionType::NOT_IMPLEMENTED);
	REQUIRE(Requests(server, "DELETE").empty());
}

TEST_CASE("S3 MoveFile refuses moves it can not do server-side before sending anything", "[httpfs][s3][move]") {
	MockS3Server server(MockS3ServerConfig {});
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	ConfigureMockClient(con, server.Endpoint());
	auto &fs = FileSystem::GetFileSystem(*con.context);

	SECTION("a target that is not on S3") {
		BeginTransaction(con);
		REQUIRE(ExceptionKind([&]() { fs.MoveFile(SOURCE_PATH, TestCreatePath("move_target.bin")); }) ==
		        ExceptionType::NOT_IMPLEMENTED);
	}
	SECTION("a target that resolves to another secret") {
		S3TestHelper::RequireQueryOk(con, StringUtil::Format(R"(
CREATE OR REPLACE SECRET move_other_secret (
	TYPE S3,
	SCOPE 's3://refresh-bucket/other',
	KEY_ID 'OTHER_KEY',
	SECRET 'OTHER_SECRET',
	REGION 'us-east-1',
	ENDPOINT '%s',
	USE_SSL false,
	URL_STYLE 'path'
))",
		                                                     server.Endpoint()));
		BeginTransaction(con);
		REQUIRE(ExceptionKind([&]() { fs.MoveFile(SOURCE_PATH, "s3://refresh-bucket/other/b.bin"); }) ==
		        ExceptionType::NOT_IMPLEMENTED);
	}
	SECTION("the same object is left alone") {
		BeginTransaction(con);
		fs.MoveFile(S3TestHelper::S3_PATH, S3TestHelper::S3_PATH);
	}
	REQUIRE(Requests(server, "PUT").empty());
	REQUIRE(Requests(server, "DELETE").empty());
}

// Against the S3 test server (scripts/set_s3_test_server_variables.sh); skipped without it
TEST_CASE("S3 MoveFile moves objects on the S3 test server", "[httpfs][s3][move][s3-test-server]") {
	if (!std::getenv("S3_TEST_SERVER_AVAILABLE")) {
		return;
	}
	string endpoint = std::getenv("DUCKDB_S3_ENDPOINT");
	string use_ssl = std::getenv("DUCKDB_S3_USE_SSL");
	string key_id = std::getenv("AWS_ACCESS_KEY_ID");
	string secret = std::getenv("AWS_SECRET_ACCESS_KEY");
	string region = std::getenv("AWS_DEFAULT_REGION");

	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	S3TestHelper::RequireQueryOk(con, "SET enable_global_s3_configuration=false");
	S3TestHelper::RequireQueryOk(con, StringUtil::Format(R"(
CREATE SECRET move_test_server (
	TYPE S3, KEY_ID '%s', SECRET '%s', REGION '%s', ENDPOINT '%s', USE_SSL %s, URL_STYLE 'path'
))",
	                                                     key_id, secret, region, endpoint, use_ssl));
	BeginTransaction(con);
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto prefix = "s3://test-bucket/move-file/" + UUID::ToString(UUID::GenerateRandomUUID());

	SECTION("a parquet file within a bucket, to a key that needs encoding") {
		auto source = prefix + "/staged/part 1.parquet";
		auto target = prefix + "/k=a+b/part 1.parquet";
		S3TestHelper::RequireQueryOk(
		    con, StringUtil::Format("COPY (SELECT range AS i FROM range(1000)) TO '%s' (FORMAT parquet)", source));
		fs.MoveFile(source, target);
		REQUIRE_FALSE(fs.FileExists(source));
		auto result = con.Query(StringUtil::Format("SELECT count(*), sum(i) FROM read_parquet('%s')", target));
		REQUIRE_FALSE(result->HasError());
		REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 1000);
		REQUIRE(result->Collection().GetValue(1, 0).GetValue<int64_t>() == 499500);
	}
	SECTION("across buckets") {
		auto source = prefix + "/a.csv";
		auto target = StringUtil::Replace(prefix, "test-bucket", "test-bucket-2") + "/a.csv";
		S3TestHelper::RequireQueryOk(con, StringUtil::Format("COPY (SELECT 42 AS x) TO '%s'", source));
		fs.MoveFile(source, target);
		REQUIRE_FALSE(fs.FileExists(source));
		REQUIRE(fs.FileExists(target));
		fs.RemoveFile(target);
	}
	SECTION("a missing source") {
		auto message = ExceptionMessage([&]() { fs.MoveFile(prefix + "/missing.bin", prefix + "/target.bin"); });
		INFO(message);
		REQUIRE((StringUtil::Contains(message, "404") || StringUtil::Contains(message, "NoSuchKey")));
		REQUIRE_FALSE(fs.FileExists(prefix + "/target.bin"));
	}
}

} // namespace duckdb
