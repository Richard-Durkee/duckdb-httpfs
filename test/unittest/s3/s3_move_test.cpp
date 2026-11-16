#include "catch.hpp"

#include "s3/mock_s3_server.hpp"
#include "s3/s3_test_helper.hpp"

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "test_helpers.hpp"

#include <algorithm>
#include <cstdlib>
#include <cstring>

namespace duckdb {

namespace {

static constexpr const char *KEY_ID = "MOVE_TEST_KEY";
static constexpr const char *SOURCE_BUCKET = "move-source";
static constexpr const char *SOURCE_KEY = "staged dir/a+b%c \xC3\xA9.bin";
static constexpr const char *SOURCE_PATH = "s3://move-source/staged dir/a+b%c \xC3\xA9.bin";
static constexpr const char *ENCODED_COPY_SOURCE = "/move-source/staged%20dir/a%2Bb%25c%20%C3%A9.bin";
static constexpr const char *TARGET_PATH = "s3://move-target/k=1/a.bin";
static constexpr const char *SOURCE_ETAG = "\"move-test-etag\"";
static constexpr const char *COPY_RESULT = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CopyObjectResult><LastModified>"
                                           "2026-11-16T00:00:00.000Z</LastModified><ETag>\"move-test-etag\"</ETag>"
                                           "</CopyObjectResult>";

//! The target is the server's object, which answers PUT; the source is an extra object, which answers HEAD and DELETE
static MockS3ServerConfig MoveConfig(int copy_status, const string &copy_body) {
	MockS3ServerConfig config;
	config.object.bucket = "move-target";
	config.object.key = "k=1/a.bin";
	MockS3ObjectConfig source;
	source.bucket = SOURCE_BUCKET;
	source.key = SOURCE_KEY;
	config.extra_objects.push_back(source);
	config.metadata.etag = SOURCE_ETAG;
	config.upload.object_put.status = copy_status;
	config.http_response.object_put_body = copy_body;
	return config;
}

static void ConfigureMockClient(Connection &con, const string &endpoint, const string &extra_options = "") {
	S3TestHelper::RequireQueryOk(con, "SET httpfs_client_implementation='httplib'");
	S3TestHelper::RequireQueryOk(con, "SET httpfs_connection_caching=false");
	S3TestHelper::RequireQueryOk(con, "SET enable_global_s3_configuration=false");
	S3TestHelper::RequireQueryOk(con, StringUtil::Format(R"(
CREATE OR REPLACE SECRET move_secret (
	TYPE S3,
	KEY_ID '%s',
	SECRET 'MOVE_TEST_SECRET',
	REGION 'us-east-1',
	ENDPOINT '%s',
	USE_SSL false,
	URL_STYLE 'path'%s
))",
	                                                     KEY_ID, endpoint, extra_options));
	// file system calls resolve their secret in the connection's transaction
	S3TestHelper::RequireQueryOk(con, "BEGIN");
}

static ErrorData MoveError(FileSystem &fs, const string &source, const string &target) {
	try {
		fs.MoveFile(source, target);
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	return ErrorData();
}

static vector<MockS3RequestObservation> CopyRequests(const MockS3Server &server) {
	auto observations = server.Observations();
	vector<MockS3RequestObservation> result;
	std::copy_if(observations.begin(), observations.end(), std::back_inserter(result),
	             [](const MockS3RequestObservation &observation) {
		             return observation.method == "PUT" &&
		                    !MockS3HeaderValues(observation, "x-amz-copy-source").empty();
	             });
	return result;
}

static bool SentDelete(const MockS3Server &server) {
	auto observations = server.Observations();
	return std::any_of(observations.begin(), observations.end(),
	                   [](const MockS3RequestObservation &observation) { return observation.method == "DELETE"; });
}

static string HeaderValue(const MockS3RequestObservation &observation, const string &name) {
	auto values = MockS3HeaderValues(observation, name);
	return values.size() == 1 ? values[0] : string();
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

static bool IsSigned(const MockS3RequestObservation &observation, const string &header) {
	auto signed_headers = SignedHeaders(observation);
	return std::find(signed_headers.begin(), signed_headers.end(), header) != signed_headers.end();
}

} // namespace

TEST_CASE("S3 MoveFile copies with a signed, conditional CopyObject and then deletes the source",
          "[httpfs][s3][move]") {
	MockS3Server server(MoveConfig(200, COPY_RESULT));
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	ConfigureMockClient(con, server.Endpoint(), ", KMS_KEY_ID 'move-kms-key', REQUESTER_PAYS true");
	auto &fs = FileSystem::GetFileSystem(*con.context);

	fs.MoveFile(SOURCE_PATH, TARGET_PATH);

	auto copies = CopyRequests(server);
	REQUIRE(copies.size() == 1);
	auto &copy = copies[0];
	REQUIRE(HeaderValue(copy, "x-amz-copy-source") == ENCODED_COPY_SOURCE);
	REQUIRE(HeaderValue(copy, "x-amz-copy-source-if-match") == SOURCE_ETAG);
	REQUIRE(copy.kms_key_id == "move-kms-key");
	REQUIRE(copy.server_side_encryption == "aws:kms");
	REQUIRE(HeaderValue(copy, "x-amz-request-payer") == "requester");
	for (auto header : {"x-amz-copy-source", "x-amz-copy-source-if-match", "x-amz-server-side-encryption",
	                    "x-amz-server-side-encryption-aws-kms-key-id", "x-amz-request-payer"}) {
		INFO(header);
		REQUIRE(IsSigned(copy, header));
	}
	REQUIRE(copy.body_size == 0);
	REQUIRE(S3TestHelper::CountObservations(server.Observations(), "HEAD", KEY_ID, 200) == 1);
	REQUIRE(S3TestHelper::CountObservations(server.Observations(), "DELETE", KEY_ID, 204) == 1);
}

TEST_CASE("S3 MoveFile keeps the source unless CopyObject returns a CopyObjectResult", "[httpfs][s3][move]") {
	string body;
	string expected;
	SECTION("an empty body") {
		expected = "returned no CopyObjectResult";
	}
	SECTION("a body that is not XML") {
		body = "<html><body>proxy error</body></html>";
		expected = "returned no CopyObjectResult";
	}
	SECTION("a truncated CopyObjectResult") {
		body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CopyObjectResult><ETag>\"move-test";
		expected = "returned no CopyObjectResult";
	}
	SECTION("an error document without a code") {
		body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code/></Error>";
		expected = "S3 returned an embedded error";
	}
	SECTION("an error document behind the 200 status") {
		body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>AccessDenied</Code><Message>copy refused"
		       "</Message></Error>";
		expected = "AccessDenied: copy refused";
	}
	MockS3Server server(MoveConfig(200, body));
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	ConfigureMockClient(con, server.Endpoint());
	auto &fs = FileSystem::GetFileSystem(*con.context);

	auto error = MoveError(fs, SOURCE_PATH, TARGET_PATH);
	INFO(error.RawMessage());
	REQUIRE(error.HasError());
	REQUIRE(StringUtil::Contains(error.RawMessage(), expected));
	REQUIRE(StringUtil::Contains(error.RawMessage(), "the source was not deleted"));
	REQUIRE(CopyRequests(server).size() == 1);
	REQUIRE_FALSE(SentDelete(server));
}

TEST_CASE("S3 MoveFile retries a throttled or failed CopyObject", "[httpfs][s3][move]") {
	auto config = MoveConfig(200, COPY_RESULT);
	config.failures.transient_put_failures = 1;
	SECTION("SlowDown behind the 200 status") {
		config.failures.transient_put_status = 200;
		config.failures.transient_put_code = "SlowDown";
	}
	SECTION("InternalError behind the 200 status") {
		config.failures.transient_put_status = 200;
		config.failures.transient_put_code = "InternalError";
	}
	SECTION("a 500") {
		config.failures.transient_put_status = 500;
		config.failures.transient_put_code = "InternalError";
	}
	MockS3Server server(config);
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	ConfigureMockClient(con, server.Endpoint());
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	auto &fs = FileSystem::GetFileSystem(*con.context);

	fs.MoveFile(SOURCE_PATH, TARGET_PATH);
	REQUIRE(CopyRequests(server).size() == 2);
	REQUIRE(S3TestHelper::CountObservations(server.Observations(), "DELETE", KEY_ID, 204) == 1);
}

TEST_CASE("S3 MoveFile reports a source it copied but could not delete", "[httpfs][s3][move]") {
	auto config = MoveConfig(200, COPY_RESULT);
	config.extra_objects[0].delete_status = 403;
	config.extra_objects[0].delete_error_code = "AccessDenied";
	MockS3Server server(config);
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	ConfigureMockClient(con, server.Endpoint());
	auto &fs = FileSystem::GetFileSystem(*con.context);

	auto error = MoveError(fs, SOURCE_PATH, TARGET_PATH);
	INFO(error.RawMessage());
	REQUIRE(error.HasError());
	REQUIRE(
	    StringUtil::Contains(error.RawMessage(), "succeeded, but deleting the source failed, so both objects exist"));
	REQUIRE(CopyRequests(server).size() == 1);
}

TEST_CASE("S3 MoveFile checks the source before copying", "[httpfs][s3][move]") {
	auto config = MoveConfig(200, COPY_RESULT);
	SECTION("a missing source") {
		DuckDB db(nullptr);
		S3TestHelper::LoadExtension(db);
		Connection con(db);
		MockS3Server server(config);
		ConfigureMockClient(con, server.Endpoint());
		auto &fs = FileSystem::GetFileSystem(*con.context);
		auto error = MoveError(fs, "s3://move-source/missing.bin", TARGET_PATH);
		INFO(error.RawMessage());
		REQUIRE(error.HasError());
		REQUIRE(StringUtil::Contains(error.RawMessage(), "404"));
		REQUIRE(CopyRequests(server).empty());
		REQUIRE_FALSE(SentDelete(server));
	}
	SECTION("a source larger than one CopyObject copies is left to the caller") {
		config.metadata.head_content_length = (5ULL << 30ULL) + 1;
		DuckDB db(nullptr);
		S3TestHelper::LoadExtension(db);
		Connection con(db);
		MockS3Server server(config);
		ConfigureMockClient(con, server.Endpoint());
		auto &fs = FileSystem::GetFileSystem(*con.context);
		REQUIRE(MoveError(fs, SOURCE_PATH, TARGET_PATH).Type() == ExceptionType::NOT_IMPLEMENTED);
		REQUIRE(CopyRequests(server).empty());
		REQUIRE_FALSE(SentDelete(server));
	}
	SECTION("a source replaced since it was checked fails the precondition") {
		DuckDB db(nullptr);
		S3TestHelper::LoadExtension(db);
		Connection con(db);
		MockS3Server server(MoveConfig(412, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>"
		                                    "PreconditionFailed</Code><Message>At least one of the pre-conditions you "
		                                    "specified did not hold</Message></Error>"));
		ConfigureMockClient(con, server.Endpoint());
		auto &fs = FileSystem::GetFileSystem(*con.context);
		auto error = MoveError(fs, SOURCE_PATH, TARGET_PATH);
		INFO(error.RawMessage());
		REQUIRE(error.HasError());
		REQUIRE(StringUtil::Contains(error.RawMessage(), "412"));
		REQUIRE_FALSE(SentDelete(server));
	}
}

TEST_CASE("S3 MoveFile refuses moves it can not do server-side before sending anything", "[httpfs][s3][move]") {
	MockS3Server server(MoveConfig(200, COPY_RESULT));
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	auto &fs = FileSystem::GetFileSystem(*con.context);
	string source = SOURCE_PATH;
	string target;

	SECTION("a target that is not on S3") {
		ConfigureMockClient(con, server.Endpoint());
		target = TestCreatePath("move_target.bin");
	}
	SECTION("a target that resolves to another secret") {
		S3TestHelper::RequireQueryOk(con, StringUtil::Format(R"(
CREATE OR REPLACE SECRET move_other_secret (
	TYPE S3,
	SCOPE 's3://move-target/other',
	KEY_ID 'OTHER_KEY',
	SECRET 'OTHER_SECRET',
	REGION 'us-east-1',
	ENDPOINT '%s',
	USE_SSL false,
	URL_STYLE 'path'
))",
		                                                     server.Endpoint()));
		ConfigureMockClient(con, server.Endpoint());
		target = "s3://move-target/other/b.bin";
	}
	SECTION("a versioned target") {
		S3TestHelper::RequireQueryOk(con, "SET s3_enable_deprecated_url_parameters=true");
		ConfigureMockClient(con, server.Endpoint());
		target = string(TARGET_PATH) + "?s3_version_id=v1";
	}
	SECTION("the same object") {
		ConfigureMockClient(con, server.Endpoint());
		fs.MoveFile(SOURCE_PATH, SOURCE_PATH);
		REQUIRE(server.Observations().empty());
		return;
	}
	auto error = MoveError(fs, source, target);
	INFO(error.RawMessage());
	REQUIRE(error.Type() == ExceptionType::NOT_IMPLEMENTED);
	REQUIRE(server.Observations().empty());
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
	S3TestHelper::RequireQueryOk(con, "BEGIN");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto prefix = "s3://test-bucket/move-file/" + UUID::ToString(UUID::GenerateRandomUUID());

	SECTION("a parquet file within a bucket, to a key that needs encoding") {
		auto source = prefix + "/staged/part 1+%\xC3\xA9.parquet";
		auto target = prefix + "/k=a+b/part 1+%\xC3\xA9.parquet";
		S3TestHelper::RequireQueryOk(
		    con, StringUtil::Format("COPY (SELECT range AS i FROM range(1000)) TO '%s' (FORMAT parquet)", source));
		fs.MoveFile(source, target);
		REQUIRE_FALSE(fs.FileExists(source));
		auto result = con.Query(StringUtil::Format("SELECT count(*), sum(i) FROM read_parquet('%s')", target));
		REQUIRE_FALSE(result->HasError());
		REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 1000);
		REQUIRE(result->Collection().GetValue(1, 0).GetValue<int64_t>() == 499500);
		fs.RemoveFile(target);
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
		auto error = MoveError(fs, prefix + "/missing.bin", prefix + "/target.bin");
		INFO(error.RawMessage());
		REQUIRE(error.HasError());
		REQUIRE(StringUtil::Contains(error.RawMessage(), "404"));
		REQUIRE_FALSE(fs.FileExists(prefix + "/target.bin"));
	}
}

} // namespace duckdb
