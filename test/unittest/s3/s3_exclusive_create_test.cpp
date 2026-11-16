#include "catch.hpp"

#include "s3/mock_s3_server.hpp"
#include "s3/s3_test_helper.hpp"

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdlib>

namespace duckdb {

namespace {

static constexpr const char *KEY_ID = "EXCLUSIVE_TEST_KEY";
static constexpr const char *PATH = "s3://exclusive-bucket/k=1/a.bin";

static MockS3ServerConfig ExclusiveConfig() {
	MockS3ServerConfig config;
	config.object.bucket = "exclusive-bucket";
	config.object.key = "k=1/a.bin";
	config.upload.md5_etags = true;
	return config;
}

static void Configure(Connection &con, const string &endpoint, const string &extra_options = "") {
	S3TestHelper::RequireQueryOk(con, "SET httpfs_client_implementation='httplib'");
	S3TestHelper::RequireQueryOk(con, "SET httpfs_connection_caching=false");
	S3TestHelper::RequireQueryOk(con, "SET enable_global_s3_configuration=false");
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	S3TestHelper::RequireQueryOk(con, StringUtil::Format(R"(
CREATE OR REPLACE SECRET exclusive_secret (
	TYPE S3,
	KEY_ID '%s',
	SECRET 'EXCLUSIVE_TEST_SECRET',
	REGION 'us-east-1',
	ENDPOINT '%s',
	USE_SSL false,
	URL_STYLE 'path'%s
))",
	                                                     KEY_ID, endpoint, extra_options));
	S3TestHelper::RequireQueryOk(con, "BEGIN");
}

enum class Payload : uint8_t { SINGLE_PUT, MULTIPART };

static ErrorData WriteFile(Connection &con, const string &path, Payload payload, bool exclusive = true) {
	try {
		if (payload == Payload::MULTIPART) {
			S3TestHelper::RequireQueryOk(con, "SET s3_uploader_max_filesize='50GB'");
		}
		auto &fs = FileSystem::GetFileSystem(*con.context);
		auto flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW;
		if (exclusive) {
			flags |= FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE;
		}
		auto handle = fs.OpenFile(path, flags);
		string data = payload == Payload::MULTIPART ? string(10 * 1024 * 1024 + 1, 'x') : "exclusive payload";
		handle->Write(QueryContext(*con.context), &data[0], data.size());
		handle->Close();
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	return ErrorData();
}

//! The request that creates the object: the single PUT, or the multipart completion
static vector<MockS3RequestObservation> CreatingRequests(const MockS3Server &server) {
	auto observations = server.Observations();
	vector<MockS3RequestObservation> result;
	std::copy_if(observations.begin(), observations.end(), std::back_inserter(result),
	             [](const MockS3RequestObservation &observation) {
		             return (observation.method == "PUT" && !observation.part_number.IsValid()) ||
		                    (observation.method == "POST" && !observation.upload_id.empty());
	             });
	return result;
}

static void RequireObjectExists(const ErrorData &error) {
	INFO(error.RawMessage());
	REQUIRE(error.HasError());
	REQUIRE(error.Type() == ExceptionType::IO);
	REQUIRE(StringUtil::Contains(error.RawMessage(), "the object already exists"));
	auto errno_entry = error.ExtraInfo().find("errno");
	REQUIRE(errno_entry != error.ExtraInfo().end());
	REQUIRE(errno_entry->second == std::to_string(EEXIST));
}

} // namespace

TEST_CASE("S3 exclusive create sends If-None-Match on the request that creates the object",
          "[httpfs][s3][exclusive-create]") {
	for (auto payload : {Payload::SINGLE_PUT, Payload::MULTIPART}) {
		for (auto exclusive : {true, false}) {
			DYNAMIC_SECTION((payload == Payload::MULTIPART ? "multipart" : "single PUT")
			                << (exclusive ? " exclusive" : " not exclusive")) {
				MockS3Server server(ExclusiveConfig());
				DuckDB db(nullptr);
				S3TestHelper::LoadExtension(db);
				Connection con(db);
				Configure(con, server.Endpoint());
				auto error = WriteFile(con, PATH, payload, exclusive);
				INFO(error.RawMessage());
				REQUIRE_FALSE(error.HasError());
				auto creating = CreatingRequests(server);
				REQUIRE(creating.size() == 1);
				auto values = MockS3HeaderValues(creating[0], "If-None-Match");
				if (exclusive) {
					REQUIRE(values == vector<string> {"*"});
				} else {
					REQUIRE(values.empty());
				}
				// parts are never conditional
				for (auto &observation : server.Observations()) {
					if (observation.part_number.IsValid()) {
						REQUIRE(MockS3HeaderValues(observation, "If-None-Match").empty());
					}
				}
			}
		}
	}
}

TEST_CASE("S3 exclusive create of an existing object fails as an existing file does",
          "[httpfs][s3][exclusive-create]") {
	for (auto payload : {Payload::SINGLE_PUT, Payload::MULTIPART}) {
		DYNAMIC_SECTION((payload == Payload::MULTIPART ? "multipart" : "single PUT")) {
			auto config = ExclusiveConfig();
			config.upload.object_exists = true;
			MockS3Server server(config);
			DuckDB db(nullptr);
			S3TestHelper::LoadExtension(db);
			Connection con(db);
			Configure(con, server.Endpoint());
			RequireObjectExists(WriteFile(con, PATH, payload));
			if (payload == Payload::MULTIPART) {
				// the upload is aborted once, by the check that it is not the one that created the object
				REQUIRE(S3TestHelper::CountObservations(server.Observations(), "DELETE", KEY_ID, 204) == 1);
				REQUIRE(S3TestHelper::CountObservations(server.Observations(), "DELETE", KEY_ID, 404) == 0);
			}
		}
	}
}

TEST_CASE("S3 exclusive create succeeds when a retry finds the object its first attempt created",
          "[httpfs][s3][exclusive-create]") {
	for (auto payload : {Payload::SINGLE_PUT, Payload::MULTIPART}) {
		DYNAMIC_SECTION((payload == Payload::MULTIPART ? "multipart" : "single PUT")) {
			auto config = ExclusiveConfig();
			config.upload.create_then_precondition_failed = true;
			MockS3Server server(config);
			DuckDB db(nullptr);
			S3TestHelper::LoadExtension(db);
			Connection con(db);
			Configure(con, server.Endpoint());
			auto error = WriteFile(con, PATH, payload);
			INFO(error.RawMessage());
			REQUIRE_FALSE(error.HasError());
			REQUIRE(S3TestHelper::CountObservations(server.Observations(), "HEAD", KEY_ID, 200) == 1);
		}
	}
}

TEST_CASE("S3 exclusive create retries a PUT that races another conditional write", "[httpfs][s3][exclusive-create]") {
	auto config = ExclusiveConfig();
	config.failures.transient_put_failures = 1;
	config.failures.transient_put_status = 409;
	config.failures.transient_put_code = "ConditionalRequestConflict";
	MockS3Server server(config);
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	Configure(con, server.Endpoint());
	auto error = WriteFile(con, PATH, Payload::SINGLE_PUT);
	INFO(error.RawMessage());
	REQUIRE_FALSE(error.HasError());
	REQUIRE(S3TestHelper::CountObservations(server.Observations(), "PUT", KEY_ID, 409) == 1);
	REQUIRE(S3TestHelper::CountObservations(server.Observations(), "PUT", KEY_ID, 200) == 1);
}

TEST_CASE("S3 exclusive create reports a multipart completion that races another conditional write",
          "[httpfs][s3][exclusive-create]") {
	auto config = ExclusiveConfig();
	config.failures.completion_fault.count = 1;
	config.failures.completion_fault.status = 409;
	config.failures.completion_fault.code = "ConditionalRequestConflict";
	MockS3Server server(config);
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	Configure(con, server.Endpoint());
	auto error = WriteFile(con, PATH, Payload::MULTIPART);
	INFO(error.RawMessage());
	REQUIRE(error.HasError());
	REQUIRE(StringUtil::Contains(error.RawMessage(), "conflicted with a concurrent write"));
	// the completion is not sent again, and the upload is aborted
	REQUIRE(S3TestHelper::CountObservations(server.Observations(), "POST", KEY_ID, 409) == 1);
	REQUIRE(S3TestHelper::CountObservations(server.Observations(), "DELETE", KEY_ID, 204) == 1);
}

TEST_CASE("S3 exclusive create reports another writer's object with the same name", "[httpfs][s3][exclusive-create]") {
	// the upload completed, but the object's ETag is not this upload's
	auto config = ExclusiveConfig();
	config.upload.create_then_precondition_failed = true;
	config.upload.md5_etags = false;
	MockS3Server server(config);
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	Configure(con, server.Endpoint());
	RequireObjectExists(WriteFile(con, PATH, Payload::SINGLE_PUT));
}

TEST_CASE("S3 exclusive create with SSE-KMS reports an unknown outcome", "[httpfs][s3][exclusive-create]") {
	auto config = ExclusiveConfig();
	config.upload.create_then_precondition_failed = true;
	MockS3Server server(config);
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	Configure(con, server.Endpoint(), ", KMS_KEY_ID 'exclusive-kms-key'");
	auto error = WriteFile(con, PATH, Payload::SINGLE_PUT);
	INFO(error.RawMessage());
	REQUIRE(error.HasError());
	REQUIRE(StringUtil::Contains(error.RawMessage(), "unknown outcome"));
}

TEST_CASE("S3 exclusive create refuses what it can not do", "[httpfs][s3][exclusive-create]") {
	MockS3Server server(ExclusiveConfig());
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	auto &fs = FileSystem::GetFileSystem(*con.context);
	SECTION("on GCS") {
		S3TestHelper::RequireQueryOk(con, "SET enable_global_s3_configuration=false");
		S3TestHelper::RequireQueryOk(con, StringUtil::Format(R"(
CREATE SECRET exclusive_gcs (TYPE GCS, KEY_ID 'GCS_KEY', SECRET 'GCS_SECRET', ENDPOINT '%s', USE_SSL false, URL_STYLE 'path')
)",
		                                                     server.Endpoint()));
		S3TestHelper::RequireQueryOk(con, "BEGIN");
		REQUIRE_THROWS_AS(fs.OpenFile("gcs://exclusive-bucket/k=1/a.bin", FileFlags::FILE_FLAGS_WRITE |
		                                                                      FileFlags::FILE_FLAGS_FILE_CREATE_NEW |
		                                                                      FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE),
		                  NotImplementedException);
	}
	SECTION("returning no handle for an existing object") {
		Configure(con, server.Endpoint());
		REQUIRE_THROWS_AS(fs.OpenFile(PATH, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW |
		                                        FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE |
		                                        FileFlags::FILE_FLAGS_NULL_IF_EXISTS),
		                  NotImplementedException);
	}
	REQUIRE(server.Observations().empty());
}

// Against the S3 test server (scripts/set_s3_test_server_variables.sh); skipped without it
TEST_CASE("S3 exclusive create on the S3 test server", "[httpfs][s3][exclusive-create][s3-test-server]") {
	if (!std::getenv("S3_TEST_SERVER_AVAILABLE")) {
		return;
	}
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	S3TestHelper::RequireQueryOk(con, "SET enable_global_s3_configuration=false");
	S3TestHelper::RequireQueryOk(
	    con, StringUtil::Format(R"(
CREATE SECRET exclusive_test_server (
	TYPE S3, KEY_ID '%s', SECRET '%s', REGION '%s', ENDPOINT '%s', USE_SSL %s, URL_STYLE 'path'
))",
	                            std::getenv("AWS_ACCESS_KEY_ID"), std::getenv("AWS_SECRET_ACCESS_KEY"),
	                            std::getenv("AWS_DEFAULT_REGION"), std::getenv("DUCKDB_S3_ENDPOINT"),
	                            std::getenv("DUCKDB_S3_USE_SSL")));
	S3TestHelper::RequireQueryOk(con, "BEGIN");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	for (auto payload : {Payload::SINGLE_PUT, Payload::MULTIPART}) {
		auto path = "s3://test-bucket/exclusive-create/" + UUID::ToString(UUID::GenerateRandomUUID()) + ".bin";
		auto first = WriteFile(con, path, payload);
		INFO(first.RawMessage());
		REQUIRE_FALSE(first.HasError());
		RequireObjectExists(WriteFile(con, path, payload));
		REQUIRE_FALSE(WriteFile(con, path, payload, false).HasError());
		fs.RemoveFile(path);
	}
}

} // namespace duckdb
