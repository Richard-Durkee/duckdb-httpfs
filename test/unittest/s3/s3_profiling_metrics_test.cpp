#include "catch.hpp"

#include "s3/mock_s3_server.hpp"
#include "s3/s3_test_helper.hpp"

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static void ConfigureProfilingTest(Connection &con, MockS3Server &server, const string &client) {
	S3TestHelper::RequireQueryOk(con, "SET httpfs_client_implementation='" + client + "'");
	S3TestHelper::RequireQueryOk(con, "SET http_retries=3");
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	S3TestHelper::RequireQueryOk(con, StringUtil::Format(R"(
CREATE OR REPLACE SECRET profiling_s3 (
	TYPE S3,
	PROVIDER CONFIG,
	SCOPE 's3://refresh-bucket/',
	KEY_ID 'FRESH_KEY',
	SECRET 'S3TestHelper::FRESH_SECRET',
	REGION 'us-east-1',
	ENDPOINT '%s',
	USE_SSL false,
	URL_STYLE 'path'
))",
	                                                     server.Endpoint()));
	S3TestHelper::RequireQueryOk(con, "SET enable_profiling='no_output'");
}

//! The profile of the query that ran last. Reading a metric runs a query itself, so the profile is taken only once.
struct QueryProfile {
	explicit QueryProfile(Connection &con_p)
	    : con(con_p), json(con_p.GetProfilingInformation(ProfilerPrintFormat::JSON())) {
	}

	//! The value of a metric, zero when the profile does not mention it
	double Metric(const string &metric) {
		auto result = con.Query("SELECT coalesce(json_extract_string($profile$" + json + "$profile$::JSON, '$." +
		                        metric + "'), '0')::DOUBLE");
		REQUIRE(result);
		INFO((result->HasError() ? result->GetError() : string()));
		REQUIRE_FALSE(result->HasError());
		return result->Collection().GetValue(0, 0).GetValue<double>();
	}

	Connection &con;
	string json;
};

TEST_CASE("Profile metrics count every attempt of a read, retries included", "[httpfs][s3][profiling]") {
	for (const string client : {"curl", "httplib"}) {
		DYNAMIC_SECTION(client) {
			MockS3ServerConfig config;
			config.failures.transient_get_failures = 1;
			MockS3Server server(config);
			DuckDB db(nullptr);
			Connection con(db);
			S3TestHelper::LoadExtension(db);
			ConfigureProfilingTest(con, server, client);

			auto result = con.Query("SELECT length(content) FROM read_text('s3://refresh-bucket/object.bin')");
			REQUIRE(result);
			INFO((result->HasError() ? result->GetError() : string()));
			REQUIRE_FALSE(result->HasError());
			QueryProfile profile(con);
			auto observations = server.Observations();
			INFO(MockS3DescribeObservations(observations));

			// Every attempt that reached the server is a request, the failed GET included, and it is retried once
			REQUIRE(profile.Metric("httpfs.requests") == observations.size());
			REQUIRE(profile.Metric("httpfs.retries") == 1);
			REQUIRE(profile.Metric("httpfs.request_time") > 0);
		}
	}
}

TEST_CASE("Profile metrics count the requests of a write", "[httpfs][s3][profiling]") {
	for (const string client : {"curl", "httplib"}) {
		DYNAMIC_SECTION(client) {
			MockS3ServerConfig config;
			MockS3Server server(config);
			DuckDB db(nullptr);
			Connection con(db);
			S3TestHelper::LoadExtension(db);
			ConfigureProfilingTest(con, server, client);

			auto write = con.Query("COPY (SELECT 42 AS value) TO 's3://refresh-bucket/object.bin' (FORMAT CSV)");
			REQUIRE(write);
			INFO((write->HasError() ? write->GetError() : string()));
			REQUIRE_FALSE(write->HasError());
			QueryProfile profile(con);
			auto observations = server.Observations();
			INFO(MockS3DescribeObservations(observations));
			REQUIRE(profile.Metric("httpfs.requests") == observations.size());
			REQUIRE(profile.Metric("httpfs.retries") == 0);
			REQUIRE(profile.Metric("httpfs.request_time") > 0);
		}
	}
}

TEST_CASE("Profile metrics reach a query whose connection created its HTTP state during the query",
          "[httpfs][s3][profiling]") {
	MockS3ServerConfig config;
	MockS3Server server(config);
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	// Fresh connections: the HTTP state does not exist yet when their first, profiled, query begins
	for (idx_t i = 0; i < 2; i++) {
		Connection con(db);
		ConfigureProfilingTest(con, server, i == 0 ? "curl" : "httplib");
		auto before = server.Observations().size();
		// The profile belongs to the query's result, so it has to stay alive
		auto result = con.Query("SELECT length(content) FROM read_text('s3://refresh-bucket/object.bin')");
		REQUIRE(result);
		REQUIRE_FALSE(result->HasError());
		auto requests = server.Observations().size() - before;
		REQUIRE(requests > 0);
		QueryProfile profile(con);
		REQUIRE(profile.Metric("httpfs.requests") == requests);
	}
}

TEST_CASE("A file handle may outlive the connection that opened it", "[httpfs][s3][profiling]") {
	MockS3ServerConfig config;
	MockS3Server server(config);
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);

	unique_ptr<FileHandle> handle;
	{
		Connection con(db);
		ConfigureProfilingTest(con, server, "curl");
		S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
		auto &fs = FileSystem::GetFileSystem(*con.context);
		handle = fs.OpenFile(S3TestHelper::S3_PATH, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
		S3TestHelper::RequireQueryOk(con, "ROLLBACK");
	}
	// The state of the closed connection must not point at it any more
	string buffer(9, '\0');
	handle->Read(&buffer[0], buffer.size(), 7);
	REQUIRE(buffer == server.ObjectData().substr(7, buffer.size()));
}

} // namespace duckdb
