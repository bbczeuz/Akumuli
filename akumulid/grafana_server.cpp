#include "grafana_server.h"
#include "utility.h"
#include <cstring>
#include <thread>
#include <iostream>

#include <boost/bind.hpp>
#include <boost/algorithm/string.hpp>

namespace Akumuli {
namespace Grafana {

//! Microhttpd callback functions
namespace MHD {
#if 1
static ssize_t read_callback(void *data, uint64_t pos, char *buf, size_t max)
{
    AKU_UNUSED(pos);
    ReadOperation* cur = (ReadOperation*)data;
    auto status = cur->get_error();
    if (status) {
        return MHD_CONTENT_READER_END_OF_STREAM;
    }
    size_t sz;
    bool is_done;
    std::tie(sz, is_done) = cur->read_some(buf, max);
    if (is_done) {
        return MHD_CONTENT_READER_END_OF_STREAM;
    } else {
        if (sz == 0u) {
            // Not at the end of the stream but data is not ready yet.
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    return sz;
}
#endif

#if 1
static void free_callback(void *data) {
    ReadOperation* cur = (ReadOperation*)data;
    cur->close();
    delete cur;
}
#endif

static int http_static_response(MHD_Connection *p_connection, const char *p_message, int p_response_code, const char *p_content_type = nullptr)
{
	assert(p_message);
	struct MHD_Response *response;
	response = MHD_create_response_from_buffer (std::strlen (p_message), (void*) p_message, MHD_RESPMEM_MUST_COPY);
	if (p_content_type != nullptr)
	{
		MHD_add_response_header(response, "Content-Type", p_content_type);
#if 0
		//Optional headers (InfluxDB 0.9.6.1 sets them)
		MHD_add_response_header(response, "Request-Id", "51dfed75-bcc0-11e5-8728-000000000000"); //FIXME: Use unique values
		MHD_add_response_header(response, "X-Influxdb-Version", "0.9.99"); //FIXME: Find valid value
		MHD_add_response_header(response, "Access-Control-Expose-Headers", "Date, X-Influxdb-Version");
		MHD_add_response_header(response, "Access-Control-Allow-Methods", "DELETE, GET, OPTIONS, POST, PUT");
		MHD_add_response_header(response, "Access-Control-Allow-Headers", "Accept, Accept-Encoding, Authorization, Content-Length, Content-Type, X-CSRF-Token, X-HTTP-Method-Override");
#endif
		const char *origin = MHD_lookup_connection_value(p_connection, MHD_HEADER_KIND, "Origin");
		if ((origin != nullptr) && ( origin[0] != 0))
		{
			MHD_add_response_header(response, "Access-Control-Allow-Origin", origin); // Grafana expects this header!
		}
	}
	int ret = MHD_queue_response (p_connection, p_response_code, response);
	MHD_destroy_response (response);
	return ret;
}

static std::string parse_show_measurements(MHD_Connection *p_connection, const std::vector<std::string> &p_tokens, const char *p_query)
{
	unsigned long nvals_requested = 0;
	if (p_tokens.size() == 2)
	{
		//Return all measurement names
		//TODO: Implement
		//const char *temporary_fix = "{\"results\":[{\"series\":[{\"name\":\"measurements\",\"columns\":[\"name\"],\"values\":[[\"aggregation_value\"],[\"chrony_value\"],[\"collectd_value\"],[\"cpu_value\"],[\"df_free\"],[\"df_used\"],[\"df_value\"],[\"wxt_\"],[\"wxt_value\"]]}]}]}";
		//return http_static_response(p_connection, temporary_fix, MHD_HTTP_OK, "application/json");
		return R"({ "output": { "format": "json" }, "select": "names" }";
	} else if ((p_tokens.size() == 4) && (boost::iequals(p_tokens[2],"limit")) && ((nvals_requested = std::stoul(p_tokens[3])) > 0))
	{
		//Request up to nvals_requested measurement names
		//TODO: Implement
		//const char *temporary_fix = "{\"results\":[{\"series\":[{\"name\":\"measurements\",\"columns\":[\"name\"],\"values\":[[\"aggregation_value\"]]}]}]}";
		//return http_static_response(p_connection, temporary_fix, MHD_HTTP_OK, "application/json");
		return R"({ "output": { "format": "json" }, "select": "names", "limit": )" + std::to_string(nvals_requested) + " }";
		//return "{ \"output\": { \"format\": \"json\" }, \"select\": \"names\", \"limit\": " + std::to_string(nvals_requested) + " }";

	} else {
		std::runtime_error err("Invalid query");
		BOOST_THROW_EXCEPTION(err);
	}
	assert(0); //Shouldn't arrive here
}

#if 0
static int parse_show_tag_keys(MHD_Connection *p_connection, const std::vector<std::string> &p_tokens, const char *p_query)
{
	//TODO: Implement
	const char *temporary_fix = "{\"results\":[{\"series\":[{\"name\":\"cpu_value\",\"columns\":[\"tagKey\"],\"values\":[[\"host\"],[\"instance\"],[\"type\"],[\"type_instance\"]]}]}]}";
	return http_static_response(p_connection, temporary_fix, MHD_HTTP_OK, "application/json");
}

static int parse_show_tag_values(MHD_Connection *p_connection, const std::vector<std::string> &p_tokens, const char *p_query)
{
	//TODO: Implement
	const char *temporary_fix = "{\"results\":[{\"series\":[{\"name\":\"hostTagValues\",\"columns\":[\"host\"],\"values\":[[\"www.example.com\"],[\"mail.example.com\"],[\"ftp.example.com\"],[\"wiki.example.com\"]]}]}]}";
	return http_static_response(p_connection, temporary_fix, MHD_HTTP_OK, "application/json");
}

static int parse_show_tag(MHD_Connection *p_connection, const std::vector<std::string> &p_tokens, const char *p_query)
{
	if ((p_tokens.size() >= 5) && (boost::iequals(p_tokens[3],"from")))
	{
		if (boost::iequals(p_tokens[2],"keys"))
		{
			//Example: SHOW TAG KEYS FROM "cpu_value"
			return parse_show_tag_keys(p_connection, p_tokens, p_query);
		} else if (boost::iequals(p_tokens[2],"values"))
		{
			//Example: SHOW TAG VALUES FROM "cpu_value" WITH KEY = "host"
			return parse_show_tag_values(p_connection, p_tokens, p_query);
		} else {
			return http_static_response(p_connection, "token[2]: expected (KEYS,VALUES) FROM <metric>", MHD_HTTP_BAD_REQUEST);
		}
	} else {
		return http_static_response(p_connection, "token[2]: expected KEYS/VALUES FROM", MHD_HTTP_BAD_REQUEST);
	}
}

static int parse_select(MHD_Connection *p_connection, const std::vector<std::string> &p_tokens, const char *p_query)
{
	//Example: SELECT mean("value") FROM "cpu_value" WHERE time > now() - 6h GROUP BY time(30s) fill(null)
	//TODO: Implement
	const char *temporary_fix = "{\"results\":[{\"series\":[{\"name\":\"cpu_value\",\"columns\":[\"time\",\"mean\"],\"values\":[[1452979800000,2.378373672222222e+08],[1452980700000,2.378835611111111e+08],[1452981600000,2.379613391111111e+08],[1453001400000,2.3967489180555555e+08]]}]}]}";
	return http_static_response(p_connection, temporary_fix, MHD_HTTP_OK, "application/json");
}
#endif


static std::string parse_query_string(MHD_Connection *p_connection, const char *p_query)
{
	std::string query{p_query};
	std::vector<std::string> tokens;
	boost::split(tokens, query, boost::algorithm::is_space(), boost::token_compress_on);
	//TODO: p_query could contain multiple semicolon separated queries

	if (tokens.empty() || tokens[0].empty())
	{
		std::runtime_error err("Empty query");
		BOOST_THROW_EXCEPTION(err);
	}
	if (boost::iequals(tokens[0],"show"))
	{
		if (tokens.size() >= 2)
		{
			if (boost::iequals(tokens[1],"measurements"))
			{
				//Example: SHOW MEASUREMENTS LIMIT 1
				//TODO: Implement
				return parse_show_measurements(p_connection, tokens, p_query);
			} else if (boost::iequals(tokens[1],"field"))
			{
				//Example: SHOW FIELD KEYS FROM "cpu_value"
				//TODO: Implement
				std::runtime_error err("SHOW FIELD: Unimplemented");
				BOOST_THROW_EXCEPTION(err);
				//return parse_show_field(p_connection, tokens, p_query);
			} else if (boost::iequals(tokens[1],"tag"))
			{
				//Example: SHOW TAG KEYS FROM "cpu_value"
				//TODO: Implement
				std::runtime_error err("SHOW TAG: Unimplemented");
				BOOST_THROW_EXCEPTION(err);
				//return parse_show_tag(p_connection, tokens, p_query);
			}
		}

		std::runtime_error err("token[1]: expected MEASUREMENTS, FIELD, TAG");
		BOOST_THROW_EXCEPTION(err);
	} else if (boost::iequals(tokens[0],"select"))
	{
		//Example: SELECT mean("value") FROM "cpu_value" WHERE time > now() - 6h GROUP BY time(30s) fill(null)
		std::runtime_error err("SELECT: Unimplemented");
		BOOST_THROW_EXCEPTION(err);
		//return parse_select(p_connection, tokens, p_query);
	} else {
		std::runtime_error err("token[0]: expected SHOW, SELECT");
		BOOST_THROW_EXCEPTION(err);
	}

	assert(0); //Should never happen
}


static int parse_http_query(MHD_Connection *p_connection, void *p_cls, void **p_con_cls)
{
	const char *db_name  = MHD_lookup_connection_value(p_connection, MHD_GET_ARGUMENT_KIND, "db");
	//const char *epoch    = MHD_lookup_connection_value(p_connection, MHD_GET_ARGUMENT_KIND, "epoch"); //Timestamp format (default: RFC3339 UTC in ns)
	//const char *password = MHD_lookup_connection_value(p_connection, MHD_GET_ARGUMENT_KIND, "p");
	//const char *username = MHD_lookup_connection_value(p_connection, MHD_GET_ARGUMENT_KIND, "u");
	//const char *chunk_size = MHD_lookup_connection_value(p_connection, MHD_GET_ARGUMENT_KIND, "chunk_size"); //Limit result batch size (default: 10k)
	//const char *pretty_json= MHD_lookup_connection_value(p_connection, MHD_GET_ARGUMENT_KIND, "pretty"); //Prettify JSON outputs (default: false)
	const char *db_query = MHD_lookup_connection_value(p_connection, MHD_GET_ARGUMENT_KIND, "q");
	if (db_name == nullptr)
	{
		return http_static_response(p_connection, "DB name missing", MHD_HTTP_BAD_REQUEST);
	}
	if (db_query == nullptr)
	{
		return http_static_response(p_connection, "DB query missing", MHD_HTTP_BAD_REQUEST);
	}
	std::cout << "MHD: db = " << db_name << ", query = " << db_query << std::endl;

	//Translate and forward query to ReadOperationBuilder
        ReadOperationBuilder *queryproc = static_cast<ReadOperationBuilder*>(p_cls);
        ReadOperation* cursor = static_cast<ReadOperation*>(*p_con_cls);

        if (cursor == nullptr)
	{
		//New request
		cursor = queryproc->create();
		*p_con_cls = cursor;
		return MHD_YES;
        }


        auto error_response = [&](const char* msg) {
            char buffer[0x200];
            int len = snprintf(buffer, 0x200, "-%s\r\n", msg);
            auto response = MHD_create_response_from_buffer(len, buffer, MHD_RESPMEM_MUST_COPY);
            int ret = MHD_queue_response(p_connection, MHD_HTTP_BAD_REQUEST, response);
            MHD_destroy_response(response);
            return ret;
        };

        // Should be called once per request
        try {
		std::string query_json = parse_query_string(p_connection, db_query);
		cursor->append(query_json.c_str(), query_json.size());
		cursor->start();
        } catch (const std::exception& err) {
            return error_response(err.what());
        }

        // Check for error
        auto err = cursor->get_error();
        if (err != AKU_SUCCESS) {
            const char* error_msg = aku_error_message(err);
            return error_response(error_msg);
        }

        auto response = MHD_create_response_from_callback(MHD_SIZE_UNKNOWN, 64*1024, &read_callback, cursor, &free_callback);
        int ret = MHD_queue_response(p_connection, MHD_HTTP_OK, response);
        MHD_destroy_response(response);
        return ret;

}

static int parse_http_write(MHD_Connection *p_connection)
{
	const char *db_name  = MHD_lookup_connection_value(p_connection, MHD_GET_ARGUMENT_KIND, "db");
	if (db_name == nullptr)
	{
		return http_static_response(p_connection, "DB name missing", MHD_HTTP_BAD_REQUEST);
	}
	//TODO: Process write requests! Temporary fix: Silently discard data
	return http_static_response(p_connection, "", MHD_HTTP_NO_CONTENT);
}

#if 0
static int parse_http_api_suggest(MHD_Connection *p_connection)
{
	//Docs: http://opentsdb.net/docs/build/html/api_http/suggest.html
	//- Case sensitive search for types, tag keys or tag values
	//- Matches begin of name ('sy' matches 'system', 'symbol', 'syblabla')
	//- Returns top 25 results if no max param is given
	//- Results are sorted alphabetically
	//- Results come as JSON array of string
	const char *metric_type  = MHD_lookup_connection_value(p_connection, MHD_GET_ARGUMENT_KIND, "type"); //Required
	//const char *metric_query = MHD_lookup_connection_value(p_connection, MHD_GET_ARGUMENT_KIND, "q"); //Optional
	//const char *metric_limit = MHD_lookup_connection_value(p_connection, MHD_GET_ARGUMENT_KIND, "max"); //Optional
	if (metric_type == nullptr)
	{
		return http_static_response(p_connection, "Metric type missing", MHD_HTTP_BAD_REQUEST);
	}
	if (strcmp(metric_type,"metrics") == 0)
	{
		//TODO: Implement
		const char *temporary_fix = "[\n  \"sys.cpu.0.nice\",\n  \"sys.cpu.0.system\",\n  \"sys.cpu.0.user\",\n  \"sys.cpu.1.nice\",\n  \"sys.cpu.1.system\",\n  \"sys.cpu.1.user\"\n]";
		return http_static_response(p_connection, temporary_fix, MHD_HTTP_OK, "application/json");
	} else if (strcmp(metric_type,"tagk") == 0)
	{
		//TODO: Implement
		const char *temporary_fix = "[\n  \"host\",\n  \"plugin\",\n  \"plugin_instance\",\n  \"type\",\n  \"type_instance\"\n]";
		return http_static_response(p_connection, temporary_fix, MHD_HTTP_OK, "application/json");
	} else if (strcmp(metric_type,"tagv") == 0)
	{
		//TODO: Implement
		return http_static_response(p_connection, "Unimplemented type value", MHD_HTTP_BAD_REQUEST);
	} else {
		return http_static_response(p_connection, "Unknown type value", MHD_HTTP_BAD_REQUEST);
	}

}

static int parse_http_api_aggregators(MHD_Connection *p_connection)
{
	//Docs: http://opentsdb.net/docs/build/html/api_http/aggregators.html
	//- Returns implemented aggregation functions
	//- Results come as JSON array of string

	//TODO: Implement
	const char *temporary_fix = "[\n    \"min\",\n    \"sum\",\n    \"max\",\n    \"avg\",\n    \"dev\"\n]";
	return http_static_response(p_connection, temporary_fix, MHD_HTTP_OK, "application/json");
}

static int parse_http_options_api_query(MHD_Connection *p_connection)
{
	//Minimal implementation

	struct MHD_Response *response;
	response = MHD_create_response_from_buffer (0,nullptr, MHD_RESPMEM_PERSISTENT);
	MHD_add_response_header(response, "Allow", "GET,POST,DELETE");
	int ret = MHD_queue_response (p_connection, MHD_HTTP_OK, response);
	MHD_destroy_response (response);
	return ret;
	//TODO: Implement
	//const char *temporary_fix = "{ \"POST\": {\"parameters\": { \"start\": {\"type\": \"string\", \"required\": true}, \"queries\":{\"type\": \"string\", \"required\": true}, \"end\": { \"type\": \"string\", \"required\": true }, } }";
	//return http_static_response(p_connection, temporary_fix, MHD_HTTP_OK, "application/json");
}
#endif

static int handle_connection(void           *p_cls,
                             MHD_Connection *p_connection,
                             const char     *p_url,
                             const char     *p_method,
                             const char     *p_version,
                             const char     *p_upload_data,
                             size_t         *p_upload_data_size,
                             void          **p_con_cls)
{
	assert(p_method);
	if (strcmp(p_method, "GET") == 0)
	{
		assert(p_url);
		std::cout << "MHD: method = " << p_method << ", url = " << p_url << std::endl;
		if (strcmp(p_url, "/query") == 0)
		{
			//InfluxDB Language
			return parse_http_query(p_connection, p_cls, p_con_cls);
#if 0
		} else if (strcmp(p_url, "/api/suggest") == 0)
		{
			//OpenTSDB language
			//GET /api/suggest?max=1000&q=cpu&type=metrics
			return parse_http_api_suggest(p_connection);
		} else if (strcmp(p_url, "/api/aggregators") == 0)
		{
			//OpenTSDB language
			//GET /api/aggregators
			return parse_http_api_aggregators(p_connection);
#endif
		} else {
			return http_static_response(p_connection, "File not found", MHD_HTTP_NOT_FOUND);
		}

		return http_static_response(p_connection, "Hello browser", MHD_HTTP_OK);
	} else if (strcmp(p_method, "POST") == 0)
	{
		if (strcmp(p_url, "/write") == 0)
		{
			return parse_http_write(p_connection);
		} else {
			return http_static_response(p_connection, "Invalid POST path", MHD_HTTP_BAD_REQUEST);
		}
#if 0
	} else if (strcmp(p_method, "OPTIONS") == 0)
	{
		if (strcmp(p_url, "/api/query") == 0)
		{
			return parse_http_options_api_query(p_connection);
		} else {
			return http_static_response(p_connection, "Invalid OPTIONS path", MHD_HTTP_BAD_REQUEST);
		}
#endif
	} else {
		return http_static_response(p_connection, "Unsupported method", MHD_HTTP_METHOD_NOT_ALLOWED);
	}
	return MHD_YES;
#if 0
    if (strcmp(method, "POST") == 0) {
        ReadOperationBuilder *queryproc = static_cast<ReadOperationBuilder*>(cls);
        ReadOperation* cursor = static_cast<ReadOperation*>(*con_cls);

        if (cursor == nullptr) {
            cursor = queryproc->create();
            *con_cls = cursor;
            return MHD_YES;
        }
        if (*upload_data_size) {
            cursor->append(upload_data, *upload_data_size);
            *upload_data_size = 0;
            return MHD_YES;
        }

        auto error_response = [&](const char* msg) {
            char buffer[0x200];
            int len = snprintf(buffer, 0x200, "-%s\r\n", msg);
            auto response = MHD_create_response_from_buffer(len, buffer, MHD_RESPMEM_MUST_COPY);
            int ret = MHD_queue_response(connection, MHD_HTTP_BAD_REQUEST, response);
            MHD_destroy_response(response);
            return ret;
        };

        // Should be called once
        try {
            cursor->start();
        } catch (const std::exception& err) {
            return error_response(err.what());
        }

        // Check for error
        auto err = cursor->get_error();
        if (err != AKU_SUCCESS) {
            const char* error_msg = aku_error_message(err);
            return error_response(error_msg);
        }

        auto response = MHD_create_response_from_callback(MHD_SIZE_UNKNOWN, 64*1024, &read_callback, cursor, &free_callback);
        int ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
        MHD_destroy_response(response);
        return ret;
    } else {
        static const char* SIGIL = "";
        auto queryproc = static_cast<ReadOperationBuilder*>(cls);
        auto cursor = static_cast<const char*>(*con_cls);
        if (cursor == nullptr) {
            *con_cls = const_cast<char*>(SIGIL);
            return MHD_YES;
        }
        std::string path = url;
        if (path == "/stats") {
            std::string stats = queryproc->get_all_stats();
            auto response = MHD_create_response_from_buffer(stats.size(), const_cast<char*>(stats.data()), MHD_RESPMEM_MUST_COPY);
            int ret = MHD_add_response_header(response, "content-type", "application/json");
            if (ret == MHD_NO) {
                return ret;
            }
            ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
            MHD_destroy_response(response);
            return ret;
        }
    }
#endif
}
} //namespace MHD

GrafanaServer::GrafanaServer(unsigned short port, std::shared_ptr<ReadOperationBuilder> qproc, AccessControlList const& acl)
    : acl_(acl)
    , proc_(qproc)
    , port_(port)
{
}


GrafanaServer::GrafanaServer(unsigned short port, std::shared_ptr<ReadOperationBuilder> qproc)
    : GrafanaServer(port, qproc, AccessControlList())
{
}
#if 0
static unsigned int get_number_of_cores()
{
	cpu_set_t cs;
	CPU_ZERO(&cs);
	if (sched_getaffinity(0, sizeof(cs), &cs) != 0)
	{
		BOOST_THROW_EXCEPTION(std::runtime_error("Unable to count cores"));
	}
	return (unsigned int)CPU_COUNT(&cs);
}
#endif

void GrafanaServer::start(SignalHandler* sig, int id)
{
	//Response rate: ~7ksps
	//Using MHD_USE_THREAD_PER_CONNECTION: Load ~150%
	//Using MHD_USE_SELECT_INTERNALLY: Load ~120%
	//Using MHD_USE_EPOLL_INTERNALLY_LINUX_ONLY: Load ~80%
	//unsigned int n_threads = get_number_of_cores();
	daemon_ = MHD_start_daemon(0 /*| MHD_USE_THREAD_PER_CONNECTION*/
#if EPOLL_SUPPORT
				| MHD_USE_EPOLL_INTERNALLY_LINUX_ONLY|MHD_USE_EPOLL_TURBO
#else
				| MHD_USE_SELECT_INTERNALLY
#endif
				| MHD_USE_DEBUG | MHD_USE_DUAL_STACK,
				port_,
				NULL, //MHD_AcceptPolicyCallback
				NULL, //void *apc_cls
				&MHD::handle_connection, //MHD_AccessHandlerCallback dh
				proc_.get(), //void *dh_cls
//				MHD_OPTION_THREAD_POOL_SIZE, n_threads,
				MHD_OPTION_END);
    if (daemon_ == nullptr) {
        BOOST_THROW_EXCEPTION(std::runtime_error("can't start daemon"));
    }

    auto self = shared_from_this();
    sig->add_handler(boost::bind(&GrafanaServer::stop, std::move(self)), id);
}

void GrafanaServer::stop() {
    MHD_stop_daemon(daemon_);
}

struct GrafanaServerBuilder {

    GrafanaServerBuilder() {
        ServerFactory::instance().register_type("GRAFANA", *this);
    }

    std::shared_ptr<Server> operator () (std::shared_ptr<IngestionPipeline>,
                                         std::shared_ptr<ReadOperationBuilder> qproc,
                                         const ServerSettings& settings) {
        return std::make_shared<GrafanaServer>(settings.port, qproc);
    }
};

static GrafanaServerBuilder reg_type;

}  // namespace Grafana
}  // namespace Akumuli

