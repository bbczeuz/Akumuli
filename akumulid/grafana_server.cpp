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
#define NUMBER_OF_THREADS CPU_COUNT
#if 0
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

#if 0
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
		MHD_add_response_header (response, "Content-Type", p_content_type);
	}
	int ret = MHD_queue_response (p_connection, p_response_code, response);
	MHD_destroy_response (response);
	return ret;
}

static int parse_show_measurments(MHD_Connection *p_connection, const std::vector<std::string> &p_tokens, const char *p_query)
{
	unsigned long nvals_requested = 0;
	if (p_tokens.size() == 2)
	{
		//Return all measurment names
		//TODO: Implement
		const char *temporary_fix = "{\"results\":[{\"series\":[{\"name\":\"measurements\",\"columns\":[\"name\"],\"values\":[[\"aggregation_value\"]]}]}]}";
		return http_static_response(p_connection, temporary_fix, MHD_HTTP_OK, "application/json");
	} else if ((p_tokens.size() == 4) && (boost::iequals(p_tokens[2],"limit")) && ((nvals_requested = std::stoul(p_tokens[3])) > 0))
	{
		//Request up to nvals_requested measurement names
		//TODO: Implement
		const char *temporary_fix = "{\"results\":[{\"series\":[{\"name\":\"measurements\",\"columns\":[\"name\"],\"values\":[[\"aggregation_value\"],[\"chrony_value\"],[\"collectd_value\"],[\"cpu_value\"],[\"df_free\"],[\"df_used\"],[\"df_value\"],[\"wxt_\"],[\"wxt_value\"]]}]}]}";
		return http_static_response(p_connection, temporary_fix, MHD_HTTP_OK, "application/json");

	} else {
		return http_static_response(p_connection, "Invalid query", MHD_HTTP_BAD_REQUEST);
	}
	assert(0); //Shouldn't arrive here
	return MHD_NO;
}

static int parse_query_string(MHD_Connection *p_connection, const char *p_query)
{
	std::string query{p_query};
	std::vector<std::string> tokens;
	boost::split(tokens, query, boost::algorithm::is_space(), boost::token_compress_on);
	//TODO: p_query could contain multiple semicolon separated queries
#if 0
	std::cout << "Tokens: ";
	for (const auto token:tokens)
	{
		std::cout << token.c_str() << ", ";
	}
	std::cout << std::endl;
#endif
	if (tokens.empty() || tokens[0].empty())
	{
		return http_static_response(p_connection, "Empty query", MHD_HTTP_BAD_REQUEST);
	}
	if (boost::iequals(tokens[0],"show"))
	{
		if ((tokens.size() >= 2) && (boost::iequals(tokens[1],"measurements")))
		{
			return parse_show_measurments(p_connection, tokens, p_query);
		}
	} else {
		return http_static_response(p_connection, "token[0]: expected SHOW, SELECT", MHD_HTTP_BAD_REQUEST);
	}

	return http_static_response(p_connection, "Hello browser", MHD_HTTP_OK);
}


static int parse_http_query(MHD_Connection *p_connection)
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

	return parse_query_string(p_connection, db_query);
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
			return parse_http_query(p_connection);
		} else {
			return http_static_response(p_connection, "File not found", MHD_HTTP_NOT_FOUND);
		}

		//Test connection: GET /query?db=collectd_db&epoch=ms&q=SHOW+MEASUREMENTS+LIMIT+1
		// --> Expected response header: Content-Type: application/json\r\n
		// --> Body: {"results":[{"series":[{"name":"measurements","columns":["name"],"values":[["aggregation_value"]]}]}]}
		// List metrics: GET /query?db=collectd_db&epoch=ms&p=YTkXsOsoAfAN4R1O1M9K&q=SHOW+MEASUREMENTS&u=grafana_user
		// --> Header: Content-Type: application/json\r\n
		// --> Body: {"results":[{"series":[{"name":"measurements","columns":["name"],"values":[["aggregation_value"],["chrony_value"],["collectd_value"],["cpu_value"],["df_free"],["df_used"],["df_value"],["disk_io_time"],["disk_read"],["vmem_majflt"],["vmem_minflt"],["vmem_out"],["vmem_value"]]}]}]} 
		//
		//
		return http_static_response(p_connection, "Hello browser", MHD_HTTP_OK);
	} else if (strcmp(p_method, "POST") == 0)
	{
		if (strcmp(p_url, "/write") == 0)
		{
			return parse_http_write(p_connection);
		} else {
			return http_static_response(p_connection, "Invalid POST path", MHD_HTTP_BAD_REQUEST);
		}
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

