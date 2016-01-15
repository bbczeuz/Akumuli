#include <iostream>
#include <memory>

#if 0
#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#endif
#include "../akumulid/collectd_typesdb.h"
#include "../akumulid/collectd_typesdb.h"
#include "../akumulid/collectd_protoparser.h"
//#include "../libakumuli/seriesparser.h"
#include "test_collectd_protoparser_mini.h"

#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/propertyconfigurator.h>

log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("test_collectd_protoparser"));


using namespace Akumuli;

static const char* skip_space(const char* p, const char* end)
{
	while(p < end && (*p == ' ' || *p == '\t'))
	{
		p++;
	}
	return p;
}

static const char* copy_until(const char* begin, const char* end, const char pattern, char** out)
{
	char* it_out = *out;
	while(begin < end)
	{
		*it_out = *begin;
		it_out++;
		begin++;
		if (*begin == pattern)
		{
			break;
		}
	}
	*out = it_out;
	return begin;
}

static const char* skip_tag(const char* p, const char* end, bool *error)
{
    // skip until '='
    while(p < end && *p != '=' && *p != ' ' && *p != '\t') {
        p++;
    }
    if (p == end || *p != '=') {
        *error = true;
        return end;
    }
    // skip until ' '
    const char* c = p;
    while(c < end && *c != ' ') {
        c++;
    }
    *error = c == p;
    return c;
}

struct MySeriesParser
{
	static aku_Status to_normal_form(const char* begin, const char* end,
                                        char* out_begin, char* out_end,
                                        const char** keystr_begin,
                                        const char** keystr_end)
{
	//Split p_begin into <metric> <tag>+
	//Remove whitespaces
	//Sort tags
	
	assert(begin); //First char
	assert(end);   //After last char
	assert(out_begin); //Buffer for normalized form
	assert(out_end); //End of above
	assert(keystr_begin); //First tag
	assert(keystr_end); //After last tag

    // Verify args
    if (end < begin) {
        return AKU_EBAD_ARG;
    }
    if (out_end < out_begin) {
        return AKU_EBAD_ARG;
    }
    int series_name_len = end - begin;
    if (series_name_len > AKU_LIMITS_MAX_SNAME) {
        return AKU_EBAD_DATA;
    }
    if (series_name_len > (out_end - out_begin)) {
        return AKU_EBAD_ARG;
    }

    char* it_out = out_begin;
    const char* it = begin;

    // Get metric name
    it = skip_space(it, end);
    it = copy_until(it, end, ' ', &it_out);
    it = skip_space(it, end);

	if (it == end)
	{
		// At least one tag should be specified
		return AKU_EBAD_DATA;
	}

	*keystr_begin = it_out;

	//Split tags into tags[] array
	// Get pointers to the keys
	const char* tags[AKU_LIMITS_MAX_TAGS];
	auto ix_tag = 0u;
	bool error = false;
	while(it < end && ix_tag < AKU_LIMITS_MAX_TAGS)
	{
		tags[ix_tag] = it;
		it = skip_tag(it, end, &error);
		it = skip_space(it, end);
		if (!error)
		{
			ix_tag++;
		} else {
			break;
		}
	}
	if (error)
	{
		// Bad string
		return AKU_EBAD_DATA;
	}
	if (ix_tag == 0)
	{
		// User should specify at least one tag
		return AKU_EBAD_DATA;
	}

    std::sort(tags, tags + ix_tag, [tags, end](const char* lhs, const char* rhs) {
        // lhs should be always less thenn rhs
        auto lenl = 0u;
        auto lenr = 0u;
        if (lhs < rhs) {
            lenl = rhs - lhs;
            lenr = end - rhs;
        } else {
            lenl = end - lhs;
            lenr = lhs - rhs;
        }
        auto it = 0u;
        while(true) {
            if (it >= lenl || it >= lenr) {
                return it < lenl;
            }
            if (lhs[it] == '=' || rhs[it] == '=') {
                return lhs[it] == '=';
            }
            if (lhs[it] < rhs[it]) {
                return true;
            } else if (lhs[it] > rhs[it]) {
                return false;
            }
it++;
        }
        return true;
    });

    // Copy tags to output string
    for (auto i = 0u; i < ix_tag; i++) {
        // insert space
        *it_out++ = ' ';
        // insert tag
        const char* tag = tags[i];
        copy_until(tag, end, ' ', &it_out);
    }
    
    *keystr_begin = skip_space(*keystr_begin, out_end);
    *keystr_end = it_out;
    return AKU_SUCCESS;
}
};

struct ConsumerMock : ProtocolConsumer {
    std::vector<aku_ParamId>     param_;
    std::vector<aku_Timestamp>   ts_;
    std::vector<double>          data_;
    std::vector<std::string>     bulk_;

    void write(const aku_Sample& sample) {
        param_.push_back(sample.paramid);
        ts_.push_back(sample.timestamp);
        data_.push_back(sample.payload.float64);
    }

    void add_bulk_string(const Byte *buffer, size_t n) {
        bulk_.push_back(std::string(buffer, buffer + n));
    }

    aku_Status series_to_param_id(const char *p_buf, size_t p_buf_size, aku_Sample *p_sample) {
	const char *begin = p_buf;
	const char *end = p_buf+p_buf_size;
//	uint64_t &paramid = sample->paramid;
	char buffer[AKU_LIMITS_MAX_SNAME];
//	memset(buffer,0,sizeof(buffer));
	const char* keystr_begin = nullptr;
	const char* keystr_end = nullptr;
	auto status = MySeriesParser::to_normal_form(begin, end,
				       buffer, buffer+AKU_LIMITS_MAX_SNAME,
				       &keystr_begin, &keystr_end);
#if 0
	std::stringstream sm;
	sm << "to_normal_form: begin=\"" << begin << "\", ";
	sm << "to_normal_form: buffer=\"" << buffer << "\"";
	sm << "to_normal_form: keystr_begin =\"" << keystr_begin << "\"";
	LOG4CXX_INFO(logger, sm.str().c_str())
#endif
	if (status == AKU_SUCCESS) {
#if 0
		auto id = matcher_->match(buffer, keystr_end);
		if (id == 0) {
			*value = matcher_->add(buffer, keystr_end);
		} else {
			*value = id;
		}
#endif
	}
	return status;
    }
};


	void null_deleter(const char* s) {}

std::shared_ptr<const Byte> buffer_from_static_string(const char* str) {
    return std::shared_ptr<const Byte>(str, &null_deleter);
}

#if 1

namespace Akumuli
{
struct CollectdProtoParser_tester: CollectdProtoParser
{

	static void test_make_tag_chain()
	{
#if 0
		tVarList vl = {
			.host            = "localhost",
		       	.plugin          = "df",
		       	.plugin_instance = "root",
		       	.type            = "percent_used",
		       	.type_instance   = "used",
			.timestamp = 0,
			.interval  = 0
		};
#else
		tVarList vl;
	       	vl.host            = "localhost";
	       	vl.plugin          = "df";
	       	vl.plugin_instance = "root";
	       	vl.type            = "percent_used";
	       	vl.type_instance   = "used";
#endif
		std::string tag_chain = CollectdProtoParser::make_tag_chain(vl,"value");
	}
};
};

int main()
{
	//Init log
	log4cxx::BasicConfigurator::configure();
	//log4cxx::PropertyConfigurator::configure("~/.akumulid");
	//log4cxx::LoggerPtr rootLogger = log4cxx::Logger::getRootLogger();
	//logger = Logger::getLogger("sysmonLogger"); 

	try
	{
#if 0
		std::shared_ptr<TypesDB> types = std::make_shared<TypesDB>();
		types->parse_line("vmpage_number           value:GAUGE:0:4294967295");
		types->parse_line("vmpage_io               in:DERIVE:0:U, out:DERIVE:0:U");
		types->parse_line("swap_io                 value:DERIVE:0:U");
		types->parse_line("vmpage_faults           minflt:DERIVE:0:U, majflt:DERIVE:0:U");
		types->parse_line("percent                 value:GAUGE:0:100.1");
		types->parse_line("swap                    value:GAUGE:0:1099511627776");
		std::shared_ptr<ConsumerMock> consumer(new ConsumerMock());
		//std::shared_ptr<ProtocolConsumer> consumer = std::make_shared<ProtocolConsumer>();
		//CollectdProtoParser copropa(static_cast<ProtocolConsumer>(consumer),types);
		CollectdProtoParser parser(consumer,types);

		auto packet_buf = std::shared_ptr<const Byte>((const char *)collec_pcap, &null_deleter);
		PDU pdu = {
			.buffer = packet_buf,
			.size = sizeof(collec_pcap),
			.pos = 0u
		};

		//aku_Sample sample;
		//const char tag_chain[] = "variable host=test_vm_mon_as34288_net plugin=vmem plugin_instance= type=vmpage_number type_instance=anon_transparent_hugepages";
		//const char tag_chain[] = "var ztag=zvalue ytag=yvalue xtag=xvalue";
		//consumer->series_to_param_id(tag_chain,sizeof(tag_chain),&sample);
		for (auto i=0u;i<1000000;i++)
		{
			parser.parse_next(pdu);
		}
#else
		for (auto i=0u;i<5000000;i++)
		{
			CollectdProtoParser_tester::test_make_tag_chain();
		}
#endif

	} catch (std::exception &e)
	{
			LOG4CXX_WARN(logger, e.what());
	} catch (const char *e)
	{
		if (e != nullptr)
		{
			LOG4CXX_WARN(logger, e);
		} else {
			LOG4CXX_WARN(logger, "nullptr exception");
		}
	} catch (...)
	{
		LOG4CXX_WARN(logger, L"Unhandled exception");
	}
}
#endif

#if 0 
BOOST_AUTO_TEST_CASE(Test_collectd_protoparser_0) {

}
#endif

