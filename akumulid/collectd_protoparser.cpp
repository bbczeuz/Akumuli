
#include "collectd_protoparser.h"

#include <endian.h> //be64toh
#include <arpa/inet.h> //ntohs

#define TAG_QUOTES 0
//#define TAG_QUOTES "\""

//Testing Reading:
//curl http://localhost:8181 --data '{ "metric": "df_value", "range":{ "from":"20160105T213503.1", "to":  "20160202T030500" }, "where": { "plugin_instance": ["home"] } }'
//curl http://localhost:8181 --data '{"output": {"format": "csv"}, "metric": "test", "range": {"from": "20160111T152647.70", "to": "20160111T152647.72"}}'
//curl http://localhost:8181 --data '{ "output": { "format": "csv" }, "select": "names"}'
//One needs to wait for the data to propagate before reading stuff: (invoke push_text.sh twice)
//curl http://localhost:8181 --data '{"output":{"format":"csv"},"metric":"testi","range":{"from": "20160114T131220", "to": "20160114T131221"}}'

//Testing Writing:
//date ++%Y-%m-%dT%H:%M:%SZ | awk '{print "+df_value host=postgres plugin=df plugin_instance=var type=percent_bytes type_instance=used \r\n"$0"\r\n+24.3"}' | nc localhost 8282
//date ++%Y-%m-%dT%H:%M:%SZ | awk '{print "+test tag1=A tag2=C tag3=I\r\n"$0"\r\n+24.3"}' | nc localhost 8282
//date -u ++%Y%m%dT%H%M%S.%N | awk '{print "+testi tag1=A tag2=C tag3=I\r\n"$0"\r\n+24.3"}' | nc localhost 8282
//
//functests/push_text.sh

namespace Akumuli {

//Code from collectd: daemon/plugin.h
//BEGIN
typedef unsigned long long counter_t;
typedef double gauge_t;
typedef int64_t derive_t;
typedef uint64_t absolute_t;

union value_u
{
	counter_t  counter;
	gauge_t    gauge;
	derive_t   derive;
	absolute_t absolute;
};
typedef union value_u value_t;
//END

CollectdProtoParser::CollectdProtoParser(std::shared_ptr<ProtocolConsumer> consumer, std::shared_ptr<const TypesDB> p_typesdb):
	consumer_(consumer),
	logger_("collectd-protocol-parser", 32),
	typesdb_(p_typesdb)
{
}

uint64_t CollectdProtoParser::parse_uint64_t(const char *p_buf, size_t p_buf_size)
{
	//Collectd sends integers as big-endian values
	uint64_t val;
	if (p_buf_size != sizeof(val))
	{
		std::stringstream fmt;
		fmt << "Wrong buffer size (is: " << p_buf_size << ", expected: " << sizeof(val) << ")";
		std::runtime_error err(fmt.str());
		BOOST_THROW_EXCEPTION(err);
	}
//	memcpy(&val,&p_buf,sizeof(val));
	//XXX: Any assumptions about endianness?
	val = *(uint64_t*)p_buf;
	return be64toh(val);
}

//From collectd: daemon/utils_time.h
#define CDTIME_T_TO_DOUBLE(t) (((double) (t)) / 1.0737418240)


void CollectdProtoParser::escape_redis(std::string &p_str)
{
	//As there isn't any escaping, replace any non-alphanummeric chars by '_'

#if 0
	boost::replace_all(p_str, "\"", "\\\"");
#else
	auto p_str_size = p_str.size();
	auto p_str_ptr  = p_str.c_str();
	for (size_t idx=0;idx < p_str_size; ++idx)
	{
		char now_char = p_str_ptr[idx];
		if ((now_char >= 'a') && (now_char <= 'z')) continue;
		if ((now_char >= 'A') && (now_char <= 'Z')) continue;
		if ((now_char >= '0') && (now_char <= '9')) continue;

		p_str.at(idx) = '_';
		continue;
#if 0
		switch (now_char)
		{
			case '.':
			case ',':
			case '-':
			case '_':
			case ':':
			case ';':
				continue;
			case '"': now_char = 
		}
		if ((now_char < '-') || (now_char > '~'))
		{
			p_str.at(idx) = '_';
		}
#endif
	}
#endif
}

std::string CollectdProtoParser::make_tag_chain(const tVarList &p_vl, const std::string &p_varname)
{
	//Assuming values are ALREADY escaped!

	std::string tag_chain{p_vl.plugin};
	tag_chain.reserve(200);
	tag_chain.push_back('_');
	tag_chain += p_varname;

	//Using const char* + push_back instead of std::string increases speed from 312ksps to 715ksps
	//std::vector<std::pair<const std::string &,const std::string &>> tags
	//std::vector<std::pair<const char *,const std::string &> > tags
	const std::array<std::pair<const char *,const std::string &>, 5> tags
	{
		{
			{"host",            p_vl.host},
			{"plugin",          p_vl.plugin},
			{"plugin_instance", p_vl.plugin_instance},
			{"type",            p_vl.type},
			{"type_instance",   p_vl.type_instance}
		}
	};
	for (auto now_tag: tags)
	{
#if 0
		tag_chain += " " + now_tag.first + "=" + now_tag.second;
#else
		tag_chain.push_back(' ');
		tag_chain += now_tag.first;
		tag_chain.push_back('=');
		tag_chain += now_tag.second;
#endif
	}
	//tag_chain += '\0'; //Tag must be \0 terminated
	if (tag_chain.size() > AKU_LIMITS_MAX_SNAME)
	{
		std::stringstream fmt;
		fmt << "Tag chain too long (size: " << tag_chain.size() << ", limit: " << AKU_LIMITS_MAX_SNAME;
		std::runtime_error err(fmt.str());
		BOOST_THROW_EXCEPTION(err);
	}
	//consumer_->series_to_param_id(tag_chain.c_str(), tag_chain.size(), &sample);
	const char tta[]="metric taga=B";
	return tta;
	//return tag_chain;
}


void CollectdProtoParser::parse_values(const char *p_buf, size_t p_buf_size, const CollectdProtoParser::tVarList &p_vl)
{
	size_t nvals;
	if (p_buf_size < sizeof(uint16_t))
	{
		std::stringstream fmt;
		fmt << "Wrong header buffer size (is: " << p_buf_size << ", expected: " << sizeof(uint16_t) << ")";
		std::runtime_error err(fmt.str());
		BOOST_THROW_EXCEPTION(err);
	}

	nvals = ntohs(p_buf[0] + (p_buf[1]<<8));
	uint8_t *val_types = (uint8_t *)(p_buf+2);
	value_t *val_vals  = (value_t *)(p_buf+2 + nvals*sizeof(*val_types));

#if 0
	std::shared_ptr<uint8_t> val_types( new uint8_t[nvals], std::default_delete<uint8_t[]>() );
	std::shared_ptr<value_t> val_vals(  new value_t[nvals], std::default_delete<value_t[]>() );
#endif
	
	size_t buf_size_expected = sizeof(uint16_t) + nvals*(sizeof(*val_types)+sizeof(*val_vals));

	if (p_buf_size != buf_size_expected)
	{
		std::stringstream fmt;
		fmt << "Wrong value buffer size (is: " << p_buf_size << ", expected: " << buf_size_expected << ")";
		std::runtime_error err(fmt.str());
		BOOST_THROW_EXCEPTION(err);
	}


	//logger_.trace() << "Processing " << nvals << " values";
	//ASSUMING: val_names only contains escaped names
	//logger_.trace() << "Looking up type " << p_vl.type.c_str() << " from a list of " << typesdb_->size() << " types";
	const auto typenames_it = typesdb_->find(p_vl.type.c_str());
	if (typenames_it == typesdb_->end())
	{
		logger_.info()  << "Variable type \""        << p_vl.type.c_str() << "\" not found in types.db ("
				<< "plugin \""               << p_vl.plugin.c_str()
				<< "\", instance = \""       << p_vl.plugin_instance.c_str() 
				<< "\", type_instance = \""  << p_vl.type_instance.c_str() << "\"). Skipping.";
		return;
	} else {
		const auto &typenames = typenames_it->second;
		if (nvals > typenames.size())
		{
			logger_.info() << "Skipping the last " << nvals - typenames.size() << " variables (missing type info)";
			nvals = typenames.size();
		}

		for (size_t val_idx = 0u; val_idx < nvals; ++val_idx)
		{
			uint64_t value_be = (val_vals[val_idx].absolute); //XXX: using absolute as this is the only unsigned fixed type
			uint64_t value = be64toh(value_be); //XXX: using absolute as this is the only unsigned fixed type

			aku_Sample sample{ .timestamp = p_vl.timestamp };

			switch (val_types[val_idx])
			{
			case VAR_TYPE_GAUGE:
				//Data arrives as little endian double
				sample.payload.type    = AKU_PAYLOAD_FLOAT;
				sample.payload.float64 = val_vals[val_idx].gauge;
				sample.payload.size    = sizeof(aku_Sample);
				break;
			case VAR_TYPE_COUNTER:
			case VAR_TYPE_DERIVE:
			case VAR_TYPE_ABSOLUTE:
				//Data arrives as big endian uint64_t
				sample.payload.type    = AKU_PAYLOAD_FLOAT;
				sample.payload.float64 = static_cast<double>(value); //Value will be converted to double
				sample.payload.size    = sizeof(aku_Sample);
				break;
			default:
				std::stringstream fmt;
				fmt << "Unknown value type: " << (unsigned int)val_types[val_idx];
				std::runtime_error err(fmt.str());
				BOOST_THROW_EXCEPTION(err);
			}

			//Generate series tag chain
			std::string tag_chain = make_tag_chain(p_vl, typenames[val_idx].name_);
			consumer_->series_to_param_id(tag_chain.c_str(), std::strlen(tag_chain.c_str()), &sample);
#if 0
			logger_.info() << "Value: .ts = " << sample.timestamp
				<< ", .paramid = "        << sample.paramid
				<< ", .tag_chain = "      << tag_chain.c_str() 
				<< ", .type = "           << sample.payload.type
				<< ", .size = "           << sample.payload.size
				<< ", .data = "           << sample.payload.data
				<< ", .value = "          << sample.payload.float64
			;
#endif

			//Put sample to DB
			consumer_->write(sample);
		} //for (val_idx)
	} //if (!val_names.empty())
}

void CollectdProtoParser::assign_zerostring(std::string &p_dest, const char *p_src, size_t p_src_size)
{
	//Assigns chars at p_src to p_dest skipping terminating 0; Clears p_dest if p_src is NULL or empty
	if ((p_src_size == 0) || (p_src == NULL))
	{
		p_dest.clear();
	} else {
		if (p_src_size == 1)
		{
			if (p_src[0] == 0)
			{
				p_dest.clear();
			} else {
				p_dest = p_src[0];
			}
		} else if (p_src[p_src_size-1] == 0)
		{
			p_dest.assign(p_src,p_src_size-1);
		} else {
			p_dest.assign(p_src,p_src_size);
		}
	}
}

void CollectdProtoParser::parse_next(PDU pdu)
{
	//logger_.trace() << "Parsing PDU";

	//tVarList vl = {};
	tVarList vl;
	vl.timestamp = 0;
	vl.interval  = 0;
	const size_t part_header_size = 2*sizeof(uint16_t);
	while (pdu.pos+part_header_size < pdu.size)
	{
		const char *part_head = pdu.buffer.get() + pdu.pos;
		ePartTypes  part_type = static_cast<ePartTypes>(ntohs(part_head[0] + (part_head[1]<<8)));
		size_t      part_size = ntohs(part_head[2] + (part_head[3]<<8));
		//logger_.trace() << "PDU .pos = " << pdu.pos << ", .size = " << pdu.size << ", part_size = " << part_size << ", part_type = " << part_type;
		if (part_size > (pdu.size - pdu.pos))
		{
			logger_.info() << "PDU part_size(" << part_size << ") > remaining PDU buffer(" << (pdu.size-pdu.pos) << ")";
			break;
		}
		if (part_size < part_header_size)
		{
			logger_.info() << "PDU part_size(" << part_size << ") < part_header_size(" << part_header_size << ")";
			break;
		}

		part_head += part_header_size;
		part_size -= part_header_size;

		switch (part_type)
		{
		case TYPE_HOST:
			assign_zerostring(vl.host,part_head,part_size);
			escape_redis(vl.host);
			break;
		case TYPE_TIME_HR:
			//logger_.info() << "PDU timestamp: 0x" << std::hex << parse_uint64_t(part_head,part_size) << std::dec;
			vl.timestamp = CDTIME_T_TO_DOUBLE(parse_uint64_t(part_head,part_size));
			break;
		case TYPE_INTERVAL_HR:
			//logger_.info() << "PDU interval: 0x" << std::hex << parse_uint64_t(part_head,part_size) << std::dec;
			vl.interval  = CDTIME_T_TO_DOUBLE(parse_uint64_t(part_head,part_size));
			break;
		case TYPE_PLUGIN:
			assign_zerostring(vl.plugin,part_head,part_size);
			escape_redis(vl.plugin);
			//logger_.info() << "plugin(" << vl.plugin.c_str() << ")";
			//XXX: Commented out as some collectd plugins (aggregation,cpu,df,memory) sometimes don't update the type field when changing plugin/plugin_instance (!)
			//vl.plugin_instance.clear();
			//vl.type.clear();
			//vl.type_instance.clear();
			break;
		case TYPE_PLUGIN_INSTANCE:
			assign_zerostring(vl.plugin_instance,part_head,part_size);
			escape_redis(vl.plugin_instance);
			//logger_.info() << "plugin_instance(" << vl.plugin_instance.c_str() << ")";
			//vl.type.clear();
			//vl.type_instance.clear();
			break;
		case TYPE_TYPE:
			assign_zerostring(vl.type,part_head,part_size);
			escape_redis(vl.type);
			//logger_.info() << "type(" << vl.type.c_str() << ")";
			//vl.type_instance.clear();
			break;
		case TYPE_TYPE_INSTANCE:
			assign_zerostring(vl.type_instance,part_head,part_size);
			escape_redis(vl.type_instance);
			//logger_.info() << "type_instance(" << vl.type_instance.c_str() << ")";
			break;
		case TYPE_VALUES:
			//logger_.info() << "parse_values";
			parse_values(part_head,part_size,vl);
			break;
		default:
			logger_.info() << "PDU part_type(" << part_type << ") unknown";
			break;
		}

		pdu.pos += part_size + part_header_size;
	}

#if 0
	//Crash after first packet
	std::stringstream fmt;
	fmt << "Stopping after first packet";
	std::runtime_error err(fmt.str());
	BOOST_THROW_EXCEPTION(err);
#endif

}


}  // namespace
                 