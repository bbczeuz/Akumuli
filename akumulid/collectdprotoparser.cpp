
#include "collectdprotoparser.h"

#include <endian.h> //be64toh
#include <arpa/inet.h> //ntohs

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

CollectdProtoParser::CollectdProtoParser(std::shared_ptr<ProtocolConsumer> consumer, const std::string &p_typesdb_path):
	consumer_(consumer),
	logger_("collectd-protocol-parser", 32),
	typesdb_path(p_typesdb_path)
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
	val = *(uint64_t*)p_buf;
	return be64toh(val);
}

#if 0
double CollectdProtoParser::parse_double64_t(const char *p_buf, size_t p_buf_size)
{
	//Collectd sends doubles as little-endian values
#if 0
	uint64_t val = parse_uint64_t(p_buf,p_buf_size);
	uint32_t time_low   = val & 0x00000000ffffffff;
	uint32_t time_high  = val >> 32;
	double rv = double(time_low) >> 30;
	rv += double(time_high) << 2;
#endif
	uint64_t val;
	if (p_buf_size != sizeof(val))
	{
		std::stringstream fmt;
		fmt << "Wrong buffer size (is: " << p_buf_size << ", expected: " << sizeof(val) << ")";
		std::runtime_error err(fmt.str());
		BOOST_THROW_EXCEPTION(err);
	}
//	memcpy(&val,&p_buf,sizeof(val));
	val = *(uint64_t*)p_buf;
	return reinterpret_cast<double>(val);
}
#endif

//From collectd: daemon/utils_time.h
#define CDTIME_T_TO_DOUBLE(t) (((double) (t)) / 1073741824.0)


void CollectdProtoParser::escape_redis(std::string &p_str)
{
	for (auto now_char: p_str)
	{
		if ((now_char < '-') || (now_char > '~'))
		{
			now_char = '_';
		}
	}
}

void CollectdProtoParser::parse_values(const char *p_buf, size_t p_buf_size, const CollectdProtoParser::tVarList &p_vl)
{
	uint16_t nvals;
	if (p_buf_size < sizeof(nvals))
	{
		std::stringstream fmt;
		fmt << "Wrong header buffer size (is: " << p_buf_size << ", expected: " << sizeof(nvals) << ")";
		std::runtime_error err(fmt.str());
		BOOST_THROW_EXCEPTION(err);
	}

	nvals = ntohs(p_buf[0] + p_buf[1]<<8);
	uint8_t *val_types = (uint8_t*)(p_buf+2);
	value_t *val_vals  = (value_t *)(p_buf+2+nvals*sizeof(*val_types));

#if 0
	std::shared_ptr<uint8_t> val_types( new uint8_t[nvals], std::default_delete<uint8_t[]>() );
	std::shared_ptr<value_t> val_vals(  new value_t[nvals], std::default_delete<value_t[]>() );
#endif
	
	size_t buf_size_expected = sizeof(nvals) + nvals*(sizeof(*val_types)+sizeof(*val_vals));

	if (p_buf_size != buf_size_expected)
	{
		std::stringstream fmt;
		fmt << "Wrong value buffer size (is: " << p_buf_size << ", expected: " << buf_size_expected << ")";
		std::runtime_error err(fmt.str());
		BOOST_THROW_EXCEPTION(err);
	}

	aku_Sample sample;

	logger_.info() << "Processing " << nvals << " values";
	//TODO: Process types.db and check number of values!
	//types.db-Format: <name><typename>+   with <name>=[a-z_]+  and <typename>=\w+<name>:<type>:<min><max> with <name>=[a-z_]+ and <type> = [{absolute},{derive},{gauge},{counter}] and <min>,<max> = \n+
	//ASSUMING: val_names only contains escaped names
	std::vector<std::string> val_names{"value"};
	if (!val_names.empty())
	{
		if (nvals > val_names.size())
		{
			logger_.info() << "Skipping the last " << nvals - val_names.size() << " variables (missing type info)";
			nvals = val_names.size();
		}

		for (auto val_idx = 0; val_idx < nvals; ++val_idx)
		{
			uint64_t value_be = (val_vals[val_idx].absolute); //XXX: using absolute as this is the only unsigned fixed type
			uint64_t value = be64toh(value_be); //XXX: using absolute as this is the only unsigned fixed type

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
			std::string tag_chain;
			{
				//Escape values
				assert(val_idx < val_names.size());
				tag_chain = p_vl.plugin;
				escape_redis(tag_chain);
				tag_chain += "_" + val_names[val_idx];

				std::vector<std::pair<std::string,std::string>> tags
				{
					{"host",            p_vl.host},
					{"plugin",          p_vl.plugin},
					{"plugin_instance", p_vl.plugin_instance},
					{"type",            p_vl.type},
					{"type_instance",   p_vl.type_instance}
				};
				for (auto now_tag: tags)
				{
					if ((!now_tag.second.empty()) && (now_tag.second[now_tag.second.size()-1]==0))
					{
						now_tag.second.erase(now_tag.second.end()-1);
					}
					escape_redis(now_tag.second);
					tag_chain += " " + now_tag.first + "=" + now_tag.second;
				}
				if (tag_chain.size() > AKU_LIMITS_MAX_SNAME)
				{
					std::stringstream fmt;
					fmt << "Tag chain too long (size: " << tag_chain.size() << ", limit: " << AKU_LIMITS_MAX_SNAME;
					std::runtime_error err(fmt.str());
					BOOST_THROW_EXCEPTION(err);
				}
				consumer_->series_to_param_id(tag_chain.c_str(), tag_chain.size(), &sample);
			}
			
#if 0
			logger_.info() << "Value: .ts = " << p_vl.timestamp 
				<< ", .invl = "   << p_vl.interval 
				<< ", .host = "   << p_vl.host.c_str() 
				<< ", .plugin = " << p_vl.plugin.c_str() 
				<< ", .plugin_instance = " << p_vl.plugin_instance.c_str() 
				<< ", .type = "            << p_vl.type.c_str() 
				<< ", .type_instance = "   << p_vl.type_instance.c_str() 
				<< ", typeof(value) = "    << (unsigned int)val_types[val_idx] 
				<< ", .size = "            << sizeof(value) 
				<< ", .value = "           << sample.payload.float64;
#else
			logger_.info() << "Value: .ts = " << p_vl.timestamp 
				<< ", .invl = "           << p_vl.interval 
				<< ", .tag_chain = "      << tag_chain.c_str() 
				<< ", .paramid = "        << sample.paramid
				<< ", typeof(value) = "   << (unsigned int)val_types[val_idx] 
				<< ", .size = "           << sizeof(value) 
				<< ", .value = "          << sample.payload.float64;
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
	logger_.info() << "Parsing PDU";

	tVarList vl;
	const size_t part_header_size = 2*sizeof(uint16_t);
	while (pdu.pos+part_header_size < pdu.size)
	{
		const char *part_head = pdu.buffer.get() + pdu.pos;
		ePartTypes  part_type = static_cast<ePartTypes>(ntohs(part_head[0] + part_head[1]<<8));
		size_t      part_size = ntohs(part_head[2] + part_head[3]<<8);
		logger_.info() << "PDU .pos = " << pdu.pos << ", .size = " << pdu.size << ", part_size = " << part_size << ", part_type = " << part_type;
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
			vl.plugin_instance.clear();
			vl.type.clear();
			vl.type_instance.clear();
			break;
		case TYPE_PLUGIN_INSTANCE:
			assign_zerostring(vl.plugin_instance,part_head,part_size);
			vl.type.clear();
			vl.type_instance.clear();
			break;
		case TYPE_TYPE:
			assign_zerostring(vl.type,part_head,part_size);
			vl.type.assign(part_head,part_size);
			vl.type_instance.clear();
			break;
		case TYPE_TYPE_INSTANCE:
			assign_zerostring(vl.type_instance,part_head,part_size);
			break;
		case TYPE_VALUES:
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
                 
