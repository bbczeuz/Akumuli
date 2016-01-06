
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

CollectdProtoParser::CollectdProtoParser(std::shared_ptr<ProtocolConsumer> consumer):
	consumer_(consumer),
	logger_("collectd-protocol-parser", 32)
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
			vl.host.assign(part_head,part_size);
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
			vl.plugin.assign(part_head,part_size);
			break;
		case TYPE_PLUGIN_INSTANCE:
			vl.plugin_instance.assign(part_head,part_size);
			break;
		case TYPE_TYPE:
			vl.type.assign(part_head,part_size);
			break;
		case TYPE_TYPE_INSTANCE:
			vl.type_instance.assign(part_head,part_size);
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

	//Crash after first packet
	std::stringstream fmt;
	fmt << "Stopping after first packet";
	std::runtime_error err(fmt.str());
	BOOST_THROW_EXCEPTION(err);
}


}  // namespace
                 
