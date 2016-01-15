#pragma once

#include "protocolparser.h"
#include "collectd_typesdb.h"

namespace Akumuli {

struct CollectdProtoParser_tester;

class CollectdProtoParser {
//	mutable Caller *caller_;
//	mutable std::queue<PDU> buffers_;
//	static const PDU POISON_;  //< This object marks end of the stream
//	bool done_;
	std::shared_ptr<ProtocolConsumer> consumer_;
	Logger logger_;
	std::shared_ptr<const TypesDB> typesdb_;

protected:
	enum ePartTypes
	{
		TYPE_HOST            = 0x0000,
		TYPE_TIME            = 0x0001,
		TYPE_TIME_HR         = 0x0008,
		TYPE_PLUGIN          = 0x0002,
		TYPE_PLUGIN_INSTANCE = 0x0003,
		TYPE_TYPE            = 0x0004,
		TYPE_TYPE_INSTANCE   = 0x0005,
		TYPE_VALUES          = 0x0006,
		TYPE_INTERVAL        = 0x0007,
		TYPE_INTERVAL_HR     = 0x0009,
		TYPE_MESSAGE         = 0x0100,
		TYPE_SEVERITY        = 0x0101,
		TYPE_SIGN_SHA256     = 0x0200,
		TYPE_ENCR_AES256     = 0x0210,
	};

	enum eVarTypes
	{
		VAR_TYPE_COUNTER     = 0x0000,
		VAR_TYPE_GAUGE       = 0x0001,
		VAR_TYPE_DERIVE      = 0x0002,
		VAR_TYPE_ABSOLUTE    = 0x0003,
	};

	struct tVarList
	{
		//We need to copy the data anyhow (escaping), so a std::string is not a big performance penalty
#define tVarListVARTYPE std::string
//#define tVarListVARTYPE const char * 
		tVarListVARTYPE host;
		tVarListVARTYPE plugin;
		tVarListVARTYPE plugin_instance;
		tVarListVARTYPE type;
		tVarListVARTYPE type_instance;
		uint64_t timestamp = 0, interval = 0;
	};


	static uint64_t parse_uint64_t(const char *p_buf, size_t p_buf_size);
	//uint64_t parse_double64_t(const char *p_buf, size_t p_buf_size);
	void parse_values(const char *p_buf, size_t p_buf_size, const tVarList &p_vl);
	static void escape_redis(std::string &p_str);
	static void assign_zerostring(std::string &p_dest, const char *p_src, size_t p_src_size);
	static std::string make_tag_chain(const tVarList &p_vl, const std::string &p_varname);

public:
	CollectdProtoParser(std::shared_ptr<ProtocolConsumer> consumer, std::shared_ptr<const TypesDB> p_typesdb);
	void start();
	void parse_next(PDU pdu);

	friend struct CollectdProtoParser_tester;
};


}  // namespace
//struct Bla
//{ int a,cc,b,c,d;
//};
//
//Bla bla = { .a = 1, .c = 2, 3, 4 }
