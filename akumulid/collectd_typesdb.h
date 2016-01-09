//
// Copyright 2016 Claudius Zingerli <akumulimail@zeuz.ch>
// Distributed under Apache License Version 2.0
//
// Parse types.db for importing collectd data into akumuli

#pragma once
#include <fstream>
#include <string>
#include <map>
#include <boost/algorithm/string.hpp>

struct TypesDBentry
{
	std::string name_;
	typedef enum
	{
		GAUGE,
		COUNTER,DCOUNTER,
		DERIVE,DDERIVE,
		ABSOLUTE,
		COMPUTE
	} eMappedName;
	eMappedName mapped_name_;
	double min_,max_;

	static const char* str_from_mapped_name(eMappedName p_name)
	{
		switch (p_name)
		{
			#define CASE_STR_RET_STR(x) case x: return #x; break
			default:
			CASE_STR_RET_STR(GAUGE);
			CASE_STR_RET_STR(COUNTER);
			CASE_STR_RET_STR(DCOUNTER);
			CASE_STR_RET_STR(DERIVE);
			CASE_STR_RET_STR(DDERIVE);
			CASE_STR_RET_STR(ABSOLUTE);
			CASE_STR_RET_STR(COMPUTE);
		}
	}
	static eMappedName mapped_name_from_str(std::string const&p_str)
	{
		//Translates p_str to eMappedName value; Defaults to GAUGE
		if (boost::iequals(p_str,"gauge"))
		{
			return TypesDBentry::GAUGE;
		} else
		if (boost::iequals(p_str,"counter"))
		{
			return TypesDBentry::COUNTER;
		} else
		if (boost::iequals(p_str,"dcounter"))
		{
			return TypesDBentry::DCOUNTER;
		} else
		if (boost::iequals(p_str,"derive"))
		{
			return TypesDBentry::DERIVE;
		} else
		if (boost::iequals(p_str,"dderive"))
		{
			return TypesDBentry::DDERIVE;
		} else
		if (boost::iequals(p_str,"absolute"))
		{
			return TypesDBentry::ABSOLUTE;
		} else
		if (boost::iequals(p_str,"compute"))
		{
			return TypesDBentry::COMPUTE;
		} else {
			return TypesDBentry::GAUGE;
		}
	}
};

struct TypesDB: std::map<std::string, std::vector<TypesDBentry> >
{
	void load(const std::string &p_path);      // Loads a whole types.db file 
	void parse_line(const std::string p_line); // Parses a single line
	void dump(std::ostream &p_out) const;      // Dumps the current contents to p_out
};

