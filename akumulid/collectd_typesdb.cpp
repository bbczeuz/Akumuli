//
// Copyright 2016 Claudius Zingerli <akumulimail@zeuz.ch>
// Distributed under Apache License Version 2.0
//
// Parse types.db for importing collectd data into akumuli
//

#include <fstream>
#include <boost/algorithm/string.hpp>
#include "collectd_typesdb.h"

void TypesDB::load(const std::string &p_path)
{
	//Load and parse types.db file

	//We don't do any locking check here, as this function doesn't do any changes to the object itself (parse_line does)
	
	std::ifstream inf(p_path);
	if (!inf.good())
	{
		const char* msg = strerror(errno);
		std::string fmt("ERROR: Input file ("+p_path+") bad: ");
		fmt += msg;
		std::runtime_error err(fmt);
		BOOST_THROW_EXCEPTION(err);
	}
	std::string now_line;
	while (std::getline(inf,now_line))
	{
		boost::trim(now_line);
		auto compos = now_line.find('#');
		if (compos != std::string::npos)
		{
			now_line.erase(compos);
		}
		if (now_line.empty())
		{
			continue;
		}
		//cout << "now_line = \"" << now_line.c_str() << "\"\n";
		parse_line(now_line);
	}
}

void TypesDB::parse_line(const std::string p_line)
{
	if (locked_)
	{
		throw std::domain_error("object locked");
	}
	//std::string now_line = "load                    shortterm:GAUGE:0:5000, midterm:GAUGE:0:5000, longterm:GAUGE:0:5000";
	//std::string now_line = "latency                 value:GAUGE:0:U";
	typedef std::vector< std::string > StrVector;
	StrVector parts;

	//Split into value name and type sections
	boost::split( parts, p_line, boost::is_any_of("\t ,"), boost::token_compress_on );
	auto parts_begin = parts.begin();
	auto parts_end   = parts.end();
	auto nvals       = parts.size();

	//std::cout << "Value name: " << parts_begin->c_str() << "\n"; 
	if (nvals < 2)
	{
		std::string fmt("Found no value types (line: \"" + p_line + "\")");
		std::runtime_error err(fmt);
		BOOST_THROW_EXCEPTION(err);
	}

	//std::cout << "Found " << --nvals << " value types:\n";
	++parts_begin; //Skip value name

	//Process individual sections
	for (auto now_part = parts_begin; now_part != parts_end; ++now_part)
	{ 
		auto now_part_str = *now_part;
		//std::cout << "\t" << now_part_str.c_str() << "\n";

		StrVector sections;
		boost::split( sections, now_part_str, boost::is_any_of(":"), boost::token_compress_on ); 
		if (sections.size() != 4)
		{
			std::string fmt("Found more or less than four sections per type (type: \"" + now_part_str + "\")");
			std::runtime_error err(fmt);
			BOOST_THROW_EXCEPTION(err);
			continue;
		}
		map_[parts[0]].push_back(TypesDBentry{
			.name_ = sections[0],
			.mapped_name_ = TypesDBentry::mapped_name_from_str(sections[1]),
			.min_  = boost::iequals(sections[2],"u") ? -std::numeric_limits<double>::infinity(): std::stod(sections[2]),
			.max_  = boost::iequals(sections[3],"u") ?  std::numeric_limits<double>::infinity(): std::stod(sections[3])
		});
	}
}


void TypesDB::dump(std::ostream &p_out) const
{
	for (const auto now_val: map_)
	{
		for (const auto now_part: now_val.second)
		{
			p_out   << now_val.first.c_str() << "." << now_part.name_.c_str() << ": " 
				<< TypesDBentry::str_from_mapped_name(now_part.mapped_name_) 
				<< " " << now_part.min_ <<" ... " << now_part.max_ << std::endl;
		}
	}
}

#if 0
const TypesDB::entry_type& TypesDB::operator[](  const std::string& p_key ) const
{
	auto iter = map_.find(p_key);
	if (iter != map_.end())
	{
		return iter.second;
	} else {
		return <new entry? smart ptr? wtf?>
	}
}
#endif


const char* TypesDBentry::str_from_mapped_name(eMappedName p_name)
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


TypesDBentry::eMappedName TypesDBentry::mapped_name_from_str(std::string const&p_str)
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

