//
// Copyright 2016 Claudius Zingerli <akumulimail@zeuz.ch>
// Distributed under Apache License Version 2.0
//
// Parse types.db for importing collectd data into akumuli
//

#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <boost/algorithm/string.hpp>
#include "collectd_typesdb.h"

void TypesDB::load(const std::string &p_path)
{
	std::ifstream inf(p_path);
	if (!inf.good())
	{
		std::cerr << "ERROR: Input file bad\n";
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
		cout << "now_line = \"" << now_line.c_str() << "\"\n";
		parse_line(now_line);
	}
}

void TypesDB::parse_line(const std::string p_line)
{
	//std::string now_line = "load                    shortterm:GAUGE:0:5000, midterm:GAUGE:0:5000, longterm:GAUGE:0:5000";
	//std::string now_line = "latency                 value:GAUGE:0:U";
	typedef std::vector< std::string > StrVector;
	StrVector parts;

	//Split into value name and type sections
	boost::split( parts, p_line, boost::is_any_of("\t ,"), boost::token_compress_on );
	auto parts_begin = parts.begin();
	auto parts_end   = parts.end();
	auto nvals       = parts.size();

	std::cout << "Value name: " << parts_begin->c_str() << "\n"; 
	if (nvals < 2)
	{
		std::cerr << "ERROR: Found no value parts\n";
		return;
	}

	std::cout << "Found " << --nvals << " value types:\n";
	++parts_begin; //Skip value name

	//Process individual sections
	for (auto now_part = parts_begin; now_part != parts_end; ++now_part)
	{ 
		auto now_part_str = *now_part;
		std::cout << "\t" << now_part_str.c_str() << "\n";

		StrVector sections;
		boost::split( sections, now_part_str, boost::is_any_of(":"), boost::token_compress_on ); 
		if (sections.size() != 4)
		{
			std::cerr << "ERROR: Found other than four sections per part\n";
			continue;
		}
		this->operator[](parts[0]).push_back(TypesDBentry{
			.name_ = sections[0],
			.mapped_name_ = TypesDBentry::mapped_name_from_str(sections[1]),
			.min_  = boost::iequals(sections[2],"u") ? -std::numeric_limits<double>::infinity(): std::stod(sections[2]),
			.max_  = boost::iequals(sections[3],"u") ?  std::numeric_limits<double>::infinity(): std::stod(sections[3])
		});
	}
}


void TypesDB::dump(std::ostream &p_out) const
{
	for (const auto now_val: *this)
	{
		for (const auto now_part: now_val.second)
		{
			p_out   << now_val.first.c_str() << "." << now_part.name_.c_str() << ": " 
				<< TypesDBentry::str_from_mapped_name(now_part.mapped_name_) 
				<< " " << now_part.min_ <<" ... " << now_part.max_ << std::endl;
		}
	}
}

