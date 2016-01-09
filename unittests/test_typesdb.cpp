#include <iostream>
//#include <iterator>
#include <string>
//#include <regex>

#include <boost/regex.hpp>

using std::cout;
using std::endl;
using std::flush;

int main()
{
	//Load file
	//Parse lines
	//Parse single line
	//std::string now_line = "load                    shortterm:GAUGE:0:5000, midterm:GAUGE:0:5000, longterm:GAUGE:0:5000";
	std::string now_line = "latency                 value:GAUGE:0:U";
#if 0
	boost::cmatch matches;
	boost::regex type_regex("([a-zA-Z_0-9]+)([a-zA-Z0-9:]+)(.*)");

	if(regex_match(now_line.c_str(), matches, type_regex))
	{
		cout << "size(matches)=" << matches.size();
	}
#endif
	boost::regex space_regex("(\\S+)");
	auto words_begin = boost::sregex_iterator(now_line.begin(), now_line.end(), space_regex);
	auto words_end   = boost::sregex_iterator();
	auto nvals       = std::distance(words_begin, words_end);
	if (nvals-- < 2)
	{
		std::cerr << "ERROR: Found less than two parts\n";
	}
	std::cout << "Value name: " << words_begin->str().c_str() << "\n"; 
	++words_begin;
	std::cout << "Found " << nvals << " words:\n";
	for (auto now_part = words_begin; now_part != words_end; ++now_part)
	{ 
		auto now_part_str = now_part->str();
		std::cout << "\t" << now_part_str.c_str() << "\n";
		//FIXME: Below regex is broken
		boost::regex match_regex("(\\S+)([:]{1,1})(\\S+)([:]{1,1})(\\S+)([:]{1,1})(\\S+)");
		boost::cmatch parts;
		if(regex_match(now_part_str.c_str(), parts, match_regex))
		{
			for (auto now_part: parts)
			{
				cout << "\t\t.first = " << now_part.first << ", .second = " << now_part.second << "\n";
			}
		}
	}

}
