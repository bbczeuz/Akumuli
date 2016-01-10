#include <iostream>
#include <fstream>
#include <string>
#include <map>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <boost/algorithm/string.hpp>
#include "../akumulid/collectd_typesdb.h"

#if 0
int main()
{
	//Load file
	TypesDB typesdb;
	typesdb.load("types_test.db");
	typesdb.dump(cout);
}
#endif

BOOST_AUTO_TEST_CASE(Test_typesdb_0) {
	TypesDB typesdb;
	typesdb.parse_line("name 	value:GAUGE:0:U");
	BOOST_CHECK(typesdb.size() == 1);
	typesdb.parse_line("name 	value:GAUGE:0:U");
	BOOST_CHECK(typesdb.size() == 1);
	typesdb.parse_line("namei 	value:GAUGE:0:U");
	BOOST_CHECK(typesdb.size() == 2);
}

BOOST_AUTO_TEST_CASE(Test_typesdb_1) {
	TypesDB typesdb;
	typesdb.parse_line("name 	value:GAUGE:0:U");
	typesdb.parse_line("namei 	value:GAUGE:0:U");
	std::string name_str("name");
	BOOST_CHECK(typesdb.exists("name"));
	BOOST_CHECK(typesdb.exists(name_str));
	BOOST_CHECK(typesdb.find("name") != typesdb.end());
	BOOST_CHECK(typesdb.find(name_str) != typesdb.end());
	BOOST_CHECK(typesdb.find("namei") != typesdb.end());
}

BOOST_AUTO_TEST_CASE(Test_typesdb_2) {
	TypesDB typesdb;
	typesdb.parse_line("name 	value:GAUGE:0:U");
	BOOST_CHECK(typesdb["name"].size() == 1);
}

BOOST_AUTO_TEST_CASE(Test_typesdb_3) {
	TypesDB typesdb;
	typesdb.parse_line("names 	value:GAUGE:0:U");
	typesdb.parse_line("namei 	value:ABSOLUTE:U:-100");
	typesdb.parse_line("namek 	value:COUNTER:-100:1000");
	BOOST_CHECK(typesdb["names"][0].name_.compare("value") == 0);
	BOOST_CHECK(typesdb["names"][0].mapped_name_ == TypesDBentry::GAUGE);
	BOOST_CHECK(typesdb["namei"][0].mapped_name_ == TypesDBentry::ABSOLUTE);
	BOOST_CHECK(typesdb["namek"][0].mapped_name_ == TypesDBentry::COUNTER);
	BOOST_CHECK(typesdb["names"][0].min_ == 0);
	BOOST_CHECK(typesdb["namei"][0].min_ < -1<<31);
	BOOST_CHECK(typesdb["namek"][0].min_ == -100);
	BOOST_CHECK(typesdb["names"][0].max_ > 1<<31); 
	BOOST_CHECK(typesdb["namei"][0].max_ == -100);
	BOOST_CHECK(typesdb["namek"][0].max_ == 1000); 
}

BOOST_AUTO_TEST_CASE(Test_typesdb_4) {
	TypesDB typesdb;
	typesdb.parse_line("names 	value:GAUGE:0:U, type2:COUNTER:U:0");
	BOOST_CHECK(typesdb["names"].size() == 2);
	BOOST_CHECK(typesdb["names"][0].name_.compare("value") == 0);
	BOOST_CHECK(typesdb["names"][1].name_.compare("type2") == 0);
	BOOST_CHECK(typesdb["names"][0].mapped_name_ == TypesDBentry::GAUGE);
	BOOST_CHECK(typesdb["names"][1].mapped_name_ == TypesDBentry::COUNTER);
	BOOST_CHECK(typesdb["names"][0].min_ == 0);
	BOOST_CHECK(typesdb["names"][1].min_ < -1<<31);
	BOOST_CHECK(typesdb["names"][0].max_ > 1<<31); 
	BOOST_CHECK(typesdb["names"][1].max_ == 0); 
}

BOOST_AUTO_TEST_CASE(Test_typesdb_10) {
	TypesDB typesdb;
	typesdb.lock();
	int caught=0;
	try
	{
		typesdb.parse_line("names       value:GAUGE:0:U");
	} catch (std::domain_error &e)
	{
		caught=1;
	}
	BOOST_CHECK(caught==1);
}
//TODO: Any negative tests needed?
// - Invalid characters, names, values
// - empty strings
// - multi-line strings
// - any escaping?

