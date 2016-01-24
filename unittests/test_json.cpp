#include <iostream>
#include <map>
#include <string>
#include <memory>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>


#if 0
{
	"results":
	{
		"series":
		{
			"name": "measurements",
			"columns":
				[
					"measurements"
				],
			"values":
				[
					"vmem_value",
					"df_value"
					"mem_value"
				],
		}
	}
}
#endif
#if 1
static std::string to_json(boost::property_tree::ptree tree)
{
	std::stringstream stream;
	boost::property_tree::json_parser::write_json(stream, tree);
	return stream.str();
}
#endif

#if 0
namespace progon
{
	struct Json;
	struct Json
	{
		typedef std::shared_ptr<Json> child_type;
		//typedef Json child_type;
		std::map<std::string, child_type> array;
		//std::map<std::string, child_type> array;
		child_type child;
		std::string name,value;
		void serialize(std::stringstream &p_out)
		{
#if 0
			if (!array.empty())
			{
				for (const auto ary:array)
				{
					if (!ary.first.empty())
					{
						p_out << "\"" << name.c_str() << "\": \n";
					}
					p_out << "[\n";
					ary.second->serialize(p_out);
					p_out << "]\n";
				}
			} else {
#else
			{
#endif
				if (!name.empty())
				{
					p_out << "\"" << name.c_str() << "\": \n";
				}
				if (!value.empty())
				{
					p_out << "\"" << name.c_str() << "\"\n";
				} else {
					p_out << "{\n";
//					child->serialize(p_out);
					p_out << "}\n";
				}
			}
		}
	};
};
#endif

int main()
{
	std::string reference = R"({
	"results": [
	{
		"series": [
			{
				"name": "measurements",
				"columns": [
					"name"
				],
				"values": [
					[
						"aggregation_value"
					],
					[
						"chrony_value"
					],
					[
						"wxt_value"
					]
				]
			}
		]
	}
	]
})";

	std::cout << "Reference:\n" << reference.c_str() << std::endl;

#if 0	
	progon::Json json;
	//json.array["results"] = "";//]->child->name = "ble";
	json.child["results"] = "";//]->child->name = "ble";

	std::stringstream ss;
	json.serialize(ss);
	std::cout << "Serialized:\n" << ss.str().c_str() << std::endl;
#endif
#if 1
	boost::property_tree::ptree response_tree_;
	response_tree_.put("results.series.name","measurements");
	boost::property_tree::ptree cols_tree,cols0_tree,cols1_tree;
	cols1_tree.put("series","measurements");
	cols0_tree.push_back(std::make_pair("",cols1_tree));
	cols_tree.push_back(std::make_pair("",cols0_tree));
	response_tree_.add_child("results",cols_tree);

	std::string respstr = to_json(response_tree_);
	std::cout << "Result:\n" << respstr.c_str() << std::endl;
#endif
#if 0
	Json json0,json1;

	json0.put("results.series.name", "wrong name");
	json0.put("results.series.name", "measurements"); //Replace old value
	json0.append("results.series.columns", "measurements");
	json0.append("results.series.values", "vmem_value");
	json0.append("results.series.values", "df_value");

	json1.append("","vmem_value");
	json1.append("","df_value");
	json0.put("results.series.values",json1);

	std::string serialized = json0.serialize();

	auto name = json0.get("results.series.name");
	auto values = json0.get("results.series.values");
	for (const auto value: values)
	{
		std::cout << "value = " << value.c_str() << std::endl;
	}
#endif
}

