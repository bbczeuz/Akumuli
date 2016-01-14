//
// Copyright 2016 Claudius Zingerli <akumulimail@zeuz.ch>
// Distributed under Apache License Version 2.0
//
// Parse types.db for importing collectd data into akumuli
// vim: ctags -R --c++-kinds=+p --fields=+iaS --extra=+q --language-force=C++ -f ~/.vim/tags/akumuli ~/git/Akumuli/

#pragma once
#include <string>
#include <map>
#include <vector>

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

	static const char* str_from_mapped_name(eMappedName p_name);
	static eMappedName mapped_name_from_str(std::string const&p_str);
};

struct TypesDB
{
	typedef std::vector<TypesDBentry> entry_type;
	typedef std::map<std::string, entry_type > map_type;

	TypesDB(): locked_(false) {};
	void load(const std::string &p_path);      // Loads a whole types.db file 
	void parse_line(const std::string p_line); // Parses a single line
	void dump(std::ostream &p_out) const;      // Dumps the current contents to p_out
	void lock() { locked_ = true; }            // Disables load/parse functions (safety against changes when used in multiple threads)

	//Read-only interface to map_
	map_type::size_type size()     const { return map_.size(); };
	map_type::const_iterator end() const { return map_.end(); };
	map_type::const_iterator find( const std::string& p_key ) const { return map_.find(p_key); };
	bool exists(const std::string& p_key) { return map_.find(p_key) != map_.end(); };
	const entry_type& operator[](  const std::string& p_key ) const { return map_.at(p_key); }; //Throws std::out_of_range if p_key doesn't exist

private:
	bool locked_;
	map_type map_;
};

