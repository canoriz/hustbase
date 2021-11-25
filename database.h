#pragma once
#include <vector>
#include "result.h"
#include "RC.h"
#include "table.h"
#include "index.h"
#include "QU_Manager.h"

const int PATH_SIZE = 320;


class DataBase {
private:
	char sysroot[PATH_SIZE];
	bool opened;

	std::vector<Table> opened_tables;
	std::vector<Index> opened_indices;

private:
	void prefix_root(char* dest, const char* const subdir);
	bool close_table(const char* const table_name);
	bool close_index(const char* const index_name);

public:
	DataBase(const char* const dbpath = "") {
		strcpy(this->sysroot, dbpath);
		this->opened = false;
	}
	char* const name();
	bool in_use();
	bool close();
	bool update_table_metadata();
	bool update_table_metadata_legacy();
	Result<bool, RC> remove_file(const char* const file_name);
	Result<bool, RC> drop_table(char* const table_name);
	Result<bool, RC> add_table(char* const table_name, int count, AttrInfo* attrs);
	Result<bool, RC> add_index(char* const table_name, char* const column_name, char* const index_name);
	Result<bool, RC> drop_index(char* const index_name);
	Result<bool, RC> insert(char* const table, const int n, Value* const vals);
	Result<Table, RC> open_table(char* const table_name);
	Result<Index, RC> open_index(char* const index_name);
	Result<Table, RC> table_product(char* const t1, char* const t2, char* const dest);
	Result<Table, RC> query(
		int nColumns, RelAttr** columns,        /* []*RelAttr  columns */
		int nTables, char** tables,             /* *(*char)(*TYPE) tables */
		int nConditions, Condition* conditions, /* []Condition conditions */
		SelResult* res);

public:
	static Result<DataBase, RC> open(const char* const db_root);
	static bool				    create(char* dbpath);
};
