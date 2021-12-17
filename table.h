/* table.h
   created by Cao 2021 Nov 14
   
   wrapper of RM_Manager */
#pragma once
#ifndef _TABLE_H_
#define _TABLE_H_

#include <vector>
#include "RC.h"
#include "result.h"
#include "metadata.h"
#include "RM_Manager.h"
#include "QU_Manager.h"


class Table {
public:
	bool dirty = false;
	TableMetaData meta;
	char name[21];
	RM_FileHandle file;

public:
	Table() :name("") {}
	static Result<bool, RC>  create(char* path, char* name, int count, AttrInfo* attrs);
	static Result<bool, RC>  create_prod_unit(char* path, char* name);
	static Result<Table, RC> open(char* path, char* name);

public:
	bool close();
	bool destroy();
	bool remove_index_flag_on(char* const column);
	bool add_index_flag_on(char* const column, char* const index);
	bool store_metadata_to(char* const path);
	Result<RID, RC> insert_record(char* const data);
	Result<ColumnRec*, RC> get_column(char* const column);
	Result<bool, RC> scan_open(RM_FileScan* file_scan, int n_con, Con* conditions);
	Result<bool, RC> scan_next(RM_FileScan* file_scan, RM_Record* rec);
	Result<bool, RC> scan_close(RM_FileScan* file_scan);
	bool make_select_result(SelResult* res);
	int blk_size();

	Result<bool, RC> product(Table& b, Table& dest_table);
	Result<bool, RC> project(Table& dest);
	Result<bool, RC> select(Table& dest, int n_con, Condition* conditions);

	Result<bool, RC> update_match(
		char* column, Value* v,
		int  n, Condition* conditions
	);
	Result<bool, RC> get_by_rid(RID* rid, RM_Record* rec);
	Result<bool, RC> remove_by_rid(const RID* rid);
	Result<bool, RC> remove_match(int n, Condition* conditions);
	Result<bool, RC> turn_to_con(Condition* cond, Con* con);
};


#endif
