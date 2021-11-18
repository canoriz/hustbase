
#pragma warning(disable : 4996)
#include <stdio.h>
#include <string.h>
#include <vector>
#include "SYS_Manager.h"
#include "QU_Manager.h"
#include "result.h"
#include "metadata.h"
#include "table.h"
#include "index.h"


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

public:
	static Result<DataBase, RC> open(const char* const db_root);
	static bool				    create(char* dbpath);
};

bool DataBase::create(char* dbpath) {
	return (
		Table::create(dbpath, "SYSTABLE", 0, NULL).ok &&
		Table::create(dbpath, "SYSCOLUMN", 0, NULL).ok
	);
}

char* const DataBase::name() {
	return this->sysroot;
}

void DataBase::prefix_root(char* dest, const char* const subdir) {
	// sysroot + "\" + subdir
	strcpy(dest, this->sysroot);
	strcat(dest, "\\");
	strcat(dest, subdir);
}

bool DataBase::in_use() {
	return this->opened;
}

Result<DataBase, RC> DataBase::open (const char* const db_root) {
	DataBase db(db_root);
	char full_path[PATH_SIZE];
	db.prefix_root(full_path, "SYSTABLE");
	FILE* fp = fopen(full_path, "rb");

	if (fp) {
		db.opened = true;
		fclose(fp);
		return Result<DataBase, RC>(db);
	}
	return Result<DataBase, RC>::Err(DB_NOT_EXIST);
}

bool DataBase::close() {
	if (this->opened) {
		this->opened = false;

		bool all_table_closed = true;
		for (auto &t : this->opened_tables) {
			all_table_closed &= t.close();
		}
		return all_table_closed;
	}

	// close nothing???
	return true;
}

bool DataBase::update_table_metadata()
{
	// update table metadata to file
	for (auto& t : this->opened_tables) {
		if (t.dirty) {
			char meta_path[PATH_SIZE] = "";
			this->prefix_root(meta_path, ".");
			strcat(meta_path, t.name);
			t.store_metadata_to(meta_path);
			t.dirty = false;
		}
	}
	this->update_table_metadata_legacy();
	return true;
}

bool DataBase::update_table_metadata_legacy() {
	// legacy mode
	char full_path[PATH_SIZE] = "";
	this->prefix_root(full_path, "SYSTABLE");
	this->drop_table("SYSTABLE");
	this->drop_table("SYSCOLUMN");

	char s01[] = "tablename";
	char s02[] = "attrcount";
	char s11[] = "tablename";
	char s12[] = "attrname";
	char s13[] = "attrtype";
	char s14[] = "attrlength";
	char s15[] = "attroffset";
	char s16[] = "ix_flag";
	char s17[] = "indexname";
	AttrInfo tab[2] = {
		{s01, chars, 20},
		{s02, ints, 4}
	};
	AttrInfo col[7] = {
		{s11, chars, 20},
		{s12, chars, 20},
		{s13, ints, 4},
		{s14, ints, 4},
		{s15, ints, 4},
		{s16, ints, 1},
		{s17, chars, 20}
	};
	Table::create(this->sysroot, "SYSTABLE", 2, tab);
	Table::create(this->sysroot, "SYSCOLUMN", 7, col);
	auto stbl_res = this->open_table("SYSTABLE");
	auto scol_res = this->open_table("SYSCOLUMN");
	if (!stbl_res.ok || !scol_res.ok) {
		return false;
	}
	auto& stbl = stbl_res.result;
	auto& scol = scol_res.result;

	for (auto& t : this->opened_tables) {
		// make [ tablename | attrcount ] char*
		const int SAME = 0;
		if (
					strcmp(t.name, "SYSTABLE") != SAME &&
					strcmp(t.name, "SYSCOLUMN") != SAME
		) {
				int count = t.meta.columns.size();
				char buf[21 + 4];
				strcpy(buf, t.name);
				memcpy(buf + 21, &count, sizeof(int));
				stbl.insert_record(buf);
				for (auto& c : t.meta.columns) {
					/*
					* insert
					* [ tablename | attrname | attrtype | attrlength | attroffset | ix? | ix_name ]
					*/
					scol.insert_record((char*)&c);
				}
		}
	}
	return true;
}

Result<bool, RC> DataBase::remove_file(const char* const file_name) {
	// remove file_name and .file_name
	// remove tables and indices
	char full_path[PATH_SIZE] = "";
	this->prefix_root(full_path, file_name);
	const int OK = 0;

	if (remove(full_path) != OK) {
		return Result<bool, RC>::Err(TABLE_NOT_EXIST);
	}

	char meta_name[PATH_SIZE] = ".";
	strcat(meta_name, file_name);
	this->prefix_root(full_path, meta_name);
	if (remove(full_path) != OK) {
		return Result<bool, RC>::Err(TABLE_NOT_EXIST);
	}
	return Result<bool, RC>::Ok(true);
}

bool DataBase::close_table(const char* const table_name) {
	for (auto it = this->opened_tables.begin(); it != this->opened_tables.end(); ++it) {
		const int SAME = 0;
		if (strcmp(table_name, it->name) == SAME) {
			// removed it from opened tables, and there should be no more to remove
			it->close();
			this->opened_tables.erase(it);
			return true;
		}
	}
	return true;
}

bool DataBase::close_index(const char* const index_name) {
	for (auto it = this->opened_indices.begin(); it != this->opened_indices.end(); ++it) {
		const int SAME = 0;
		if (strcmp(index_name, it->name) == SAME) {
			// removed it from opened tables, and there should be no more to remove
			it->close();
			this->opened_indices.erase(it);
			break;
		}
	}
	return true;
}

Result<bool, RC> DataBase::drop_table(char* const table_name) {
	auto open = this->open_table(table_name);
	if (!open.ok) {
		return Result<bool, RC>::Err(TABLE_NOT_EXIST);
	}
	Table& t = open.result;
	for (auto& c : t.meta.columns) {
		if (c.ix_flag) {
			auto res = this->drop_index(c.indexname);
			if (!res.ok) {
				return Result<bool, RC>::Err(res.err);
			}
		}
	}
	if (!this->update_table_metadata()) {
		return Result<bool, RC>::Err(FAIL);
	}
	this->close_table(table_name);
	return this->remove_file(table_name);
}

Result<bool, RC> DataBase::add_table(char* const table_name, int count, AttrInfo* attrs)
{
	auto res = Table::create(this->sysroot, table_name, count, attrs);
	if (!res.ok) {
		return Result<bool, RC>::Err(res.err);
	}
	if (!this->update_table_metadata()) {
		return Result<bool, RC>::Err(FAIL);
	}
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> DataBase::add_index(
		char* const table_name,
		char* const column_name,
		char* const index_name) {
	auto t_res = this->open_table(table_name);
	if (!t_res.ok) {
		return Result<bool, RC>::Err(TABLE_NOT_EXIST);
	}
	auto& t = t_res.result;
	auto c_res = t.get_column(column_name);
	if (!c_res.ok) {
		return Result<bool, RC>::Err(c_res.err);
	}
	auto column_record = c_res.result;

	for (auto& t : this->opened_tables) {
		const int SAME = 0;
		if (strcmp(t.name, table_name) == SAME) {
			if (!t.add_index_flag_on(column_name, index_name)) {
				return Result<bool, RC>::Err(INDEX_EXIST);
			}
			auto res = Index::create(
				this->sysroot, index_name, table_name, column_name,
				(AttrType)column_record->attrtype, column_record->attrlength
			);
			if (!res.ok) {
				// revert index mark
				t.remove_index_flag_on(column_name);
				return Result<bool, RC>::Err(res.err);
			}

			// update table metadata to file
			this->update_table_metadata();

			return Result<bool, RC>::Ok(true);
		}
	}
	return Result<bool, RC>::Err(FAIL);
}

Result<bool, RC> DataBase::drop_index(char* const index_name) {
	auto i_res = this->open_index(index_name);
	if (!i_res.ok) {
		return Result<bool, RC>::Err(INDEX_NOT_EXIST);
	}

	Index& i = i_res.result;
	auto t_res = this->open_table(i.table);
	if (!t_res.ok) {
		// the table that has index is not exist
		return Result<bool, RC>::Err(TABLE_NOT_EXIST);
	}

	for (auto& t : this->opened_tables) {
		const int SAME = 0;
		if (strcmp(t.name, i.table) == SAME) {
			if (!t.remove_index_flag_on(i.column)) {
				return Result<bool, RC>::Err(FAIL);
			}

			this->update_table_metadata();

			if (!this->close_index(index_name)) {
				return Result<bool, RC>::Err(INDEX_NOT_EXIST);
			}
			return this->remove_file(index_name);
		}
	}
	return Result<bool, RC>::Err(INDEX_NOT_EXIST);
}

Result<bool, RC> DataBase::insert(char* const table, const int n, Value* const vals)
{
	// reverse order table(a,b,c,d), values(d,c,b,a)
	auto opent = this->open_table(table);
	if (!opent.ok) {
		return Result<bool, RC>::Err(TABLE_NOT_EXIST);
	}
	auto& t = opent.result;

	char* buffer = (char*)malloc(21 * n);
	char* here = buffer;
	int processing_column_i = t.meta.columns.size() - 1;
	for (int i = 0; i < t.meta.columns.size(); i++, processing_column_i--) {
		ColumnRec* c_rec = &t.meta.columns[processing_column_i];
		int x = *(int*)vals[i].data;
		char c = *(char*)vals[i].data;
		memcpy(buffer + c_rec->attroffset, vals[i].data, c_rec->attrlength);
	}

	RID rid;
	auto ins_res = t.insert_record(buffer);
	if (!ins_res.ok) {
		free(buffer);
		return ins_res.err;
	}
	free(buffer);
	return Result<bool, RC>::Ok(true);
}

Result<Table, RC> DataBase::open_table(char* const table_name) {
	for (auto const& t : this->opened_tables) {
		const int SAME = 0;
		if (strcmp(table_name, t.name) == SAME) {
			return t;
		}
	}
	// not in opened tables
	auto res = Table::open(this->sysroot, table_name);
	if (res.ok) {
		this->opened_tables.push_back(res.result);
		return Result<Table, RC>::Ok(res.result);
	}
	return Result<Table, RC>::Err(res.err);
}

Result<Index, RC> DataBase::open_index(char* const index_name) {
	for (auto const& i : this->opened_indices) {
		const int SAME = 0;
		if (strcmp(index_name, i.name) == SAME) {
			return i;
		}
	}
	// not in opened indices
	auto res = Index::open(this->sysroot, index_name);
	if (res.ok) {
		this->opened_indices.push_back(res.result);
		return Result<Index, RC>::Ok(res.result);
	}
	return Result<Index, RC>::Err(res.err);
}

DataBase working_db;


RC execute(char * sql) {

	sqlstr *processing_sql = NULL;
	RC rc;
	processing_sql = get_sqlstr();
	rc = parse(sql, processing_sql);//只有两种返回结果SUCCESS和SQL_SYNTAX

	if (rc == SUCCESS) {
		createTable* new_table = &(processing_sql->sstr.cret);
		dropTable* drop_table = &(processing_sql->sstr.drt);
		createIndex* create_index = &(processing_sql->sstr.crei);
		dropIndex* drop_index = &(processing_sql->sstr.dri);
		inserts* insert = &(processing_sql->sstr.ins);
		switch (processing_sql->flag) {
		case 1:
			//判断SQL语句为select语句
			break;

		case 2:
			//判断SQL语句为insert语句
			return Insert(insert->relName, insert->nValues, insert->values);
			break;

		case 3:
			//判断SQL语句为update语句
			break;

		case 4:
			//判断SQL语句为delete语句
			break;

		case 5:
			//判断SQL语句为createTable语句
			return CreateTable(new_table->relName, new_table->attrCount, new_table->attributes);
			break;

		case 6:
			//判断SQL语句为dropTable语句
			return DropTable(drop_table->relName);
			break;

		case 7:
			//判断SQL语句为createIndex语句
			return CreateIndex(create_index->indexName, create_index->relName, create_index->attrName);
			break;

		case 8:
			//判断SQL语句为dropIndex语句
			return DropIndex(drop_index->indexName);
			break;

		case 9:
			//判断为help语句，可以给出帮助提示
			break;

		case 10:
			//判断为exit语句，可以由此进行退出操作
			return CloseDB();
			break;
		}
		return rc;
	}
	else {
		//fprintf(stderr, "SQL Errors: %s", sql_str->sstr.errors);
		return rc;
	}
}

RC CreateDB(char *dbpath, char *dbname) {
	if (DataBase::create(dbpath)) {
		return SUCCESS;
	}
	return FAIL;
}

RC DropDB(char *dbname) {
	// dbname: english characters only
	char full_path[PATH_SIZE] = "";
	strcpy(full_path, dbname);
	strcat(full_path, "\\.SYSTABLE");

	const int SAME = 0;
	const int OK = 0;
	if ((
			strcmp(dbname, working_db.name()) != SAME || // the dropping DB is not tht opened DB
			CloseDB() == SUCCESS // the dropping db is opened, but now closed
		) &&
		remove(full_path) == OK /* SYSTABLE remove success, indicates this is a
								hustdb directory, not regular dir, can delete */
		) {
		/*
		   Use system's rmdir command to remove directory
		   STUPID BUT WORKS
		*/
		char command[PATH_SIZE + 30] = "rmdir /s /q ";
		strcat(command, dbname);
		if (system(command) == OK) {
			return SUCCESS;
		}
	}

	return FAIL;
}

RC OpenDB(char *dbname) {
	Result<DataBase, RC> res = DataBase::open(dbname);
	if (res.ok) {
		working_db.close();
		working_db = res.result;
		return SUCCESS;
	}
	return FAIL;
}

RC CloseDB(){
	if (working_db.close()) {
		return SUCCESS;
	}
	return FAIL;
}

RC CreateTable(char *relName, int attrCount, AttrInfo *attributes) {
	auto res = working_db.add_table(relName, attrCount, attributes);
	if (res.ok) {
		return SUCCESS;
	}
	return res.err;
}

RC DropTable(char *relName) {
	// delete table, .table
	auto res = working_db.drop_table(relName);
	if (res.ok) {
		return SUCCESS;
	}
	return res.err;
}

RC IndexExist(char *relName, char *attrName, RM_Record *rec) {
	return FAIL;
}

RC CreateIndex(char *indexName, char *relName, char *attrName) {
	auto res = working_db.add_index(relName, attrName, indexName);
	if (res.ok) {
		return SUCCESS;
	}
	return res.err;
}

RC DropIndex(char *indexName) {
	auto res = working_db.drop_index(indexName);
	if (res.ok) {
		return SUCCESS;
	}
	return res.err;
}

RC Insert(char *relName, int nValues, Value * values) {
	auto res = working_db.insert(relName, nValues, values);
	if (res.ok) {
		return SUCCESS;
	}
	return res.err;
}
RC Delete(char *relName, int nConditions, Condition *conditions) {
	return FAIL;
}
RC Update(char *relName, char *attrName, Value *value, int nConditions, Condition *conditions) {
	return FAIL;
}
