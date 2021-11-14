
#pragma warning(disable : 4996)
#include <stdio.h>
#include <string.h>
#include <vector>
#include "SYS_Manager.h"
#include "QU_Manager.h"
#include "result.h"
#include "metadata.h"


const int PATH_SIZE = 320;


class Table {
private:
	TableMetaData meta;
public:
	char name[21];
	RM_FileHandle file;

public:
	Table() :name("") {}
	static Result<bool, RC>  create(char* path, char* name, int sz);
	static Result<Table, RC> open(char* path, char* name);

public:
	bool close();
	bool destroy();
};

Result<bool, RC> Table::create(char* path, char* name, int sz) {
	RC table = FAIL;
	char full_path[PATH_SIZE] = "";

	// metadata path
	strcpy(full_path, path);
	strcat(full_path, "\\.");
	strcat(full_path, name);
	if (!TableMetaData::create_file(full_path)) {
		// filed create metadata
		return Result<bool, RC>::Err(FAIL);
	}
	table = RM_CreateFile(path, sz);
	if (table == SUCCESS) {
		return Result<bool, RC>(true);
	}
	return Result<bool, RC>::Err(table);
}

Result<Table, RC> Table::open(char* path, char* name) {
	char full_path[PATH_SIZE] = "";
	// metadata path
	strcpy(full_path, path);
	strcat(full_path, "\\.");
	strcat(full_path, name);
	Result<TableMetaData, int> tmeta = TableMetaData::open(full_path);
	if (!tmeta.ok) {
		return Result<Table, RC>::Err(TABLE_NOT_EXIST);
	}

	// table path
	strcpy(full_path, path);
	strcat(full_path, "\\");
	strcat(full_path, name);
	Table t;
	t.meta = tmeta.result;
	RC res = RM_OpenFile(full_path, &t.file);
	if (res == SUCCESS) {
		strcpy(t.name, name);
		return Result<Table, RC>::Ok(t);
	}
	return Result<Table, RC>::Err(res);
}

bool Table::close() {
	this->meta.close();
	return RM_CloseFile(&this->file) == SUCCESS;
}

bool Table::destroy() {
	//TODO
	return true;
}

class DataBase {
private:
	char sysroot[PATH_SIZE];
	bool opened;

	std::vector<Table> opened_tables;

private:
	void prefix_root(char* dest, const char* const subdir);

public:
	DataBase(const char* const dbpath = "") {
		strcpy(this->sysroot, dbpath);
		this->opened = false;
	}
	char* const name();
	bool in_use();
	bool close();

public:
	static Result<DataBase, RC> open(const char* const db_root);
	static bool				    create(char* dbpath);
};

bool DataBase::create(char* dbpath) {
	return (
		Table::create(dbpath, "SYSTABLE", sizeof(TableRec)).ok &&
		Table::create(dbpath, "SYSCOLUMN", sizeof(ColumnRec)).ok
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

DataBase working_db;


RC execute(char * sql) {
	return FAIL;
/*
	sqlstr *sql_str = NULL;
	RC rc;
	sql_str = get_sqlstr();
	rc = parse(sql, sql_str);//只有两种返回结果SUCCESS和SQL_SYNTAX

	if (rc == SUCCESS) {
		switch (sql_str->flag) {
			//case 1:
			//判断SQL语句为select语句
			//break;

			case 2:
			//判断SQL语句为insert语句
			break;

			case 3:	
			//判断SQL语句为update语句
			break;

			case 4:					
			//判断SQL语句为delete语句
			break;

			case 5:
			//判断SQL语句为createTable语句
			break;

			case 6:	
			//判断SQL语句为dropTable语句
			break;

			case 7:
			//判断SQL语句为createIndex语句
			break;
	
			case 8:	
			//判断SQL语句为dropIndex语句
			break;
			
			case 9:
			//判断为help语句，可以给出帮助提示
			break;
		
			case 10: 
			//判断为exit语句，可以由此进行退出操作
			break;		
	}
	else {
		//fprintf(stderr, "SQL Errors: %s", sql_str->sstr.errors);
		return rc;
	}
*/
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
	return FAIL;
}

RC DropTable(char *relName) {
	return FAIL;
}

RC IndexExist(char *relName, char *attrName, RM_Record *rec) {
	return FAIL;
}

RC CreateIndex(char *indexName, char *relName, char *attrName) {
	return FAIL;
}

RC DropIndex(char *indexName) {
	return FAIL;
}

RC Insert(char *relName, int nValues, Value * values) {
	return FAIL;
}
RC Delete(char *relName, int nConditions, Condition *conditions) {
	return FAIL;
}
RC Update(char *relName, char *attrName, Value *value, int nConditions, Condition *conditions) {
	return FAIL;
}
