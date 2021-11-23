
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
#include "database.h"

DataBase working_db;


RC execute(char* sql) {

	sqlstr* processing_sql = NULL;
	RC rc;
	processing_sql = get_sqlstr();
	rc = parse(sql, processing_sql);//只有两种返回结果SUCCESS和SQL_SYNTAX

	if (rc == SUCCESS) {
		createTable* new_table = &(processing_sql->sstr.cret);
		dropTable* drop_table = &(processing_sql->sstr.drt);
		createIndex* create_index = &(processing_sql->sstr.crei);
		dropIndex* drop_index = &(processing_sql->sstr.dri);
		inserts* insert = &(processing_sql->sstr.ins);
		selects* select = &(processing_sql->sstr.sel);
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

RC CreateDB(char* dbpath, char* dbname) {
	if (DataBase::create(dbpath)) {
		return SUCCESS;
	}
	return FAIL;
}

RC DropDB(char* dbname) {
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

RC OpenDB(char* dbname) {
	Result<DataBase, RC> res = DataBase::open(dbname);
	if (res.ok) {
		working_db.close();
		working_db = res.result;
		return SUCCESS;
	}
	return FAIL;
}

RC CloseDB() {
	if (working_db.close()) {
		return SUCCESS;
	}
	return FAIL;
}

RC CreateTable(char* relName, int attrCount, AttrInfo* attributes) {
	auto res = working_db.add_table(relName, attrCount, attributes);
	if (res.ok) {
		return SUCCESS;
	}
	return res.err;
}

RC DropTable(char* relName) {
	// delete table, .table
	auto res = working_db.drop_table(relName);
	if (res.ok) {
		return SUCCESS;
	}
	return res.err;
}

RC IndexExist(char* relName, char* attrName, RM_Record* rec) {
	return FAIL;
}

RC CreateIndex(char* indexName, char* relName, char* attrName) {
	auto res = working_db.add_index(relName, attrName, indexName);
	if (res.ok) {
		return SUCCESS;
	}
	return res.err;
}

RC DropIndex(char* indexName) {
	auto res = working_db.drop_index(indexName);
	if (res.ok) {
		return SUCCESS;
	}
	return res.err;
}

RC Insert(char* relName, int nValues, Value* values) {
	auto res = working_db.insert(relName, nValues, values);
	if (res.ok) {
		return SUCCESS;
	}
	return res.err;
}
RC Delete(char* relName, int nConditions, Condition* conditions) {
	return FAIL;
}
RC Update(char* relName, char* attrName, Value* value, int nConditions, Condition* conditions) {
	return FAIL;
}
