
#pragma warning(disable : 4996)
#include <stdio.h>
#include <string.h>
#include <vector>
#include "SYS_Manager.h"
#include "QU_Manager.h"


const int PATH_SIZE = 320;

class DataBase {
private:
	char systbl[PATH_SIZE];
	char syscol[PATH_SIZE];
	char sysroot[PATH_SIZE];
	bool opened;

	RM_FileHandle systable;
	RM_FileHandle syscolumn;
	std::vector<RM_FileHandle> tables;

public:
	DataBase() {
		strcpy(systbl, "");
		strcpy(syscol, "");
		strcpy(sysroot, "");
		opened = false;
	}
	bool inuse();
	bool open(const char* const db_root);
	bool close();
};


bool DataBase::inuse() {
	return this->opened;
}


bool DataBase::open (const char* const db_root) {
	// close previous DB if neccessary
	if (this->inuse()) {
		this->close();
	}

	strcpy(sysroot, db_root);
	strcpy(systbl, sysroot);
	strcpy(syscol, sysroot);
	strncat(systbl, "\\SYSTABLE", 15);

	strcpy(syscol, db_root);
	strncat(syscol, "\\SYSCOLUMN", 15);

	RC table_opened = RM_OpenFile(systbl, &this->systable);
	RC column_opened = RM_OpenFile(syscol, &this->syscolumn);

	if (table_opened == SUCCESS && column_opened == SUCCESS) {
		this->opened = true;
		return true;
	}

	this->opened = false;
	return false;
}

bool DataBase::close() {
	if (this->opened) {
		this->opened = false;
		bool systable_closed =
			RM_CloseFile(&this->systable) == SUCCESS ? true : false;
		bool syscolumn_closed =
			RM_CloseFile(&this->syscolumn) == SUCCESS ? true : false;

		bool all_table_closed = true;
		for (auto t : this->tables) {
			all_table_closed &=
				RM_CloseFile(&t) == SUCCESS ? true : false;
		}
		all_table_closed &= systable_closed & syscolumn_closed;
		return all_table_closed;
	}

	// close nothing???
	return true;
}

DataBase working_db;

typedef struct TableRec {
	char tablename[21];
	int  attrcount;
} TableRec;

typedef struct ColumnRec {
	char tablename[21];
	char attrname[21];
	int  attrtype;
	int  attrlength;
	int  attroffset;
	bool ix_flag;
	char indexname[21];
} ColumnRec;


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
	char full_table_path[PATH_SIZE] = "";
	char full_column_path[PATH_SIZE] = "";

	// make path
	strcpy(full_table_path, dbpath);
	strncat(full_table_path, "\\", 1);
	strcpy(full_column_path, full_table_path);

	// make table file and column file
	strncat(full_table_path, "SYSTABLE", 10);
	strncat(full_column_path, "SYSCOLUMN", 10);

	RC table = FAIL;
	RC column = FAIL;

	table = RM_CreateFile(full_table_path, sizeof(TableRec));
	column = RM_CreateFile(full_column_path, sizeof(ColumnRec));

	if (table == SUCCESS && column == SUCCESS) {
		// create both success
		return SUCCESS;
	}

	return FAIL;
}

RC DropDB(char *dbname) {
	RC db_closed = CloseDB();
	char full_path[PATH_SIZE] = "";
	strcpy(full_path, dbname);
	strncat(full_path, "\\SYSTABLE", 15);

	int remove_table_retv = remove(full_path);

	if (remove_table_retv == 0 && db_closed == SUCCESS) {
		// SYSTABLE remove success, indicates this dir is a hust db, can delete
		/*
		   Use system's rmdir command to remove directory
		   STUPID BUT WORKS
		*/
		char command[PATH_SIZE + 30] = "rmdir /s /q ";
		strcat(command, dbname);
		int directory_remove_retv = system(command);
		if (directory_remove_retv == 0) {
			return SUCCESS;
		}
	}

	return FAIL;
}

RC OpenDB(char *dbname) {
	if (working_db.open(dbname)) {
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

