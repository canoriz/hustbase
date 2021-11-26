#include "QU_Manager.h"
#include "database.h"
#pragma warning(disable : 4996)

extern DataBase working_db;

RC Query(char * sql, SelResult * res) {
	sqlstr* processing_sql = NULL;
	RC rc;
	processing_sql = get_sqlstr();
	rc = parse(sql, processing_sql);//只有两种返回结果SUCCESS和SQL_SYNTAX
	if (rc == SUCCESS) {
		selects* select = &(processing_sql->sstr.sel);
		switch (processing_sql->flag) {
		case 1:
			//判断SQL语句为select语句
			return Select(
				select->nSelAttrs, select->selAttrs,
				select->nRelations, select->relations,
				select->nConditions, select->conditions, res
			);
			break;
		}
	}

	return FAIL;
}

RC Select(
	int nSelAttrs, RelAttr** selAttrs, /* []*RelAttr  columns */
	int nRelations, char** relations,  /* *[]char tables */
	int nConditions, Condition* conditions, /* []Condition conditions */
	SelResult* res)
{
	auto query_ok = working_db.query(
		nSelAttrs, selAttrs,
		nRelations, relations,
		nConditions, conditions,
		res
	);

	if (!query_ok.ok) {
		return query_ok.err;
	}
	return SUCCESS;
	// testing
	/*
	res->row_num = 1;
	res->col_num = 4;
	res->type[0] = ints;
	res->type[1] = ints;
	res->type[2] = floats;
	res->type[3] = chars;
	res->length[0] = 4;
	res->length[1] = 4;
	res->length[2] = 4;
	res->length[3] = 10;
	strcpy(res->fields[0], "testing-int");
	strcpy(res->fields[1], "testing-int2");
	strcpy(res->fields[2], "testing-float1");
	strcpy(res->fields[3], "testing-str");
	res->res[0] = (char**)malloc(sizeof(char*) * 10);
	res->res[0][0] = (char*)malloc(sizeof(int));
	*(int* )res->res[0][0] = 42;

	res->res[0][1] = (char*)malloc(sizeof(int));
	*(int*)res->res[0][1] = 43;

	res->res[0][2] = (char*)malloc(sizeof(float));
	*(float*)res->res[0][2] = 4.453;

	res->res[0][3] = (char*)malloc(sizeof(char)*10);
	strcpy(res->res[0][3], "hustdb");
	res->next_res = NULL;
	return SUCCESS;
	return FAIL;
	*/
}
