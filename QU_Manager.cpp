#include "QU_Manager.h"
#pragma warning(disable : 4996)


RC Query(char * sql, SelResult * res) {
	sqlstr* processing_sql = NULL;
	RC rc;
	processing_sql = get_sqlstr();
	rc = parse(sql, processing_sql);//ֻ�����ַ��ؽ��SUCCESS��SQL_SYNTAX
	if (rc == SUCCESS) {
		selects* select = &(processing_sql->sstr.sel);
		switch (processing_sql->flag) {
		case 1:
			//�ж�SQL���Ϊselect���
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
	int nSelAttrs, RelAttr** selAttrs, /* RelAttr*[]  columns */
	int nRelations, char** relations,  /* *[]char tables */
	int nConditions, Condition* conditions, /* conditions[] conditions */
	SelResult* res)
{
	return FAIL;
}
