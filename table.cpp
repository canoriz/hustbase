/* Created by Cao, 2021 Nov 14 */
#define _CRT_SECURE_NO_WARNINGS
#include "table.h"
#include "metadata.h"
#include "result.h"

const int PATH_SIZE = 320;

Result<bool, RC> Table::create(char* path, char* name, int count, AttrInfo* attrs) {
	// metadata path
	if (strlen(name) >= 20) {
		// table name too long
		return Result<bool, RC>::Err(FIELD_NAME_ILLEGAL);
	}
	for (auto i = 0; i < count; i++) {
		AttrInfo i_attr = attrs[i];
		if (strlen(i_attr.attrName) >= 20) {
			// column name too long
			return Result<bool, RC>::Err(FIELD_NAME_ILLEGAL);
		}
		for (auto j = 0; j < i; j++) {
			AttrInfo j_attr = attrs[j];
			const int SAME = 0;
			if (strcmp(j_attr.attrName, i_attr.attrName) == SAME) {
				// same column name
				return Result<bool, RC>::Err(FIELD_NAME_ILLEGAL);
			}
		}
	}

	char full_path[PATH_SIZE] = "";
	strcpy(full_path, path);
	strcat(full_path, "\\.");
	strcat(full_path, name);
	if (!TableMetaData::create_file(full_path)) {
		// failed create metadata
		return Result<bool, RC>::Err(TABLE_EXIST);
	}

	auto res = TableMetaData::open(full_path);
	if (!res.ok) {
		// open metadata failed
		return Result<bool, RC>::Err(FAIL);
	}

	// store metadata
	TableMetaData tmeta = res.result;
	int aggregate_size = 0;
	for (auto i = 0; i < count; i++) {
		AttrInfo tmp_attr = attrs[i];
		int real_length = tmp_attr.attrLength;
		if (tmp_attr.attrType == chars) {
			real_length = tmp_attr.attrLength + 1;
		}
		tmeta.columns.push_back(
			ColumnRec(
				name,
				tmp_attr.attrName, tmp_attr.attrType,
				tmp_attr.attrLength, aggregate_size,
				false, ""
			)
		);
		aggregate_size += real_length;
	}

	tmeta.table.attrcount = count;
	strcpy(tmeta.table.tablename, name);
	tmeta.table.size = aggregate_size;
	if (!tmeta.write(full_path)) {
		// write to file failed
		return Result<bool, RC>::Err(FAIL);
	}

	RC table = FAIL;

	strcpy(full_path, path);
	strcat(full_path, "\\");
	strcat(full_path, name);
	table = RM_CreateFile(full_path, aggregate_size);
	if (table != SUCCESS) {
		return Result<bool, RC>::Err(table);
	}
	return Result<bool, RC>(true);
}

Result<bool, RC> Table::create_prod_unit(char* path, char* name)
{
	return Table::create(path, name, 0, NULL);
}

Result<Table, RC> Table::open(char* path, char* name) {
	char full_path[PATH_SIZE] = "";
	// metadata path
	strcpy(full_path, path);
	strcat(full_path, "\\.");
	strcat(full_path, name);
	Result<TableMetaData, int> open = TableMetaData::open(full_path);
	if (!open.ok) {
		return Result<Table, RC>::Err(TABLE_NOT_EXIST);
	}
	auto& tmeta = open.result;
	if (!tmeta.read()) {
		return Result<Table, RC>::Err(FAIL);
	}

	// table path
	strcpy(full_path, path);
	strcat(full_path, "\\");
	strcat(full_path, name);
	Table t;
	t.meta = tmeta;
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

bool Table::remove_index_flag_on(char* const column)
{
	for(auto &c: this->meta.columns) {
		const int SAME = 0;
		if (strcmp(c.attrname, column) == SAME) {
			if (c.ix_flag) {
				c.ix_flag = false;
				this->dirty = true;
				return true;
			}
			else {
				return false;
			}
		}
	}
	return false;
}

bool Table::add_index_flag_on(char* const column, char* const index)
{
	for (auto& c : this->meta.columns) {
		const int SAME = 0;
		if (strcmp(c.attrname, column) == SAME) {
			if (!c.ix_flag) {
				c.ix_flag = true;
				this->dirty = true;
				strcpy(c.indexname, index);
				return true;
			}
			else {
				return false;
			}
		}
	}
	return false;
}

bool Table::store_metadata_to(char* const path)
{
	return this->meta.write(path);
}

Result<RID, RC> Table::insert_record(char* const data)
{
	RID rid;
	RC insert = InsertRec(&this->file, data, &rid);
	if (insert != SUCCESS) {
		return Result<RID, RC>::Err(insert);
	}
	return Result<RID, RC>::Ok(rid);
}

Result<ColumnRec*, RC> Table::get_column(char* const column)
{
	for (auto& c : this->meta.columns) {
		const int SAME = 0;
		if (strcmp(c.attrname, column) == SAME) {
			return Result<ColumnRec*, RC>::Ok(&c);
		}
	}
	return Result<ColumnRec*, RC>::Err(FAIL);
}

Result<bool, RC> Table::scan_open(RM_FileScan* file_scan, int n_con, Con* conditions)
{
	RC scan_opened = OpenScan(file_scan, &this->file, n_con, conditions);
	if (scan_opened != SUCCESS) {
		return Result<bool, RC>::Err(scan_opened);
	}
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> Table::scan_next(RM_FileScan* file_scan, RM_Record* rec)
{
	RC next_got = GetNextRec(file_scan, rec);
	if (next_got == RM_EOF) {
		// no next record
		return Result<bool, RC>::Ok(false);
	}
	if (next_got != SUCCESS) {
		return Result<bool, RC>::Err(next_got);
	}
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> Table::scan_close(RM_FileScan* file_scan)
{
	RC closed = CloseScan(file_scan);
	if (closed != SUCCESS) {
		return Result<bool, RC>::Err(closed);
	}
	return Result<bool, RC>::Ok(true);
}

bool Table::make_select_result(SelResult* res)
{
	// ugly, stupid and complex
	RM_FileScan file_scan;
	RM_Record rec;
	this->scan_open(&file_scan, 0, NULL);
	auto scan_res = this->scan_next(&file_scan, &rec);

	// initiate column titles
	int initiating_col_n = 0;
	for (auto const& c : this->meta.columns) {
		res->type[initiating_col_n] = (AttrType)c.attrtype;
		res->length[initiating_col_n] = c.attrlength;
		strcpy(res->fields[initiating_col_n], c.attrname);
		initiating_col_n++;
	}

	while (scan_res.ok && scan_res.result) {
		// TODO 100+ make list
		res->col_num = 0;
		res->res[res->row_num] = (char**)malloc(sizeof(char*) * this->meta.columns.size());
		for (auto const& c : this->meta.columns) {
			res->res[res->row_num][res->col_num] = (char*)malloc(c.attrlength);
			memcpy(
				res->res[res->row_num][res->col_num],
				rec.pData + c.attroffset, c.attrlength
			);
			res->col_num++;
		}
		res->row_num++;
		scan_res = this->scan_next(&file_scan, &rec);
	}
	this->scan_close(&file_scan);
	return true;
}

int Table::blk_size()
{
	// one record's size (in bytes)
	return this->meta.table.size;
}

Result<bool, RC> Table::product(Table& b, Table& dest_table)
{
	/* make cartesian product */

	// now, make product
	int blk_sz = this->blk_size() + b.blk_size();
	RM_FileScan scan_a;
	RM_Record rec_a;
	auto scan_a_open = this->scan_open(&scan_a, 0, NULL);
	if (!scan_a_open.ok) {
		// scan this failed
		return Result<bool, RC>::Err(scan_a_open.err);
	}

	char* buf = (char*)malloc(sizeof(char) * blk_sz);
	auto scan_a_res = this->scan_next(&scan_a, &rec_a);
	while (scan_a_res.ok && scan_a_res.result) {
		RM_FileScan scan_b;
		RM_Record rec_b;
		auto scan_b_open = b.scan_open(&scan_b, 0, NULL);
		if (!scan_b_open.ok) {
			// scan B failed
			return Result<bool, RC>::Err(scan_b_open.err);
		}
		auto scan_b_res = b.scan_next(&scan_b, &rec_b);

		while (scan_b_res.ok && scan_b_res.result) {
			memcpy(buf, rec_a.pData, this->blk_size());
			memcpy(buf + this->blk_size(), rec_b.pData, b.blk_size());
			auto insert_rec = dest_table.insert_record(buf);
			if (!insert_rec.ok) {
				return Result<bool, RC>::Err(insert_rec.err);
			}
			scan_b_res = b.scan_next(&scan_b, &rec_b);
		}
		b.scan_close(&scan_b);
		scan_a_res = this->scan_next(&scan_a, &rec_a);
	}
	free(buf);
	this->scan_close(&scan_a);

	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> Table::project(Table& dest)
{
	/* make projection */

	int blk_sz = dest.blk_size();
	RM_FileScan scan;
	RM_Record rec;
	auto scan_opened = this->scan_open(&scan, 0, NULL);
	if (!scan_opened.ok) {
		// scan open failed
		return Result<bool, RC>::Err(scan_opened.err);
	}

	char* buf = (char*)malloc(sizeof(char) * blk_sz);
	auto scan_res = this->scan_next(&scan, &rec);
	while (scan_res.ok && scan_res.result) {
		for (auto const& c : dest.meta.columns) {
			auto from_column_res = this->get_column((char*)c.attrname);
			if (!from_column_res.ok) {
				return Result<bool, RC>::Err(FLIED_NOT_EXIST);
			}
			auto from_column = from_column_res.result;
			memcpy(
				buf + c.attroffset,
				rec.pData + from_column->attroffset,
				from_column->attrlength
			);
		}

		auto insert_rec = dest.insert_record(buf);
		if (!insert_rec.ok) {
			return Result<bool, RC>::Err(insert_rec.err);
		}
		scan_res = this->scan_next(&scan, &rec);
	}
	free(buf);
	this->scan_close(&scan);

	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> Table::select(Table& dest, int n_con, Condition* conditions)
{
	// turn Conditions to Cons
	Con* cons = (Con*)malloc(sizeof(Con) * n_con);
	for (int i = 0; i < n_con; i++) {
		auto res = this->turn_to_con(&conditions[i], &cons[i]);
		if(!res.ok) {
			// cannot convert conditions[i] to cons[i]
			free(cons);
			return res.err;
		}
	}

	RM_FileScan scan;
	RM_Record rec;
	this->scan_open(&scan, n_con, cons);
	auto scan_res = this->scan_next(&scan, &rec);

	while (scan_res.ok && scan_res.result) {
		auto insert_rec = dest.insert_record(rec.pData);
		if (!insert_rec.ok) {
			free(cons);
			return Result<bool, RC>::Err(insert_rec.err);
		}
		scan_res = this->scan_next(&scan, &rec);
	}
	this->scan_close(&scan);
	free(cons);
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> Table::update_match(
	char* const column, Value* v,
	int n, Condition* conditions)
{
	// turn Conditions to Cons
	Con* cons = (Con*)malloc(sizeof(Con) * n);
	for (int i = 0; i < n; i++) {
		auto res = this->turn_to_con(&conditions[i], &cons[i]);
		if (!res.ok) {
			// cannot convert conditions[i] to cons[i]
			free(cons);
			return Result<bool, RC>::Err(res.err);
		}
	}

	RM_FileScan scan;
	RM_Record rec;
	this->scan_open(&scan, n, cons);
	auto scan_res = this->scan_next(&scan, &rec);

	while (scan_res.ok && scan_res.result) {
		auto find_column = this->get_column(column);
		if (!find_column.ok) {
			free(cons);
			this->scan_close(&scan);
			return Result<bool, RC>::Err(find_column.err);
		}
		auto const& col = find_column.result;
		if (v->type != col->attrtype) {
			free(cons);
			this->scan_close(&scan);
			return Result<bool, RC>::Err(TYPE_NOT_MATCH);
		}
		memcpy(rec.pData + col->attroffset, v->data, col->attrlength);
		RC update_res = UpdateRec(&this->file, &rec);
		if (update_res != SUCCESS) {
			free(cons);
			this->scan_close(&scan);
			return Result<bool, RC>::Err(update_res);
		}
		scan_res = this->scan_next(&scan, &rec);
	}
	this->scan_close(&scan);
	free(cons);
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> Table::get_by_rid(RID* rid, RM_Record* rec)
{
	RC r = GetRec(&this->file, rid, rec);
	if (r != SUCCESS) {
		return Result<bool, RC>::Err(r);
	}
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> Table::remove_by_rid(const RID* rid)
{
	RC delete_res = DeleteRec(&this->file, rid);
	if (delete_res != SUCCESS) {
		return Result<bool, RC>::Err(delete_res);
	}
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> Table::remove_match(int n, Condition* conditions)
{
	// turn Conditions to Cons
	Con* cons = (Con*)malloc(sizeof(Con) * n);
	for (int i = 0; i < n; i++) {
		auto res = this->turn_to_con(&conditions[i], &cons[i]);
		if (!res.ok) {
			// cannot convert conditions[i] to cons[i]
			free(cons);
			return res.err;
		}
	}

	RM_FileScan scan;
	RM_Record rec;
	this->scan_open(&scan, n, cons);
	auto scan_res = this->scan_next(&scan, &rec);

	while (scan_res.ok && scan_res.result) {
		auto delete_res = this->remove_by_rid(&rec.rid);
		if (!delete_res.ok) {
			free(cons);
			this->scan_close(&scan);
			return Result<bool, RC>::Err(delete_res.err);
		}
		scan_res = this->scan_next(&scan, &rec);
	}
	this->scan_close(&scan);
	free(cons);
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> Table::turn_to_con(Condition* cond, Con* con)
{
	con->bLhsIsAttr = cond->bLhsIsAttr;
	con->bRhsIsAttr = cond->bRhsIsAttr;
	con->compOp = cond->op;
	char full_name[PATH_SIZE];
	if (cond->bLhsIsAttr) {
		if (cond->lhsAttr.relName) {
			strcpy(full_name, cond->lhsAttr.relName);
			strcat(full_name, ".");
			strcat(full_name, cond->lhsAttr.attrName);
		}
		else {
			strcpy(full_name, cond->lhsAttr.attrName);
		}
		auto res = this->get_column(full_name);
		if (!res.ok) {
			return Result<bool, RC>::Err(FLIED_NOT_EXIST);
		}
		auto const& col = res.result;
		con->attrType    = (AttrType)col->attrtype;
		con->LattrLength = col->attrlength;
		con->LattrOffset = col->attroffset;

	}
	else {
		con->Lvalue   = cond->lhsValue.data;
		con->attrType = cond->lhsValue.type;
	}

	if (cond->bRhsIsAttr) {
		if (cond->rhsAttr.relName) {
			strcpy(full_name, cond->rhsAttr.relName);
			strcat(full_name, ".");
			strcat(full_name, cond->rhsAttr.attrName);
		}
		else {
			strcpy(full_name, cond->rhsAttr.attrName);
		}
		auto res = this->get_column(full_name);
		if (!res.ok) {
			return Result<bool, RC>::Err(FLIED_NOT_EXIST);
		}
		auto const& col = res.result;
		if ((AttrType)col->attrtype != con->attrType) {
			return Result<bool, RC>::Err(TYPE_NOT_MATCH);
		}
		con->RattrLength = col->attrlength;
		con->RattrOffset = col->attroffset;
	}
	else {
		if (cond->rhsValue.type != con->attrType) {
			return Result<bool, RC>::Err(TYPE_NOT_MATCH);
		}
		con->Rvalue = cond->rhsValue.data;
	}
	return Result<bool, RC>::Ok(true);
}
