/* Created by Cao, 2021 Nov 14 */
#define _CRT_SECURE_NO_WARNINGS
#include "table.h"
#include "metadata.h"
#include "result.h"

const int PATH_SIZE = 320;

Result<bool, RC> Table::create(char* path, char* name, int count, AttrInfo* attrs) {
	// metadata path
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

Result<Table, RC> Table::product(Table& b, Table& dest_table)
{
	/* make cartesian product */

	// now, make product
	int blk_sz = this->blk_size() + b.blk_size();
	RM_FileScan scan_a;
	RM_Record rec_a;
	auto scan_a_open = this->scan_open(&scan_a, 0, NULL);
	if (!scan_a_open.ok) {
		// scan this failed
		return Result<Table, RC>::Err(scan_a_open.err);
	}

	char* buf = (char*)malloc(sizeof(char) * blk_sz);
	auto scan_a_res = this->scan_next(&scan_a, &rec_a);
	while (scan_a_res.ok && scan_a_res.result) {
		RM_FileScan scan_b;
		RM_Record rec_b;
		auto scan_b_open = b.scan_open(&scan_b, 0, NULL);
		if (!scan_b_open.ok) {
			// scan B failed
			return Result<Table, RC>::Err(scan_b_open.err);
		}
		auto scan_b_res = b.scan_next(&scan_b, &rec_b);

		while (scan_b_res.ok && scan_b_res.result) {
			memcpy(buf, rec_a.pData, this->blk_size());
			memcpy(buf + this->blk_size(), rec_b.pData, b.blk_size());
			auto insert_rec = dest_table.insert_record(buf);
			if (!insert_rec.ok) {
				return Result<Table, RC>::Err(insert_rec.err);
			}
			scan_b_res = b.scan_next(&scan_b, &rec_b);
		}
		b.scan_close(&scan_b);
		scan_a_res = this->scan_next(&scan_a, &rec_a);
	}
	free(buf);
	this->scan_close(&scan_a);

	return Result<Table, RC>::Ok(dest_table);
}

Result<Table, RC> Table::project(Table& dest)
{
	/* make projection */

	int blk_sz = dest.blk_size();
	RM_FileScan scan;
	RM_Record rec;
	auto scan_opened = this->scan_open(&scan, 0, NULL);
	if (!scan_opened.ok) {
		// scan open failed
		return Result<Table, RC>::Err(scan_opened.err);
	}

	char* buf = (char*)malloc(sizeof(char) * blk_sz);
	auto scan_res = this->scan_next(&scan, &rec);
	while (scan_res.ok && scan_res.result) {
		int offset = 0;
		for (auto const& c : dest.meta.columns) {
			auto from_column_res = this->get_column((char*)c.attrname);
			if (!from_column_res.ok) {
				return Result<Table, RC>::Err(FLIED_NOT_EXIST);
			}
			auto from_column = from_column_res.result;
			memcpy(
				buf + offset,
				rec.pData + from_column->attroffset,
				from_column->attrlength
			);
			offset += from_column->attrlength;
		}

		auto insert_rec = dest.insert_record(buf);
		if (!insert_rec.ok) {
			return Result<Table, RC>::Err(insert_rec.err);
		}
		scan_res = this->scan_next(&scan, &rec);
	}
	free(buf);
	this->scan_close(&scan);

	return Result<Table, RC>::Ok(dest);
}
