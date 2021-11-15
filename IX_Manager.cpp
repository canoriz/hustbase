#include "IX_Manager.h"

//根据attryype指定的类型比较两块区域的值
int CmpValue(AttrType attrType, char* keyval, char* value);

//查找B+树中属性值可能有value的叶子节点，返回索引项所在页面的页面句柄、页面号和索引项编号
RC FindeIXNode(IX_IndexHandle* indexHandle, char* value, PF_PageHandle* pageHandle, PageNum* pn);

RC CreateIndex(const char* fileName, AttrType attrType, int attrLength)
{
	//创建对应的页面文件
	RC tmp;
	if ((tmp = CreateFile(fileName)) != SUCCESS) {
		return tmp;
	}

	//打开创建的文件
	int fileID;
	if ((tmp = OpenFile((char*)fileName, &fileID)) != SUCCESS) {
		return tmp;
	}

	//获取第一页，加入索引控制信息
	PF_PageHandle* pageHandle = (PF_PageHandle*)malloc(sizeof(PF_PageHandle));
	if ((tmp = AllocatePage(fileID, pageHandle)) != SUCCESS) {
		return tmp;
	}

	IX_FileHeader* fileHeader = (IX_FileHeader*)pageHandle->pFrame->page.pData;
	fileHeader->attrLength = attrLength;
	fileHeader->attrType = attrType;
	fileHeader->keyLength = attrLength + sizeof(RID);
	fileHeader->rootPage = 1;
	fileHeader->first_leaf = 1;
	fileHeader->order = (PF_PAGE_SIZE - sizeof(IX_FileHeader) - sizeof(IX_Node)) / (2 * sizeof(RID) + attrLength);

	IX_Node* IXNode = (IX_Node*)(pageHandle->pFrame->page.pData + sizeof(IX_FileHeader));
	IXNode->is_leaf = 1;
	IXNode->keynum = 0;
	IXNode->parent = 0;
	IXNode->brother = 0;
	IXNode->keys = pageHandle->pFrame->page.pData + sizeof(IX_FileHeader) + sizeof(IX_Node);
	IXNode->rids = (RID*)(IXNode->keys + fileHeader->order * (2 * sizeof(RID) + attrLength));

	//修改页面后调用PF中函数进行处理
	MarkDirty(pageHandle);

	UnpinPage(pageHandle);

	free(pageHandle);

	if ((tmp = CloseFile(fileID)) != SUCCESS) {
		return tmp;
	}

	return SUCCESS;
}

RC OpenIndex(const char* fileName, IX_IndexHandle* indexHandle)
{
	if (indexHandle->bOpen == true) {
		return IX_IHOPENNED;
	}

	//打开文件
	RC tmp;
	int fileID;
	if ((tmp = OpenFile((char*)fileName, &fileID)) != SUCCESS) {
		return tmp;
	}

	//获取索引控制信息
	PF_PageHandle* pageHandle = (PF_PageHandle*)malloc(sizeof(PF_PageHandle));
	if ((tmp = GetThisPage(fileID, 0, pageHandle)) != SUCCESS) {
		return tmp;
	}

	indexHandle->bOpen = true;
	indexHandle->fileID = fileID;
	indexHandle->fileHeader = *(IX_FileHeader*)pageHandle->pFrame->page.pData;

	UnpinPage(pageHandle);

	free(pageHandle);

	return RC();
}

RC CloseIndex(IX_IndexHandle* indexHandle)
{
	if (indexHandle->bOpen == false) {
		return IX_IHCLOSED;
	}

	indexHandle->bOpen = false;
	RC tmp;
	if ((tmp = CloseFile(indexHandle->fileID)) != SUCCESS) {
		return tmp;
	}

	return SUCCESS;
}

RC InsertEntry(IX_IndexHandle* indexHandle, void* pData, const RID* rid)
{
	RC tmp;
	AttrType attrType = indexHandle->fileHeader.attrType;
	int attrLength = indexHandle->fileHeader.attrLength;
	int keyLength = indexHandle->fileHeader.keyLength;
	return RC();
}

RC DeleteEntry(IX_IndexHandle* indexHandle, void* pData, const RID* rid)
{
	RC tmp;
	AttrType attrType = indexHandle->fileHeader.attrType;
	int attrLength = indexHandle->fileHeader.attrLength;
	int keyLength = indexHandle->fileHeader.keyLength;
	return RC();
}

RC OpenIndexScan(IX_IndexScan* indexScan, IX_IndexHandle* indexHandle, CompOp compOp, char* value)
{
	if (indexScan->bOpen == true) {
		return IX_SCANOPENNED;
	}

	RC tmp;
	PF_PageHandle pageHandle;
	PageNum pn;
	
	tmp = FindeIXNode(indexHandle, value, &pageHandle, &pn);
	if (tmp != SUCCESS) return tmp;

	IX_Node* ixNode = (IX_Node*)(pageHandle.pFrame->page.pData + sizeof(IX_FileHeader));
	AttrType attrType = indexHandle->fileHeader.attrType;
	int attrLength = indexHandle->fileHeader.attrLength;
	int keyLength = indexHandle->fileHeader.keyLength;
	char* keyval = (char*)malloc(attrLength);

	int ridIx = 0;
	switch (indexScan->compOp)
	{
	case EQual:
		break;
	case LEqual:
		pn = indexHandle->fileHeader.first_leaf;
		break;
	case NEqual:
		return FAIL;
		break;
	case LessT:
		pn = indexHandle->fileHeader.first_leaf;
		break;
	case GEqual:
		while (ridIx < ixNode->keynum) {
			memcpy(keyval, ixNode->keys + ridIx * keyLength, attrLength);
			int result = CmpValue(attrType, keyval, value);
			if (result < 0) {
				ridIx++;
			}
			else {
				break;
			}
		}
		break;
	case GreatT:
		while (ridIx < ixNode->keynum) {
			//如果读取到最后一个叶子节点的最后一个索引项，就返回EOF
			if (ridIx == ixNode->keynum && ixNode->brother == 0) {
				return FAIL;
			}

			//如果读到该节点最后一个索引项，就切换到下一页面
			if (ridIx == ixNode->keynum) {
				UnpinPage(&pageHandle);
				if ((tmp = GetThisPage(indexHandle->fileID, ixNode->brother, &pageHandle)) != SUCCESS) {
					return tmp;
				}
				pn = ixNode->brother;
				ixNode = (IX_Node*)(pageHandle.pFrame->page.pData + sizeof(IX_FileHeader));
				ridIx = 0;
			}
			memcpy(keyval, ixNode->keys + ridIx * keyLength, attrLength);
			int result = CmpValue(attrType, keyval, value);
			if (result <= 0) {
				ridIx++;
			}
			else {
				break;
			}
		}
		break;
	case NO_OP:
		pn = indexHandle->fileHeader.first_leaf;
		break;
	default:
		break;
	}

	indexScan->bOpen = true;
	indexScan->pIXIndexHandle = indexHandle;
	indexScan->compOp = compOp;
	indexScan->value = value;
	indexScan->PageHandle = pageHandle;
	indexScan->pn = pn;
	indexScan->ridIx = ridIx;

	return SUCCESS;
}

RC IX_GetNextEntry(IX_IndexScan* indexScan, RID* rid)
{
	if (indexScan->bOpen == false) {
		return IX_ISCLOSED;
	}

	//从PH内存中获取IX_Node信息
	IX_Node* ixNode = (IX_Node*)(indexScan->PageHandle.pFrame->page.pData + sizeof(IX_FileHeader));
	RC tmp;

	//如果读取到最后一个叶子节点的最后一个索引项，就返回EOF
	if (indexScan->ridIx == ixNode->keynum && indexScan->pn == 0) {
		return IX_EOF;
	}

	//如果读到该节点最后一个索引项，就切换到下一页面
	if (indexScan->ridIx == ixNode->keynum) {
		UnpinPage(&indexScan->PageHandle);
		if ((tmp = GetThisPage(indexScan->pIXIndexHandle->fileID, indexScan->pn, &indexScan->PageHandle)) != SUCCESS) {
			return tmp;
		}
		indexScan->pn = ixNode->brother;
		indexScan->ridIx = 0;
	}

	//获取属性值的相关信息
	AttrType attrType = indexScan->pIXIndexHandle->fileHeader.attrType;
	int attrLength = indexScan->pIXIndexHandle->fileHeader.attrLength;
	int keyLength = indexScan->pIXIndexHandle->fileHeader.keyLength;
	char*  keyval = (char*)malloc(attrLength);
	memcpy(keyval, ixNode->keys + indexScan->ridIx * keyLength, attrLength);
	int result = CmpValue(attrType, keyval, indexScan->value);
	free(keyval);
	//根据操作符对结果进行处理
	switch (indexScan->compOp)
	{
	case EQual:
		if (result != 0) {
			return IX_NOMOREIDXINMEM;
		}
		break;
	case LEqual:
		if (result > 0) {
			return IX_NOMOREIDXINMEM;
		}
		break;
	case NEqual:
		return FAIL;
		break;
	case LessT:
		if (result >= 0) {
			return IX_NOMOREIDXINMEM;
		}
		break;
	case GEqual:
		break;
	case GreatT:
		break;
	case NO_OP:
		break;
	default:
		break;
	}

	*rid = *(ixNode->rids + indexScan->ridIx);
	indexScan->ridIx++;

	return SUCCESS;
}

RC CloseIndexScan(IX_IndexScan* indexScan)
{
	if (indexScan->bOpen == false) {
		return IX_SCANCLOSED;
	}

	RC tmp;
	indexScan->bOpen = false;

	UnpinPage(&indexScan->PageHandle);

	return SUCCESS;
}

RC GetIndexTree(char* fileName, Tree* index) {
	return RC();
};

int CmpValue(AttrType attrType, char* keyval, char* value) {
	int result;
	float fres = *(float*)keyval - *(float*)value;
	//根据数据类型获取比较结果
	switch (attrType)
	{
	case ints:
		result = *(int*)keyval - *(int*)value;
		break;
	case chars:
		result = strcmp(keyval,value);
		break;
	case floats:
		if (fres < 0) {
			result = -1;
		}
		else if (fres > 0) {
			result = 1;
		}
		else {
			result = 0;
		}
		break;
	default:
		result = strcmp(keyval, value);
		break;
	}

	return result;
}


RC FindeIXNode(IX_IndexHandle* indexHandle, char* value, PF_PageHandle* pageHandle, PageNum* pn) {
	//从根节点开始查找需要的值
	PageNum _pn = indexHandle->fileHeader.rootPage;

	IX_Node* ixNode;
	AttrType attrType = indexHandle->fileHeader.attrType;
	int attrLength = indexHandle->fileHeader.attrLength;
	int keyLength = indexHandle->fileHeader.keyLength;
	char* keyval = (char*)malloc(attrLength);

	PF_PageHandle _pageHandle;
	RC tmp;
	int _ridIx;
	while(1){
		//每次读取页号为_pn的页面进行处理
		if ((tmp = GetThisPage(indexHandle->fileID, _pn, &_pageHandle)) != SUCCESS) {
			return tmp;
		}
		//获取节点信息，如果是叶子节点可直接返回
		ixNode = (IX_Node*)(_pageHandle.pFrame->page.pData + sizeof(IX_FileHeader));
		if (ixNode->is_leaf == 1) {
			break;
		}

		//读取节点的属性值，与给的的值进行比较，属性值小于给定值就向后移动
		int result = -1;
		for (_ridIx = 0; _ridIx < ixNode->keynum; _ridIx++) {
			memcpy(keyval, ixNode->keys + _ridIx * keyLength, attrLength);
			result = CmpValue(attrType, keyval, value);
			if (result < 0) {
				continue;
			}
			else if (result == 0){
				break;
			}
			else {
				if (_ridIx == 0) {
					return IX_INVALIDKEY;
				}
				_ridIx--;
				break;
			}
		}
		_pn = *(int*)(ixNode->rids + _ridIx);
		UnpinPage(&_pageHandle);
	} 
	*pageHandle = _pageHandle;
	free(keyval);
	return SUCCESS;
}
