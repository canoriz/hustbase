#include "IX_Manager.h"

#define PTR_SIZE sizeof(RID)

struct IndexInfo{
	AttrType attrType;
	int attrLength;
	IndexInfo(IX_IndexHandle* indexHandle): attrType(indexHandle->fileHeader.attrType), attrLength(indexHandle->fileHeader.attrLength) {
	}
};

//根据attryype指定的类型比较两块区域的值
int CmpValue(AttrType attrType, char* keyval, char* value);

//查找B+树中属性值可能有value的叶子节点，返回索引项所在页面的页面句柄、页面号
RC FindFirstIXNode(IX_IndexHandle* indexHandle, char* value, PF_PageHandle* pageHandle);

//根据属性值和RID的值精确查找索引项可能在的页面，返回页面句柄和具体位置
RC FindIXNode(IX_IndexHandle* indexHandle, char* keyval, const RID* ridval, PF_PageHandle* pageHandle);

//根据属性值和页号插入内容到具体的叶子节点中
RC InsertEntryInLeaf(IX_IndexHandle* indexHandle, void* pData, const RID* rid, PageNum pn);

//根据属性值和页号插入内容到具体的非叶子节点中
RC InsertEntryInParent(IX_IndexHandle* indexHandle, void* pData, const RID* rid, PageNum pn);

//根据属性值和页号删除节点中的内容
RC DeleteEntry(IX_IndexHandle* indexHandle, void* pData, const RID* rid, PageNum pn);

//根据pageHandle获取页面中的IX_Node节点信息
IX_Node* getIXNodefromPH(const PF_PageHandle* pageHandle) {
	return (IX_Node*)(pageHandle->pFrame->page.pData + sizeof(IX_FileHeader));
}


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
	PF_PageHandle pageHandle;
	if ((tmp = AllocatePage(fileID, &pageHandle)) != SUCCESS) {
		return tmp;
	}

	IX_FileHeader* fileHeader = (IX_FileHeader*)pageHandle.pFrame->page.pData;
	fileHeader->attrLength = attrLength;
	fileHeader->attrType = attrType;
	fileHeader->rootPage = 1;
	fileHeader->first_leaf = 1;
	fileHeader->order = (PF_PAGE_SIZE - sizeof(IX_FileHeader) - sizeof(IX_Node) - PTR_SIZE) / (PTR_SIZE + attrLength);

	IX_Node* IXNode = getIXNodefromPH(&pageHandle);
	IXNode->is_leaf = 1;
	IXNode->keynum = 0;
	IXNode->parent = 0;
	IXNode->brother = 0;
	IXNode->keys = pageHandle.pFrame->page.pData + sizeof(IX_FileHeader) + sizeof(IX_Node);
	IXNode->rids = (RID*)(IXNode->keys + fileHeader->order * attrLength);

	//修改页面后调用PF中函数进行处理
	MarkDirty(&pageHandle);

	UnpinPage(&pageHandle);

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
	if (indexHandle->bOpen == false) {
		return IX_IHCLOSED;
	}

	RC tmp;
	IndexInfo indexinfo(indexHandle);
	int order = indexHandle->fileHeader.order;
	PF_PageHandle pageHandle;

	FindFirstIXNode(indexHandle, (char*)pData, &pageHandle);

	int pn = pageHandle.pFrame->page.pageNum;

	UnpinPage(&pageHandle);

	InsertEntryInLeaf(indexHandle, pData, rid, pn);
	
	return SUCCESS;
}

RC DeleteEntry(IX_IndexHandle* indexHandle, void* pData, const RID* rid)
{
	if (indexHandle->bOpen == false) {
		return IX_IHCLOSED;
	}

	RC tmp;
	PF_PageHandle pageHandle;

	tmp = FindIXNode(indexHandle, (char*)pData, rid, &pageHandle);
	if (tmp != SUCCESS) {
		return IX_NOMEM;
	}
	
	int pn = pageHandle.pFrame->page.pageNum;

	UnpinPage(&pageHandle);

	DeleteEntry(indexHandle, pData, rid, pn);

	return SUCCESS;
}

RC OpenIndexScan(IX_IndexScan* indexScan, IX_IndexHandle* indexHandle, CompOp compOp, char* value)
{
	if (indexScan->bOpen == true) {
		return IX_SCANOPENNED;
	}

	RC tmp;
	PF_PageHandle pageHandle;
	PageNum pn;
	
	tmp = FindFirstIXNode(indexHandle, value, &pageHandle);
	if (tmp != SUCCESS) return tmp;

	IX_Node* ixNode = getIXNodefromPH(&pageHandle);
	pn = ixNode->brother;
	IndexInfo indexinfo(indexHandle);
	char* keyval = (char*)malloc(indexinfo.attrLength);

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
			memmove(keyval, ixNode->keys + ridIx * indexinfo.attrLength, indexinfo.attrLength);
			int result = CmpValue(indexinfo.attrType, keyval, value);
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
				ixNode = getIXNodefromPH(&pageHandle);
				ridIx = 0;
			}
			memmove(keyval, ixNode->keys + ridIx * indexinfo.attrLength, indexinfo.attrLength);
			int result = CmpValue(indexinfo.attrType, keyval, value);
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
	IX_Node* ixNode = getIXNodefromPH(&indexScan->PageHandle);
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
	IndexInfo indexinfo(indexScan->pIXIndexHandle);
	char*  keyval = (char*)malloc(indexinfo.attrLength);
	memmove(keyval, ixNode->keys + indexScan->ridIx * indexinfo.attrLength, indexinfo.attrLength);
	int result = CmpValue(indexinfo.attrType, keyval, indexScan->value);
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


RC FindFirstIXNode(IX_IndexHandle* indexHandle, char* value, PF_PageHandle* pageHandle) {
	//从根节点开始查找需要的值
	PageNum _pn = indexHandle->fileHeader.rootPage;

	IX_Node* ixNode;
	IndexInfo indexinfo(indexHandle);
	char* _keyval = (char*)malloc(indexinfo.attrLength);

	PF_PageHandle _pageHandle;
	RC tmp;
	int _ridIx;
	while(1){

		//每次读取页号为_pn的页面进行处理
		if ((tmp = GetThisPage(indexHandle->fileID, _pn, &_pageHandle)) != SUCCESS) {
			free(_keyval);
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
			memmove(_keyval, ixNode->keys + _ridIx * indexinfo.attrLength, indexinfo.attrLength);
			result = CmpValue(indexinfo.attrType, _keyval, value);
			if (result >= 0) {
				break;
			}
		}
		_pn = *(PageNum*)(ixNode->rids + _ridIx);
		UnpinPage(&_pageHandle);
	} 
	*pageHandle = _pageHandle;
	free(_keyval);
	return SUCCESS;
}

RC FindIXNodeInRid(IX_IndexHandle* indexHandle, char* keyval, const RID* ridval, PF_PageHandle* pageHandle) {
	//首先调用不对RID进行判断的FindIXNode对keyval进行范围排除
	RC tmp;
	PF_PageHandle _pageHandle;
	PageNum _pn;
	if ((tmp = FindFirstIXNode(indexHandle, keyval, &_pageHandle)) != SUCCESS) {
		return tmp;
	}
	
	//通过indexHandle读取索引的相关信息
	IX_Node* ixNode;
	IndexInfo indexinfo(indexHandle);
	char* _keyval = (char*)malloc(indexinfo.attrLength);
	RID _ridval;
	
	int _ridIx = 0;
	int success = 0, fail = 0;//用于跳出循环
	while (!success && !fail) {
		//获取节点信息
		ixNode = getIXNodefromPH(&_pageHandle);

		//读取节点的属性值，与给的的值进行比较，属性值小于给定值就向后移动
		for (_ridIx = 0; _ridIx < ixNode->keynum && success && fail; _ridIx++) {
			memmove(_keyval, ixNode->keys + _ridIx * indexinfo.attrLength, indexinfo.attrLength);
			int result1 = CmpValue(indexinfo.attrType, _keyval, keyval);
			if (result1 == 0) {
				memmove(&_ridval, ixNode->rids + _ridIx, indexinfo.attrLength);
				if (memcmp(&_ridval, ridval, indexinfo.attrLength) == 0) {
					*pageHandle = _pageHandle;
					success = 1;
				}
			}
			else if (result1 < 0) {
				continue;
			}
			else {
				fail = 1;
			}
		}

		//读到最后也没找到就切换下一个叶子节点
		_pn = ixNode->brother;
		_ridIx = 0;
		UnpinPage(&_pageHandle);

		//如果读取到最后一个叶子节点就直接返回
		if (_pn == 0) {
			fail = 1;
		}

		if ((tmp = GetThisPage(indexHandle->fileID, _pn, &_pageHandle)) != SUCCESS) {
			free(_keyval);
			return tmp;
		}
	}
	
	free(_keyval);
	if (fail) {
		return FAIL;
	}
	*pageHandle = _pageHandle;
	return SUCCESS;
}


RC InsertEntryInLeaf(IX_IndexHandle* indexHandle, void* pData, const RID* rid, PageNum pn) {

	RC tmp;
	IndexInfo indexinfo(indexHandle);
	int order = indexHandle->fileHeader.order;
	PF_PageHandle pageHandle;

	if ((tmp = GetThisPage(indexHandle->fileID, pn, &pageHandle)) != SUCCESS) {
		return tmp;
	}

	IX_Node* ixNode = getIXNodefromPH(&pageHandle);

	char* keyval = (char*)malloc(indexinfo.attrLength);
	int i;
	for (i = 0; i < ixNode->keynum; i++) {
		memmove(keyval, ixNode->keys + i * indexinfo.attrLength, indexinfo.attrLength);
		int result = CmpValue(indexinfo.attrType, keyval, (char*)pData);
		if (result >= 0) {
			break;
		}
	}
	int _index = i;
	free(keyval);

	//如果达到了分支上限，就进行分裂
	if (ixNode->keynum == order) {
		PF_PageHandle newpageHandle;
		if ((tmp = AllocatePage(indexHandle->fileID, &newpageHandle)) != SUCCESS) {
			return tmp;
		}

		//创建新节点，修改对应的成员的值
		IX_Node* newixNode = getIXNodefromPH(&newpageHandle);
		newixNode->is_leaf = ixNode->is_leaf;
		newixNode->brother = ixNode->brother;
		ixNode->brother = newpageHandle.pFrame->page.pageNum;
		newixNode->parent = pageHandle.pFrame->page.pageNum;
		newixNode->keys = newpageHandle.pFrame->page.pData + sizeof(IX_FileHeader) + sizeof(IX_Node);
		newixNode->rids = (RID*)(newixNode->keys + order * indexinfo.attrLength);

		//把前一节点的后半内容转移到新节点中，原有空间清空
		memmove(newixNode->keys, ixNode->keys + order / 2 * indexinfo.attrLength, (order + 1) / 2 * indexinfo.attrLength);
		memset(ixNode->keys + order / 2 * indexinfo.attrLength, 0, (order + 1) / 2 * indexinfo.attrLength);
		memmove(newixNode->rids, ixNode->rids + order / 2, (order + 1) / 2 * PTR_SIZE);
		memset(ixNode->rids + order / 2, 0, (order + 1) / 2 * PTR_SIZE);
		newixNode->keynum = (order + 1) / 2;
		ixNode->keynum -= newixNode->keynum;

		//根据插入节点位置来选择插入在新节点还是旧节点中
		if (_index <= order / 2) {
			memmove(ixNode->keys + (_index + 1) * indexinfo.attrLength, ixNode->keys + _index * indexinfo.attrLength, order / 2 * indexinfo.attrLength);
			memmove(ixNode->keys + _index * indexinfo.attrLength, pData, indexinfo.attrLength);
			memmove(ixNode->rids + _index + 1, ixNode->rids + _index, order / 2 * PTR_SIZE);
			memmove(ixNode->rids + _index, rid, PTR_SIZE);
			ixNode->keynum++;
		}
		else {
			_index -= order / 2;
			memmove(newixNode->keys + (_index + 1) * indexinfo.attrLength, newixNode->keys + _index * indexinfo.attrLength, order / 2 * indexinfo.attrLength);
			memmove(newixNode->keys + _index * indexinfo.attrLength, pData, indexinfo.attrLength);
			memmove(newixNode->rids + _index + 1, newixNode->rids + _index, order / 2 * PTR_SIZE);
			memmove(newixNode->rids + _index, rid, PTR_SIZE);
			newixNode->keynum++;
		}

		int pageNum = newpageHandle.pFrame->page.pageNum;
		void* data = malloc(indexinfo.attrLength);
		memmove(data, newixNode->keys, indexinfo.attrLength);

		MarkDirty(&newpageHandle);
		UnpinPage(&newpageHandle);

		//如果当前分裂的是根节点，那么就要产生新的根节点
		if (pageHandle.pFrame->page.pageNum != indexHandle->fileHeader.rootPage) {
			 InsertEntryInParent(indexHandle, data, (RID*)&pageNum, ixNode->parent);
		}
		else {
			PF_PageHandle rootpageHandle;
			if ((tmp = AllocatePage(indexHandle->fileID, &rootpageHandle)) != SUCCESS) {
				return tmp;
			}
			IX_Node* rootixNode = getIXNodefromPH(&rootpageHandle);
			rootixNode->keys = rootpageHandle.pFrame->page.pData + sizeof(IX_FileHeader) + sizeof(IX_Node);
			rootixNode->rids = (RID*)(rootixNode->keys + order * indexinfo.attrLength);
			rootixNode->brother = 0;
			rootixNode->is_leaf = 0;
			rootixNode->parent = 0;
			indexHandle->fileHeader.rootPage = rootpageHandle.pFrame->page.pageNum;
			ixNode->parent = rootpageHandle.pFrame->page.pageNum;
			newixNode->parent = rootpageHandle.pFrame->page.pageNum;
			memmove(rootixNode->keys, data, indexinfo.attrLength);
			memmove(rootixNode->rids, (RID*)&pageNum, PTR_SIZE);
			rootixNode->keynum++;
		}

		free(data);
	}
	else {
		memmove(ixNode->keys + (_index + 1) * indexinfo.attrLength, ixNode->keys + _index * indexinfo.attrLength, (ixNode->keynum - _index) * indexinfo.attrLength);
		memmove(ixNode->keys + _index * indexinfo.attrLength, pData, indexinfo.attrLength);
		memmove(ixNode->rids + _index + 1, ixNode->rids + _index, (ixNode->keynum - _index) * PTR_SIZE);
		memmove(ixNode->rids + _index, rid, PTR_SIZE);
		ixNode->keynum++;
	}

	MarkDirty(&pageHandle);
	UnpinPage(&pageHandle);

	return SUCCESS;
}

RC InsertEntryInParent(IX_IndexHandle* indexHandle, void* pData, const RID* rid, PageNum pn) {

	RC tmp;
	IndexInfo indexinfo(indexHandle);
	int order = indexHandle->fileHeader.order;
	PF_PageHandle pageHandle;

	if ((tmp = GetThisPage(indexHandle->fileID, pn, &pageHandle)) != SUCCESS) {
		return tmp;
	}

	IX_Node* ixNode = getIXNodefromPH(&pageHandle);

	char* keyval = (char*)malloc(indexinfo.attrLength);
	int i;
	for (i = 0; i < ixNode->keynum; i++) {
		memmove(keyval, ixNode->keys + i * indexinfo.attrLength, indexinfo.attrLength);
		int result = CmpValue(indexinfo.attrType, keyval, (char*)pData);
		if (result >= 0) {
			break;
		}
	}
	int _index = i;
	free(keyval);

	//如果达到了分支上限，就进行分裂
	if (ixNode->keynum == order) {
		PF_PageHandle newpageHandle;
		if ((tmp = AllocatePage(indexHandle->fileID, &newpageHandle)) != SUCCESS) {
			return tmp;
		}

		//创建新节点，修改对应的成员的值
		IX_Node* newixNode = getIXNodefromPH(&newpageHandle);
		newixNode->is_leaf = ixNode->is_leaf;
		newixNode->brother = ixNode->brother;
		ixNode->brother = newpageHandle.pFrame->page.pageNum;
		newixNode->parent = pageHandle.pFrame->page.pageNum;
		newixNode->keys = newpageHandle.pFrame->page.pData + sizeof(IX_FileHeader) + sizeof(IX_Node);
		newixNode->rids = (RID*)(newixNode->keys + order * indexinfo.attrLength);

		int pageNum = newpageHandle.pFrame->page.pageNum;
		void* data = malloc(indexinfo.attrLength);

		//根据插入节点位置来选择插入在新节点还是旧节点中，并且选择插入到上层的内容
		if (_index == order / 2) {
			//情况一，将插入节点值作为插入值传递给父节点
			memmove(data, pData, indexinfo.attrLength);

			//将原节点后半索引项移入新节点
			memmove(newixNode->keys, ixNode->keys + order / 2 * indexinfo.attrLength, (order + 1) / 2 * indexinfo.attrLength);
			memset(ixNode->keys + order / 2 * indexinfo.attrLength, 0, (order + 1) / 2 * indexinfo.attrLength);
			//移动指针项
			memmove(newixNode->rids + 1, ixNode->rids + order / 2, (order + 1) / 2 * PTR_SIZE);
			memset(ixNode->rids + order / 2, 0, (order + 1) / 2 * PTR_SIZE);
			memmove(newixNode->rids, rid, PTR_SIZE);
		}
		else if (_index < order / 2) {
			//情况二，将order/2 - 1位置的节点作为插入值传递给父节点
			memmove(data, ixNode->keys + (order / 2 - 1) * indexinfo.attrLength, indexinfo.attrLength);

			//移动索引项到新节点
			memmove(newixNode->keys, ixNode->keys + order / 2 * indexinfo.attrLength, (order + 1) / 2 * indexinfo.attrLength);
			memset(ixNode->keys + order / 2 * indexinfo.attrLength, 0, (order + 1) / 2 * indexinfo.attrLength);
			//插入新索引项
			memmove(ixNode->keys + (_index + 1) * indexinfo.attrLength, ixNode->keys + _index * indexinfo.attrLength, order / 2 * indexinfo.attrLength);
			memmove(ixNode->keys + _index * indexinfo.attrLength, pData, indexinfo.attrLength);
			//移动指针项到新节点
			memmove(newixNode->rids, ixNode->rids + order / 2, (order + 3) / 2 * PTR_SIZE);
			memset(ixNode->rids + order / 2, 0, (order + 3) / 2 * PTR_SIZE);
			//插入新指针项
			memmove(ixNode->rids + _index + 2, ixNode->rids + _index + 1, (order + 3) / 2 * PTR_SIZE);
			memmove(ixNode->rids + _index + 1, rid, PTR_SIZE);
		}
		else {
			//情况三，将order/2位置的节点作为插入值传递给父节点
			_index -= order / 2 + 1;
			memmove(data, ixNode->keys + order / 2 * indexinfo.attrLength, indexinfo.attrLength);

			//移动索引项到新节点
			memmove(newixNode->keys, ixNode->keys + (order / 2 + 1) * indexinfo.attrLength, (order - 1) / 2 * indexinfo.attrLength);
			memset(ixNode->keys + (order / 2 + 1) * indexinfo.attrLength, 0, (order - 1) / 2 * indexinfo.attrLength);
			//插入新索引项
			memmove(newixNode->keys + (_index + 1) * indexinfo.attrLength, newixNode->keys + _index * indexinfo.attrLength, (order - 1) / 2 * indexinfo.attrLength);
			memmove(newixNode->keys + _index * indexinfo.attrLength, pData, indexinfo.attrLength);
			//移动指针项到新节点
			memmove(newixNode->rids, ixNode->rids + order / 2 + 1, (order + 1) / 2 * PTR_SIZE);
			memset(ixNode->rids + order / 2 + 1, 0, (order + 1) / 2 * PTR_SIZE);
			//插入新指针项
			memmove(newixNode->rids + _index + 2, newixNode->rids + _index + 1, (order + 1) / 2 * PTR_SIZE);
			memmove(newixNode->rids + _index + 1, rid, PTR_SIZE);
		}

		newixNode->keynum = (order + 1) / 2;
		ixNode->keynum -= newixNode->keynum;

		MarkDirty(&newpageHandle);
		UnpinPage(&newpageHandle);

		//如果当前分裂的是根节点，那么就要产生新的根节点
		if (pageHandle.pFrame->page.pageNum != indexHandle->fileHeader.rootPage) {
			InsertEntryInParent(indexHandle, data, (RID*)&pageNum, ixNode->parent);
		}
		else {
			PF_PageHandle rootpageHandle;
			if ((tmp = AllocatePage(indexHandle->fileID, &rootpageHandle)) != SUCCESS) {
				return tmp;
			}
			IX_Node* rootixNode = getIXNodefromPH(&rootpageHandle);
			rootixNode->keys = rootpageHandle.pFrame->page.pData + sizeof(IX_FileHeader) + sizeof(IX_Node);
			rootixNode->rids = (RID*)(rootixNode->keys + order * indexinfo.attrLength);
			rootixNode->brother = 0;
			rootixNode->is_leaf = 0;
			rootixNode->parent = 0;
			indexHandle->fileHeader.rootPage = rootpageHandle.pFrame->page.pageNum;
			ixNode->parent = rootpageHandle.pFrame->page.pageNum;
			newixNode->parent = rootpageHandle.pFrame->page.pageNum;
			memmove(rootixNode->keys, data, indexinfo.attrLength);
			memmove(rootixNode->rids, (RID*)&pageNum, PTR_SIZE);
			rootixNode->keynum++;
		}

		free(data);
	}
	else {
		memmove(ixNode->keys + (_index + 1) * indexinfo.attrLength, ixNode->keys + _index * indexinfo.attrLength, (ixNode->keynum - _index) * indexinfo.attrLength);
		memmove(ixNode->keys + _index * indexinfo.attrLength, pData, indexinfo.attrLength);
		memmove(ixNode->rids + _index + 2, ixNode->rids + _index + 1, (ixNode->keynum - _index) * PTR_SIZE);
		memmove(ixNode->rids + _index + 1, rid, PTR_SIZE);
		ixNode->keynum++;
	}

	MarkDirty(&pageHandle);
	UnpinPage(&pageHandle);

	return SUCCESS;
}


RC DeleteEntry(IX_IndexHandle* indexHandle, void* pData, const RID* rid, PageNum pn) {
	RC tmp;
	IndexInfo indexinfo(indexHandle);
	int order = indexHandle->fileHeader.order;
	PF_PageHandle pageHandle;

	tmp = GetThisPage(indexHandle->fileID, pn, &pageHandle);
	if (tmp != SUCCESS) {
		return SUCCESS;
	}

	IX_Node* ixNode = getIXNodefromPH(&pageHandle);
	char* keyval = (char*)malloc(indexinfo.attrLength);
	int ridIx;
	int i;
	for (i = 0; i < ixNode->keynum; i++) {
		memmove(keyval, ixNode->keys + i * indexinfo.attrLength, indexinfo.attrLength);
		int result = CmpValue(indexinfo.attrType, keyval, (char*)pData);
		if (result == 0) {
			break;
		}
	}
	free(keyval);
	ridIx = i;

	
	//从节点中删除找到的节点
	ixNode->keynum--;
	//删除索引项
	memmove(ixNode->keys + ridIx * indexinfo.attrLength, ixNode->keys + (ridIx + 1) * indexinfo.attrLength, (ixNode->keynum - ridIx) * indexinfo.attrLength);
	memset(ixNode->keys + ixNode->keynum * indexinfo.attrLength, 0, (order - ixNode->keynum) * indexinfo.attrLength);


	//删除指针项，需要区分叶子节点和非叶子节点两种情况
	if (ixNode->is_leaf) {
		memmove(ixNode->rids + ridIx, ixNode->rids + ridIx + 1, ixNode->keynum - ridIx);
		memset(ixNode->rids + ixNode->keynum + 1, 0, (order - ixNode->keynum) * indexinfo.attrLength);
	}
	else {
		memmove(ixNode->rids + ridIx + 1, ixNode->rids + ridIx + 2, ixNode->keynum - ridIx - 1);
		memset(ixNode->rids + ixNode->keynum + 1, 0, (order - ixNode->keynum) * indexinfo.attrLength);
	}
	
	//如果当前节点是根节点且分支为一，则子节点成为新根节点
	if (pn == indexHandle->fileHeader.rootPage && ixNode->keynum == 1) {
		indexHandle->fileHeader.rootPage = *(PageNum*)ixNode->rids;
		MarkDirty(&pageHandle);
		UnpinPage(&pageHandle);
		DisposePage(indexHandle->fileID, pn);
		return SUCCESS;
	}

	//判断删除后节点数是否符合要求，过空就需要考虑删除节点
	if (indexHandle->fileHeader.rootPage != pn && ixNode->keynum < order / 2) {
		//获取父亲节点
		PF_PageHandle fapageHandle;

		tmp = GetThisPage(indexHandle->fileID, ixNode->parent, &fapageHandle);
		if (tmp != SUCCESS) {
			return SUCCESS;
		}
		IX_Node* faixNode = getIXNodefromPH(&fapageHandle);

		//在父节点中查找要删除的子节点的位置，获取其前后兄弟的信息
		char* ridval = (char*)malloc(PTR_SIZE);
		int i;
		for (i = 0; i <= faixNode->keynum; i++) {
			memmove(ridval, faixNode->rids + i, PTR_SIZE);
			int result = CmpValue(indexinfo.attrType, ridval, (char*)pData);
			if (result >= 0) {
				break;
			}
		}
		free(ridval);
		ridIx = i;
		char* keydata = (char*)malloc(indexinfo.attrLength);
		

		int broPn, poz = -1;//broId用来标识兄弟节点的页面号，poz用来指示兄弟节点的位置
		if (ridIx != 0){
			broPn = *(int*)(faixNode->rids + ridIx - 1);
			poz = 0;
			memmove(keydata, faixNode->keys + (ridIx - 1) * indexinfo.attrLength, indexinfo.attrLength);
		}
		else {
			broPn = *(int*)(faixNode->rids + ridIx + 1);
			poz = 1;
			memmove(keydata, faixNode->keys + ridIx * indexinfo.attrLength, indexinfo.attrLength);
		}

		PF_PageHandle bropageHandle;

		tmp = GetThisPage(indexHandle->fileID, broPn, &bropageHandle);
		if (tmp != SUCCESS) {
			return SUCCESS;
		}
		IX_Node* broixNode = getIXNodefromPH(&bropageHandle);
		
		//如果兄弟节点能容纳下，那么就合并两个节点，否则就需要进行索引项的移动
		if (broixNode->keynum + ixNode->keynum < order) {
			UnpinPage(&fapageHandle);
			//如果是前驱兄弟，就切换两个节点相关信息，使得只需要处理后续合并一种情况
			if (poz == 0) {
				IX_Node* tmp = broixNode;
				broixNode = ixNode;
				ixNode = tmp;
				int tmpp = broPn;
				pn = broPn;
				broPn = tmpp;
				PF_PageHandle tmpph = bropageHandle;
				bropageHandle = pageHandle;
				pageHandle = tmpph;
			}
			
			memmove(ixNode->keys + ixNode->keynum * indexinfo.attrLength, broixNode->keys, broixNode->keynum * indexinfo.attrLength);
			memmove(ixNode->rids + ixNode->keynum, broixNode->rids, broixNode->keynum * PTR_SIZE);
			ixNode->brother = broixNode->brother;
			UnpinPage(&bropageHandle);
			DisposePage(indexHandle->fileID, broPn);
			DeleteEntry(indexHandle, keydata, (RID*)&broPn, ixNode->parent);
			MarkDirty(&pageHandle);
			UnpinPage(&pageHandle);
			
		}
		else {//兄弟节点无法合并，进行索引项移动解决问题

			broixNode->keynum--;

			if (poz == 0) {//兄弟节点为前驱节点

				if (ixNode->is_leaf) {//这种情况移动前驱节点的两项内容即可

					//移出一个空位用来接受新的一个索引项和指针项
					memmove(ixNode->keys + indexinfo.attrLength, ixNode->keys , ixNode->keynum * indexinfo.attrLength);
					memmove(ixNode->rids + 1, ixNode->rids, ixNode->keynum * PTR_SIZE);
					//将前驱兄弟的索引项和指针项移动到当前节点中
					memmove(ixNode->keys, broixNode->keys + broixNode->keynum * indexinfo.attrLength, indexinfo.attrLength);
					memmove(ixNode->rids, broixNode->rids + broixNode->keynum, PTR_SIZE);
					memset(broixNode->keys + broixNode->keynum * indexinfo.attrLength, 0, indexinfo.attrLength);
					memset(broixNode->rids + broixNode->keynum, 0, PTR_SIZE);
					//移动完后，修改父节点对应位置的索引项内容
					memmove(faixNode->keys + (ridIx - 1) * indexinfo.attrLength, ixNode->keys, indexinfo.attrLength);
				}
				else {//这种情况移动前驱节点的指针，将父节点索引项移动到当前节点，前驱节点索引移上去

					//移出一个空位用来接受新的一个索引项和指针项
					memmove(ixNode->keys + indexinfo.attrLength, ixNode->keys, ixNode->keynum* indexinfo.attrLength);
					memmove(ixNode->rids + 1, ixNode->rids, ixNode->keynum* PTR_SIZE);
					//将父节点的索引项和前驱节点的指针项移动到当前节点中
					memmove(ixNode->keys, keydata, indexinfo.attrLength);
					memmove(ixNode->rids, broixNode->rids + broixNode->keynum + 1, PTR_SIZE);
					memset(broixNode->rids + broixNode->keynum + 1, 0, PTR_SIZE);
					//移动完后，修改父节点对应位置的索引项内容并清空前驱节点对应索引项位置的内容
					memmove(faixNode->keys + (ridIx - 1) * indexinfo.attrLength, broixNode->keys + broixNode->keynum * indexinfo.attrLength, indexinfo.attrLength);
					memset(broixNode->keys + broixNode->keynum * indexinfo.attrLength, 0, indexinfo.attrLength);
				}
			}
			else {//兄弟节点为后继节点的情况

				if (ixNode->is_leaf) {//移动后继节点的第一个索引项内容即可

					//先移动后继节点的第一个索引项和指针项到当前节点中
					memmove(ixNode->keys, broixNode->keys, indexinfo.attrLength);
					memmove(ixNode->rids, broixNode->rids, PTR_SIZE);
					//将后继节点的内容整体前移填补空缺
					memmove(broixNode->keys, broixNode->keys + indexinfo.attrLength, broixNode->keynum * indexinfo.attrLength);
					memmove(broixNode->rids, broixNode->rids + 1, (broixNode->keynum + 1) * PTR_SIZE);
					//修改父节点中对应内容
					memmove(faixNode->keys + ridIx * indexinfo.attrLength, broixNode->keys, indexinfo.attrLength);
				}
				else {//移动后继节点的一个指针项和父节点中的索引项到当前节点即可
					
					//先移动指针项和索引项
					memmove(ixNode->rids, broixNode->rids, PTR_SIZE);
					memmove(ixNode->keys, faixNode->keys + ridIx * indexinfo.attrLength, indexinfo.attrLength);
					//将父节点对应内容改为后继节点的第一个索引项
					memmove(faixNode->keys + ridIx * indexinfo.attrLength, broixNode->keys, indexinfo.attrLength);
					//将后继节点的内容整体前移填补空缺
					memmove(broixNode->keys, broixNode->keys + indexinfo.attrLength, broixNode->keynum * indexinfo.attrLength);
					memmove(broixNode->rids, broixNode->rids + 1, (broixNode->keynum + 1) * PTR_SIZE);
				}
			}
		}
		free(keydata);
	}
	return SUCCESS;
}


RC GetIndexTree(char* fileName, Tree* index) {
	return SUCCESS;
};