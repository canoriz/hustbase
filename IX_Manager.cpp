#include "IX_Manager.h"

//����attryypeָ�������ͱȽ����������ֵ
int CmpValue(AttrType attrType, char* keyval, char* value);

//����B+��������ֵ������value��Ҷ�ӽڵ㣬��������������ҳ���ҳ������ҳ��ź���������
RC FindeIXNode(IX_IndexHandle* indexHandle, char* value, PF_PageHandle* pageHandle, PageNum* pn, int* ridIx);

RC CreateIndex(const char* fileName, AttrType attrType, int attrLength)
{
	//������Ӧ��ҳ���ļ�
	RC tmp;
	if ((tmp = CreateFile(fileName)) != SUCCESS) {
		return tmp;
	}

	//�򿪴������ļ�
	int fileID;
	if ((tmp = OpenFile((char*)fileName, &fileID)) != SUCCESS) {
		return tmp;
	}


	//��ȡ��һҳ����������������Ϣ
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

	//�޸�ҳ������PF�к������д���
	if ((tmp = MarkDirty(pageHandle)) != SUCCESS) {
		return tmp;
	}

	if ((tmp = UnpinPage(pageHandle)) != SUCCESS) {
		return tmp;
	}

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

	//���ļ�
	RC tmp;
	int fileID;
	if ((tmp = OpenFile((char*)fileName, &fileID)) != SUCCESS) {
		return tmp;
	}

	//��ȡ����������Ϣ
	PF_PageHandle* pageHandle = (PF_PageHandle*)malloc(sizeof(PF_PageHandle));
	if ((tmp = GetThisPage(fileID, 0, pageHandle)) != SUCCESS) {
		return tmp;
	}

	indexHandle->bOpen = true;
	indexHandle->fileID = fileID;
	indexHandle->fileHeader = *(IX_FileHeader*)pageHandle->pFrame->page.pData;

	if ((tmp = UnpinPage(pageHandle)) != SUCCESS) {
		return tmp;
	}

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
	return RC();
}

RC DeleteEntry(IX_IndexHandle* indexHandle, void* pData, const RID* rid)
{
	return RC();
}

RC OpenIndexScan(IX_IndexScan* indexScan, IX_IndexHandle* indexHandle, CompOp compOp, char* value)
{
	if (indexScan->bOpen == true) {
		return IX_SCANOPENNED;
	}

	RC tmp;
	indexScan->bOpen = true;
	indexScan->pIXIndexHandle = indexHandle;
	indexScan->compOp = compOp;
	indexScan->value = value;

	indexScan->pn = indexHandle->fileHeader.rootPage;

	if ((tmp = GetThisPage(indexHandle->fileID, indexHandle->fileHeader.rootPage, &indexScan->PageHandle)) != SUCCESS) {
		return tmp;
	}

	return SUCCESS;
}

RC IX_GetNextEntry(IX_IndexScan* indexScan, RID* rid)
{
	if (indexScan->bOpen == false) {
		return IX_ISCLOSED;
	}

	//��PH�ڴ��л�ȡIX_Node��Ϣ
	IX_Node* ixNode = (IX_Node*)(indexScan->PageHandle.pFrame->page.pData + sizeof(IX_FileHeader));
	RC tmp;

	//�����ȡ�����һ��Ҷ�ӽڵ�����һ��������ͷ���EOF
	if (indexScan->ridIx == ixNode->keynum && indexScan->pn == 0) {
		return IX_EOF;
	}

	//��������ýڵ����һ����������л�����һҳ��
	if (indexScan->ridIx == ixNode->keynum) {
		if ((tmp = UnpinPage(&indexScan->PageHandle)) != SUCCESS) {
			return tmp;
		}

		if ((tmp = GetThisPage(indexScan->pIXIndexHandle->fileID, indexScan->pn, &indexScan->PageHandle)) != SUCCESS) {
			return tmp;
		}
		indexScan->pn = ixNode->brother;
		indexScan->ridIx = 0;
	}

	//��ȡ����ֵ�������Ϣ
	AttrType attrType = indexScan->pIXIndexHandle->fileHeader.attrType;
	int attrLength = indexScan->pIXIndexHandle->fileHeader.attrLength;
	int keyLength = indexScan->pIXIndexHandle->fileHeader.keyLength;
	char*  keyval = (char*)malloc(attrLength);
	memcpy(keyval, ixNode->keys + indexScan->ridIx * keyLength, attrLength);
	int result = CmpValue(attrType, keyval, indexScan->value);
	free(keyval);
	//���ݲ������Խ�����д���
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

	if ((tmp = UnpinPage(&indexScan->PageHandle)) != SUCCESS) {
		return tmp;
	}

	return SUCCESS;
}

RC GetIndexTree(char* fileName, Tree* index) {
	return RC();
};

int CmpValue(AttrType attrType, char* keyval, char* value) {
	int result;

	//�����������ͻ�ȡ�ȽϽ��
	switch (attrType)
	{
	case ints:
		result = *(int*)keyval < *(int*)value;
		break;
	case chars:
		result = strcmp(keyval,value);
		break;
	case floats:
		result = *(float*)keyval < *(float*)value;
		break;
	default:
		result = strcmp(keyval, value);
		break;
	}

	return result;
}


RC FindeIXNode(IX_IndexHandle* indexHandle, char* value, PF_PageHandle* pageHandle, PageNum* pn, int* ridIx) {
	//�Ӹ��ڵ㿪ʼ������Ҫ��ֵ
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
		//ÿ�ζ�ȡҳ��Ϊ_pn��ҳ����д���
		if ((tmp = GetThisPage(indexHandle->fileID, _pn, &_pageHandle)) != SUCCESS) {
			return tmp;
		}
		//��ȡ�ڵ���Ϣ�������Ҷ�ӽڵ��ֱ�ӷ���
		ixNode = (IX_Node*)(_pageHandle.pFrame->page.pData + sizeof(IX_FileHeader));
		if (ixNode->is_leaf == 1) {
			break;
		}

		int result = -1;
		for (_ridIx = 0; _ridIx < ixNode->keynum; _ridIx++) {
			memcpy(keyval, ixNode->keys + _ridIx * keyLength, attrLength);
			result = CmpValue(attrType, keyval, value);
			if (result < 0) {
				continue;
			}
			else {
				break;
			}
		}
		if (_ridIx == 0) {
			return IX_INVALIDKEY;
		}
		_ridIx--;
		_pn = *(int*)(ixNode->rids + _ridIx);
	} 
	*pageHandle = _pageHandle;
	free(keyval);
	return SUCCESS;
}
