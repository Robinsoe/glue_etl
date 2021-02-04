# glue_etl setup

def dbs() -> list:
    dbs = [
            'EKXSPOC',
            'LGBXSPOC',
            'S3XSPOC',
            'BKXSPOC',
            'LHXSPOC',
            'BVXSPOC',
            'HUXSPOC',
            'KFXSPOC',
            ]
    return dbs

def schs() -> list:
    schs = [
            'dbo',
            ]
    return schs

def tbls(task=None) -> list:
    tbls = {
        'main':
            [
            'tblNodeMaster',
            'tblParameters',
            'tblEvents',
            'tblWellTests',
            'tbl_CRC_CurrentOperationState',
            'tbl_CRC_FluidShotHistory',
            'tbl_CRC_MASP',
            'tbl_CRC_CurrentWellTests',
            'tblApplications',
            'tblDisableCodes',
            'tblWellDetails',
            'tblMotorKinds',
            'tblMotorSettings',
            'tblMotorSizes',
            'tblOperationStateChangesets',
            'tblOperationStateChangesetValues',
            'tblOperationStateFields',
            'tblOperationStateFieldValues',
            'tblParamStandardTypes',
            'tblPOCTypeApplications',
            'tblPOCTypes',
            'tblPUCustom',
            'tblPumpingUnitManufacturers',
            'tblPumpingUnits',
            'tblSavedParameters',
            'tblEventGroups',
            'tblESPManufacturers',
            'tblESPMotors',
            'tblESPPumps',
            'tblESPWellDetails',
            'tblESPWellMotors',
            'tblESPWellPumps',
             ],
        'archive':
            [
            'tblXDiagOutputs',
            'tblXDiagResultsLast',
            'tblXDiagScores',
            'tblXDiagResults',
            'tblXDiagRodResults',
            'tblXDiagFlags',
            'tblDataHistory',
            'tblDataHistoryArchive',
            'tblCardData',
            ],
    }
    if task:
        return tbls[task]
    else:
        return tbls['main'] + tbls['archive']



