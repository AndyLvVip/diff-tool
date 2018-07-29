-- ----------------------------------------------------------
-- 实时到账（超级网银联行号））
-- 检测是否存在联行号有变更的记录
-- ----------------------------------------------------------
-- 检测收款方模板数据
SELECT a.id,a.arrivalTimeType,a.type,a.branchBankNo,a.branchBankName,a.bankName,a.createTime FROM fin_payee a WHERE a.branchBankNo IS NOT NULL AND LENGTH(a.branchBankNo)>0 and a.type = 3 and a.dataType = 1 AND a.arrivalTimeType=1 AND a.branchBankNo IN ('313659000016', '313493080539', '402332010004', '313146000019', '313662000015', '402491000026');

-- 检测收款方在途的业务数据
SELECT a.id,a.arrivalTimeType,a.type,a.branchBankNo,a.branchBankName,a.bankName,a.createTime FROM fin_payee a join fin_payapply pa on pa.payeeId = a.id WHERE a.branchBankNo IS NOT NULL AND LENGTH(a.branchBankNo)>0 and a.type = 3 and a.dataType = 2 and pa.extPayStatus not in ('6', 'B', '7', 'C', '9') AND a.arrivalTimeType=1 AND a.branchBankNo IN ('313659000016', '313493080539', '402332010004', '313146000019', '313662000015', '402491000026');
