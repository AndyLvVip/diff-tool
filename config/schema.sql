create database ucacc_dev;
use ucacc_dev;
drop table if exists tmp_branchbank;
CREATE TABLE `tmp_branchbank` (
   `id` bigint(20) NOT NULL AUTO_INCREMENT,
   `bankNo` varchar(13) DEFAULT NULL,
   `bankName` varchar(150) DEFAULT NULL,
   `bankCode` varchar(3) DEFAULT NULL,
   `areaCode` varchar(4) DEFAULT NULL,
   `bankIndex` varchar(4) DEFAULT NULL,
   `checkBit` varchar(1) DEFAULT NULL,
   PRIMARY KEY (`id`),
   KEY `idx_BranchBank_bankNo` (`bankNo`),
   KEY `idx_BranchBank_bankName` (`bankName`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

drop table if exists tmp_supercyberbank;
CREATE TABLE `tmp_supercyberbank` (
   `id` bigint(20) NOT NULL AUTO_INCREMENT,
   `bankNo` varchar(13) DEFAULT NULL,
   `bankName` varchar(150) DEFAULT NULL,
   `bankCode` varchar(3) DEFAULT NULL,
   `areaCode` varchar(4) DEFAULT NULL,
   `bankIndex` varchar(4) DEFAULT NULL,
   `checkBit` varchar(1) DEFAULT NULL,
   `bankNickname` varchar(150) DEFAULT NULL,
   PRIMARY KEY (`id`),
   KEY `idx_SuperCyberBank_bankNo` (`bankNo`),
   KEY `idx_SuperCyberBank_bankName` (`bankName`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

drop table if exists base_branchbank;
CREATE TABLE `base_branchbank` (
   `id` bigint(20) NOT NULL AUTO_INCREMENT,
   `bankNo` varchar(13) DEFAULT NULL,
   `bankName` varchar(150) DEFAULT NULL,
   `bankCode` varchar(3) DEFAULT NULL,
   `areaCode` varchar(4) DEFAULT NULL,
   `bankIndex` varchar(4) DEFAULT NULL,
   `checkBit` varchar(1) DEFAULT NULL,
   PRIMARY KEY (`id`),
   KEY `idx_BranchBank_bankNo` (`bankNo`),
   KEY `idx_BranchBank_bankName` (`bankName`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

drop table if exists base_supercyberbank;
CREATE TABLE `base_supercyberbank` (
   `id` bigint(20) NOT NULL AUTO_INCREMENT,
   `bankNo` varchar(13) DEFAULT NULL,
   `bankName` varchar(150) DEFAULT NULL,
   `bankCode` varchar(3) DEFAULT NULL,
   `areaCode` varchar(4) DEFAULT NULL,
   `bankIndex` varchar(4) DEFAULT NULL,
   `checkBit` varchar(1) DEFAULT NULL,
   `bankNickname` varchar(150) DEFAULT NULL,
   PRIMARY KEY (`id`),
   KEY `idx_SuperCyberBank_bankNo` (`bankNo`),
   KEY `idx_SuperCyberBank_bankName` (`bankName`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;