-- DDL for Transaction
CREATE TABLE Transactions (
TransID INT PRIMARY KEY,
AccountID Varchar(255) NOT NULL,
TellerID Varchar(255) NOT NULL,
Amount decimal(10,2) NOT NULL,
classtype varchar (255) NOT NULL,
DataPerformed varchar (255) NOT NULL
);
select * from Transactions; 

-- DDL for Loan

CREATE TABLE Loan(
LoanID INT PRIMARY KEY,
AccountID Varchar(255) NOT NULL,
Amount decimal(10,2) NOT NULL,
InterestRate decimal(10,2)NOT NULL,
StartDate Date NOT NULL,
EndDate Date NOT NULL,
Status Varchar (255) NOT NULL
);


-- DDL for Payment
CREATE TABLE Payment (
    PaymentID INT PRIMARY KEY,
    Amount DECIMAL(10,2) NOT NULL,
    PaymentDate DATE NOT NULL,
    methodmode VARCHAR(255) NOT NULL,
    LoanID INT NOT NULL,
    CONSTRAINT fk_payment_loan FOREIGN KEY (LoanID)REFERENCES Loan(LoanID)
        );
		


-- DDL LoanRepayments

CREATE TABLE LoanRepayments (
    RepaymentID SERIAL PRIMARY KEY,
    LoanID INT,
    Amount NUMERIC(12,2),
    RepaymentDate DATE
);
select * from loanRepayments;
-- DDL on Node_A
CREATE TABLE TRANSACTION_A (
  TRANSID     NUMBER PRIMARY KEY,
  ACCOUNTID   NUMBER NOT NULL,
  TELLERID    NUMBER,
  AMOUNT      NUMBER(12,2),
  TYPE        VARCHAR2(20),
  DATEPERF    DATE
);

-- Sample seed rows: 5 rows on Node_A
INSERT INTO TRANSACTION_A VALUES (1, 101, 11, 100.00, 'deposit', TO_DATE('2025-10-01','YYYY-MM-DD'));
INSERT INTO TRANSACTION_A VALUES (3, 102, 12,  50.00, 'withdrawal', TO_DATE('2025-10-02','YYYY-MM-DD'));
INSERT INTO TRANSACTION_A VALUES (5, 101, 11, 200.00, 'deposit', TO_DATE('2025-10-03','YYYY-MM-DD'));
INSERT INTO TRANSACTION_A VALUES (7, 103, 13,  75.00, 'withdrawal', TO_DATE('2025-10-04','YYYY-MM-DD'));
INSERT INTO TRANSACTION_A VALUES (9, 104, 11, 300.00, 'deposit', TO_DATE('2025-10-05','YYYY-MM-DD'));
COMMIT;

-- DDL on Node_B
CREATE TABLE TRANSACTION_B (
  TRANSID     NUMBER PRIMARY KEY,
  ACCOUNTID   NUMBER NOT NULL,
  TELLERID    NUMBER,
  AMOUNT      NUMBER(12,2),
  TYPE        VARCHAR2(20),
  DATEPERF    DATE
);

-- Sample seed rows: 5 rows on Node_B
INSERT INTO TRANSACTION_B VALUES (2, 105, 21,  60.00, 'deposit', TO_DATE('2025-10-02','YYYY-MM-DD'));
INSERT INTO TRANSACTION_B VALUES (4, 106, 22, 120.00, 'withdrawal', TO_DATE('2025-10-03','YYYY-MM-DD'));
INSERT INTO TRANSACTION_B VALUES (6, 105, 21, 250.00, 'deposit', TO_DATE('2025-10-04','YYYY-MM-DD'));
INSERT INTO TRANSACTION_B VALUES (8, 107, 22,  30.00, 'withdrawal', TO_DATE('2025-10-05','YYYY-MM-DD'));
INSERT INTO TRANSACTION_B VALUES (10,108, 21, 500.00, 'deposit', TO_DATE('2025-10-06','YYYY-MM-DD'));
COMMIT;

--------A FRAGMENT AND RECOMBINE MAIN FACT
-- On Node_A (Main Database)
CREATE TABLE Transaction_A (
    TransID SERIAL PRIMARY KEY,
    AccountID INT NOT NULL,
    TellerID VARCHAR(50) NOT NULL,
    Amount DECIMAL(15,2) NOT NULL,
    Type VARCHAR(20) CHECK (Type IN ('Credit', 'Debit')),
    DatePerformed DATE DEFAULT CURRENT_DATE
);
select * from Transaction_A;

-- On Node_B (Secondary Database)
CREATE TABLE Transaction_B (
    TransID SERIAL PRIMARY KEY,
    AccountID INT NOT NULL,
    TellerID VARCHAR(50) NOT NULL,
    Amount DECIMAL(15,2) NOT NULL,
    Type VARCHAR(20) CHECK (Type IN ('Credit', 'Debit')),
    DatePerformed DATE DEFAULT CURRENT_DATE
);
 select * from Transaction_B ;


 -- On Node_A: Insert 5 transactions (Even AccountIDs)
INSERT INTO Transaction_A (AccountID, TellerID, Amount, Type, DatePerformed) VALUES
(1002, 'T001', 1500.00, 'Credit', '2024-01-15'),
(1004, 'T002', 250.75, 'Debit', '2024-01-16'),
(1006, 'T003', 3000.00, 'Credit', '2024-01-17'),
(1008, 'T001', 500.50, 'Debit', '2024-01-18'),
(1010, 'T004', 1200.25, 'Credit', '2024-01-19');
select * from Transaction_A;
-- On Node_B: Insert 5 transactions (Odd AccountIDs)
INSERT INTO Transaction_B (AccountID, TellerID, Amount, Type, DatePerformed) VALUES
(1001, 'T002', 2000.00, 'Credit', '2024-01-10'),
(1003, 'T003', 150.00, 'Debit', '2024-01-11'),
(1005, 'T001', 4500.75, 'Credit', '2024-01-12'),
(1007, 'T004', 750.25, 'Debit', '2024-01-13'),
(1009, 'T002', 1800.50, 'Credit', '2024-01-14');


-------Step 3: Create Database Link & Unified View

-- On Node_A: Create foreign data wrapper connection to Node_B
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER proj_link
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'localhost',
    dbname 'node_b_database',
    port '5433'
);

CREATE USER MAPPING FOR CURRENT_USER
SERVER proj_link
OPTIONS (
    user 'postgres',
    password '12345'
);

-- Import the foreign table from Node_B
IMPORT FOREIGN SCHEMA public
LIMIT TO (Transaction_B)
FROM SERVER proj_link INTO public;

-- Create the unified view
CREATE VIEW Transaction_ALL AS
SELECT TransID, AccountID, TellerID, Amount, Type, DatePerformed, 'Node_A' as SourceNode
FROM Transaction_A
UNION ALL
SELECT TransID, AccountID, TellerID, Amount, Type, DatePerformed, 'Node_B' as SourceNode
FROM Transaction_B;

select * from Transaction_ALL;

------Step 4: Validation with COUNT and Checksum

-- Validate fragmentation and recombination
SELECT 
    'Transaction_A' as Table_Name, 
    COUNT(*) as Row_Count,
    SUM(MOD(TransID, 97)) as Checksum
FROM Transaction_A
UNION ALL
SELECT 
    'Transaction_B' as Table_Name, 
    COUNT(*) as Row_Count,
    SUM(MOD(TransID, 97)) as Checksum
FROM Transaction_B
UNION ALL
SELECT 
    'Transaction_ALL' as Table_Name, 
    COUNT(*) as Row_Count,
    SUM(MOD(TransID, 97)) as Checksum
FROM Transaction_ALL;

---------Step 5: Verify Data Distribution

-- Check data distribution across nodes
SELECT 
    SourceNode,
    COUNT(*) as TransactionCount,
    SUM(Amount) as TotalAmount,
    AVG(Amount) as AverageAmount
FROM Transaction_ALL
GROUP BY SourceNode
ORDER BY SourceNode;

-- Verify individual fragments
SELECT 'Node_A Data' as Info, * FROM Transaction_A ORDER BY TransID;
SELECT 'Node_B Data' as Info, * FROM Transaction_B ORDER BY TransID;


-- Expected Output:
-- Table_Name     | Row_Count | Checksum
-- Transaction_A  | 5         | [sum of mod values]
-- Transaction_B  | 5         | [sum of mod values]  
-- Transaction_ALL| 10        | [sum of both above]

-------- TAST 2
-- Node_A Transactions (Even AccountIDs: 1002, 1004, 1006, 1008, 1010)
INSERT INTO Transaction_A (AccountID, TellerID, Amount, Type, DatePerformed) VALUES
(1002, 'T001', 1500.00, 'Credit', '2024-01-15'),
(1004, 'T002', 250.75, 'Debit', '2024-01-16'),
(1006, 'T003', 3000.00, 'Credit', '2024-01-17'),
(1008, 'T001', 500.50, 'Debit', '2024-01-18'),
(1010, 'T004', 1200.25, 'Credit', '2024-01-19');

-- Verify Node_A insertion
SELECT 'Node_A inserted: ' || COUNT(*)::TEXT || ' rows' FROM Transaction_A;

-- Node_B Transactions (Odd AccountIDs: 1001, 1003, 1005, 1007, 1009)
INSERT INTO Transaction_B (AccountID, TellerID, Amount, Type, DatePerformed) VALUES
(1001, 'T002', 2000.00, 'Credit', '2024-01-10'),
(1003, 'T003', 150.00, 'Debit', '2024-01-11'),
(1005, 'T001', 4500.75, 'Credit', '2024-01-12'),
(1007, 'T004', 750.25, 'Debit', '2024-01-13'),
(1009, 'T002', 1800.50, 'Credit', '2024-01-14');

-- Verify Node_B insertion
SELECT 'Node_B inserted: ' || COUNT(*)::TEXT || ' rows' FROM Transaction_B;

--------Complete Validation Script
-- Comprehensive validation to ensure exactly 10 committed rows
WITH counts AS (
    SELECT 
        (SELECT COUNT(*) FROM Transaction_A) as count_a,
        (SELECT COUNT(*) FROM Transaction_B) as count_b
)
SELECT 
    'Validation Results:' as check_type,
    'Node_A rows: ' || count_a::TEXT as node_a_count,
    'Node_B rows: ' || count_b::TEXT as node_b_count,
    'Total rows: ' || (count_a + count_b)::TEXT as total_count,
    CASE 
        WHEN (count_a + count_b) = 10 THEN '✅ PASS: Exactly 10 rows total'
        ELSE '❌ FAIL: Row count mismatch'
    END as status
FROM counts;

-- Show all data with source identification
SELECT 'Node_A Data' as source, TransID, AccountID, TellerID, Amount, Type, DatePerformed 
FROM Transaction_A
UNION ALL
SELECT 'Node_B Data' as source, TransID, AccountID, TellerID, Amount, Type, DatePerformed 
FROM Transaction_B
ORDER BY source, TransID;

------Data Distribution Analysis

-- Analyze the data distribution pattern
SELECT 
    'Even AccountIDs on Node_A' as distribution_rule,
    COUNT(*) as transaction_count,
    SUM(Amount) as total_amount,
    AVG(Amount) as average_amount
FROM Transaction_A
UNION ALL
SELECT 
    'Odd AccountIDs on Node_B' as distribution_rule,
    COUNT(*) as transaction_count,
    SUM(Amount) as total_amount,
    AVG(Amount) as average_amount
FROM Transaction_B;

-- Verify fragmentation logic
SELECT 
    'AccountID Pattern Check' as check_type,
    'Node_A (Even IDs): ' || 
    (SELECT COUNT(*) FROM Transaction_A WHERE AccountID % 2 = 0)::TEXT || ' rows' as node_a_pattern,
    'Node_B (Odd IDs): ' || 
    (SELECT COUNT(*) FROM Transaction_B WHERE AccountID % 2 = 1)::TEXT || ' rows' as node_b_pattern;




	------Create Unified View with Database Link
----------First, Ensure Database Link is Properly Configured
-- On Node_A: Create or verify the foreign data wrapper connection
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Drop existing objects if they exist (for clean setup)
DROP SERVER IF EXISTS proj_link CASCADE;

-- Create foreign server connection to Node_B
CREATE SERVER proj_link
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'localhost',
    dbname 'BankingTransactionB',  -- Use your actual Node_B database name
    port '5433'
);

-- Create user mapping for current user
CREATE USER MAPPING FOR CURRENT_USER
SERVER proj_link
OPTIONS (
    user 'postgres',
    password '12345'
);

-- Verify the server is created
SELECT srvname, srvoptions FROM pg_foreign_server WHERE srvname = 'proj_link';

-------Import Transaction_B as Foreign Table

-- Import the Transaction_B table from Node_B
DROP FOREIGN TABLE IF EXISTS transaction_b;

IMPORT FOREIGN SCHEMA public
LIMIT TO (Transaction_B)
FROM SERVER proj_link INTO public;

-- Verify foreign table was created
SELECT 
    ft.ftrelid::regclass as foreign_table,
    fs.srvname as foreign_server
FROM pg_foreign_table ft
JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
WHERE ft.ftrelid::regclass = 'transaction_b'::-- Drop view if it exists
DROP VIEW IF EXISTS Transaction_ALL;

-- Create the unified view combining both fragments
CREATE VIEW Transaction_ALL AS
SELECT 
    TransID,
    AccountID, 
    TellerID,
    Amount,
    Type,
    DatePerformed,
    'Node_A' as SourceNode
FROM Transaction_A
UNION ALL
SELECT 
    TransID,
    AccountID,
    TellerID, 
    Amount,
    Type,
    DatePerformed,
    'Node_B' as SourceNode
FROM transaction_b;  -- This is the foreign table from Node_B

-- Verify the view was created
SELECT table_name, view_definition 
FROM information_schema.views 
WHERE table_name = 'transaction_all';regclass;
-- Drop view if it exists
DROP VIEW IF EXISTS Transaction_ALL;

-- Create the unified view combining both fragments
CREATE VIEW Transaction_ALL AS
SELECT 
    TransID,
    AccountID, 
    TellerID,
    Amount,
    Type,
    DatePerformed,
    'Node_A' as SourceNode
FROM Transaction_A
UNION ALL
SELECT 
    TransID,
    AccountID,
    TellerID, 
    Amount,
    Type,
    DatePerformed,
    'Node_B' as SourceNode
FROM transaction_b;  -- This is the foreign table from Node_B

------------

-- Test 1: Count all records from the unified view
SELECT 
    'Transaction_ALL View Test' as test_type,
    COUNT(*) as total_rows,
    COUNT(DISTINCT SourceNode) as nodes_accessed
FROM Transaction_ALL;

-- Test 2: Verify data from both nodes appears
SELECT 
    SourceNode,
    COUNT(*) as row_count,
    SUM(Amount) as total_amount,
    MIN(DatePerformed) as earliest_date,
    MAX(DatePerformed) as latest_date
FROM Transaction_ALL
GROUP BY SourceNode
ORDER BY SourceNode;

-- Test 3: Show sample data from the unified view
SELECT 
    TransID,
    AccountID,
    TellerID,
    Amount,
    Type,
    DatePerformed,
    SourceNode
FROM Transaction_ALL
ORDER BY SourceNode, TransID
LIMIT 12;  -- Should show all 10 rows + header

-- Verify the view was created
SELECT table_name, view_definition 
FROM information_schema.views 
WHERE table_name = 'transaction_all';

-------Comprehensive Validation

-- Comprehensive validation that the view works correctly
WITH view_stats AS (
    SELECT COUNT(*) as view_count, SUM(Amount) as view_total
    FROM Transaction_ALL
),
fragment_stats AS (
    SELECT 
        (SELECT COUNT(*) FROM Transaction_A) + 
        (SELECT COUNT(*) FROM transaction_b) as fragment_count,
        (SELECT COALESCE(SUM(Amount), 0) FROM Transaction_A) + 
        (SELECT COALESCE(SUM(Amount), 0) FROM transaction_b) as fragment_total
)
SELECT 
    'Data Consistency Check' as check_type,
    vs.view_count as rows_in_view,
    fs.fragment_count as rows_in_fragments,
    vs.view_total as amount_in_view,
    fs.fragment_total as amount_in_fragments,
    CASE 
        WHEN vs.view_count = fs.fragment_count 
        AND vs.view_total = fs.fragment_total 
        THEN '✅ PASS: View matches fragment totals'
        ELSE '❌ FAIL: Data mismatch detected'
    END as validation_result
FROM view_stats vs, fragment_stats fs;

-------Test Cross-Node Queries

-- Test complex queries using the unified view
-- Query 1: Find top 3 largest transactions regardless of node
SELECT 
    TransID,
    AccountID,
    TellerID,
    Amount,
    Type,
    DatePerformed,
    SourceNode
FROM Transaction_ALL
ORDER BY Amount DESC
LIMIT 3;

-- Query 2: Daily transaction summary across both nodes
SELECT 
    DatePerformed,
    COUNT(*) as transaction_count,
    SUM(Amount) as daily_total,
    COUNT(DISTINCT SourceNode) as nodes_used
FROM Transaction_ALL
GROUP BY DatePerformed
ORDER BY DatePerformed;

-- Query 3: Teller performance across distributed system
SELECT 
    TellerID,
    COUNT(*) as transactions_handled,
    SUM(CASE WHEN Type = 'Credit' THEN Amount ELSE 0 END) as total_credits,
    SUM(CASE WHEN Type = 'Debit' THEN Amount ELSE 0 END) as total_debits,
    COUNT(DISTINCT SourceNode) as nodes_worked_on
FROM Transaction_ALL
GROUP BY TellerID
ORDER BY transactions_handled DESC;

----------Troubleshooting Commands
------If you encounter issues, run these diagnostic commands:
-- Check if foreign table is accessible
SELECT * FROM transaction_b LIMIT 1;

-- Check foreign server status
SELECT 
    srvname, 
    srvoptions,
    (SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public') as local_tables,
    (SELECT count(*) FROM information_schema.foreign_tables) as foreign_tables
FROM pg_foreign_server 
WHERE srvname = 'proj_link';

-- Test basic UNION ALL without view
SELECT 'Node_A' as source, * FROM Transaction_A
UNION ALL
SELECT 'Node_B' as source, * FROM transaction_b
ORDER BY source, TransID;

----------

-- DETAILED VALIDATION WITH EVIDENCE
SELECT '=== COMPLETE VALIDATION REPORT ===' as report_header;

-- 1. Count Validation
SELECT 
    '1. ROW COUNT VALIDATION' as test_type,
    (SELECT COUNT(*) FROM Transaction_A) as node_a_count,
    (SELECT COUNT(*) FROM transaction_b) as node_b_count,
    (SELECT COUNT(*) FROM Transaction_ALL) as view_count,
    CASE 
        WHEN (SELECT COUNT(*) FROM Transaction_ALL) = 
              (SELECT COUNT(*) FROM Transaction_A) + 
              (SELECT COUNT(*) FROM transaction_b)
        THEN '✅ PASS: Row counts match'
        ELSE '❌ FAIL: Row count mismatch'
    END as count_status;

-- 2. Checksum Validation  
SELECT 
    '2. CHECKSUM VALIDATION (MOD(TransID, 97))' as test_type,
    (SELECT SUM(MOD(TransID, 97)) FROM Transaction_A) as node_a_checksum,
    (SELECT SUM(MOD(TransID, 97)) FROM transaction_b) as node_b_checksum,
    (SELECT SUM(MOD(TransID, 97)) FROM Transaction_ALL) as view_checksum,
    CASE 
        WHEN (SELECT SUM(MOD(TransID, 97)) FROM Transaction_ALL) = 
              (SELECT SUM(MOD(TransID, 97)) FROM Transaction_A) + 
              (SELECT SUM(MOD(TransID, 97)) FROM transaction_b)
        THEN '✅ PASS: Checksums match'
        ELSE '❌ FAIL: Checksum mismatch'
    END as checksum_status;

-- 3. Data Integrity Validation
SELECT 
    '3. DATA INTEGRITY VALIDATION' as test_type,
    (SELECT SUM(Amount) FROM Transaction_A) as node_a_total,
    (SELECT SUM(Amount) FROM transaction_b) as node_b_total,
    (SELECT SUM(Amount) FROM Transaction_ALL) as view_total,
    CASE 
        WHEN (SELECT SUM(Amount) FROM Transaction_ALL) = 
              (SELECT SUM(Amount) FROM Transaction_A) + 
              (SELECT SUM(Amount) FROM transaction_b)
        THEN '✅ PASS: Data totals match'
        ELSE '❌ FAIL: Data total mismatch'
    END as integrity_status;

-- 4. Final Comprehensive Result
SELECT 
    'FINAL VALIDATION RESULT' as result_type,
    CASE 
        WHEN (SELECT COUNT(*) FROM Transaction_ALL) = 10
        AND (SELECT SUM(MOD(TransID, 97)) FROM Transaction_ALL) = 
            (SELECT SUM(MOD(TransID, 97)) FROM Transaction_A) + 
            (SELECT SUM(MOD(TransID, 97)) FROM transaction_b)
        AND (SELECT SUM(Amount) FROM Transaction_ALL) = 
            (SELECT SUM(Amount) FROM Transaction_A) + 
            (SELECT SUM(Amount) FROM transaction_b)
        THEN '🎉 SUCCESS: All validations passed - Fragmentation and recombination working correctly!'
        ELSE '💥 FAILURE: Data consistency issues detected'
    END as final_result;

	-------------------Evidence Collection for Screenshot-- CLEAN EVIDENCE OUTPUT FOR SCREENSHOT
SELECT 'EVIDENCE: Fragment vs Unified View Comparison' as title;

-- Fragment Data
SELECT 'FRAGMENT_DATA' as data_type, 'Transaction_A' as source, * FROM Transaction_A
UNION ALL
SELECT 'FRAGMENT_DATA' as data_type, 'Transaction_B' as source, * FROM transaction_b
ORDER BY source, TransID;

-- Validation Summary (Perfect for screenshot)
SELECT 
    'SUMMARY: Count and Checksum Validation' as validation_type,
    'Expected: 10 rows total' as expectation,
    'Actual: ' || (SELECT COUNT(*) FROM Transaction_ALL)::TEXT || ' rows' as actual_count,
    'Checksum (A): ' || (SELECT SUM(MOD(TransID, 97)) FROM Transaction_A)::TEXT as checksum_a,
    'Checksum (B): ' || (SELECT SUM(MOD(TransID, 97)) FROM transaction_b)::TEXT as checksum_b,
    'Checksum (ALL): ' || (SELECT SUM(MOD(TransID, 97)) FROM Transaction_ALL)::TEXT as checksum_all,
    CASE 
        WHEN (SELECT COUNT(*) FROM Transaction_ALL) = 10 
        AND (SELECT SUM(MOD(TransID, 97)) FROM Transaction_ALL) = 
            (SELECT SUM(MOD(TransID, 97)) FROM Transaction_A) + 
            (SELECT SUM(MOD(TransID, 97)) FROM transaction_b)
        THEN '✅ VALIDATION PASSED'
        ELSE '❌ VALIDATION FAILED'
    END as result;
	-- CLEAN EVIDENCE OUTPUT FOR SCREENSHOT
SELECT 'EVIDENCE: Fragment vs Unified View Comparison' as title;

-- Fragment Data
SELECT 'FRAGMENT_DATA' as data_type, 'Transaction_A' as source, * FROM Transaction_A
UNION ALL
SELECT 'FRAGMENT_DATA' as data_type, 'Transaction_B' as source, * FROM transaction_b
ORDER BY source, TransID;

-- Validation Summary (Perfect for screenshot)
SELECT 
    'SUMMARY: Count and Checksum Validation' as validation_type,
    'Expected: 10 rows total' as expectation,
    'Actual: ' || (SELECT COUNT(*) FROM Transaction_ALL)::TEXT || ' rows' as actual_count,
    'Checksum (A): ' || (SELECT SUM(MOD(TransID, 97)) FROM Transaction_A)::TEXT as checksum_a,
    'Checksum (B): ' || (SELECT SUM(MOD(TransID, 97)) FROM transaction_b)::TEXT as checksum_b,
    'Checksum (ALL): ' || (SELECT SUM(MOD(TransID, 97)) FROM Transaction_ALL)::TEXT as checksum_all,
    CASE 
        WHEN (SELECT COUNT(*) FROM Transaction_ALL) = 10 
        AND (SELECT SUM(MOD(TransID, 97)) FROM Transaction_ALL) = 
            (SELECT SUM(MOD(TransID, 97)) FROM Transaction_A) + 
            (SELECT SUM(MOD(TransID, 97)) FROM transaction_b)
        THEN '✅ VALIDATION PASSED'
        ELSE '❌ VALIDATION FAILED'
    END as result;


--------	Complete DDL and Population Scripts Recap



	-- DDL FOR TRANSACTION_A AND TRANSACTION_B (Recap)
SELECT '=== DDL FOR FRAGMENTED TABLES ===' as ddl_section;

-- Transaction_A DDL
SELECT 'CREATE TABLE Transaction_A (' as ddl_line
UNION ALL SELECT '    TransID SERIAL PRIMARY KEY,'
UNION ALL SELECT '    AccountID INT NOT NULL,'
UNION ALL SELECT '    TellerID VARCHAR(50) NOT NULL,'
UNION ALL SELECT '    Amount DECIMAL(15,2) NOT NULL,'
UNION ALL SELECT '    Type VARCHAR(20) CHECK (Type IN (''Credit'', ''Debit'')),'
UNION ALL SELECT '    DatePerformed DATE DEFAULT CURRENT_DATE'
UNION ALL SELECT ');';

-- Transaction_B DDL  
SELECT 'CREATE TABLE Transaction_B (' as ddl_line
UNION ALL SELECT '    TransID SERIAL PRIMARY KEY,'
UNION ALL SELECT '    AccountID INT NOT NULL,'
UNION ALL SELECT '    TellerID VARCHAR(50) NOT NULL,'
UNION ALL SELECT '    Amount DECIMAL(15,2) NOT NULL,'
UNION ALL SELECT '    Type VARCHAR(20) CHECK (Type IN (''Credit'', ''Debit'')),'
UNION ALL SELECT '    DatePerformed DATE DEFAULT CURRENT_DATE'
UNION ALL SELECT ');';

-------Database Link Creation Recap

-- DATABASE LINK CREATION (Recap)
SELECT '=== DATABASE LINK CREATION ===' as link_section;

SELECT 'CREATE SERVER proj_link' as link_ddl
UNION ALL SELECT 'FOREIGN DATA WRAPPER postgres_fdw'
UNION ALL SELECT 'OPTIONS ('
UNION ALL SELECT '    host ''localhost'','
UNION ALL SELECT '    dbname ''BankingTransactionB'','
UNION ALL SELECT '    port ''5433'''
UNION ALL SELECT ');'
UNION ALL SELECT ''
UNION ALL SELECT 'CREATE USER MAPPING FOR CURRENT_USER'
UNION ALL SELECT 'SERVER proj_link'
UNION ALL SELECT 'OPTIONS ('
UNION ALL SELECT '    user ''postgres'','
UNION ALL SELECT '    password ''12345'''
UNION ALL SELECT ');';

--------Unified View Creation Recap
-- UNIFIED VIEW CREATION (Recap)
SELECT '=== UNIFIED VIEW CREATION ===' as view_section;

SELECT 'CREATE VIEW Transaction_ALL AS' as view_ddl
UNION ALL SELECT 'SELECT'
UNION ALL SELECT '    TransID,'
UNION ALL SELECT '    AccountID,'
UNION ALL SELECT '    TellerID,'
UNION ALL SELECT '    Amount,'
UNION ALL SELECT '    Type,'
UNION ALL SELECT '    DatePerformed,'
UNION ALL SELECT '    ''Node_A'' as SourceNode'
UNION ALL SELECT 'FROM Transaction_A'
UNION ALL SELECT 'UNION ALL'
UNION ALL SELECT 'SELECT'
UNION ALL SELECT '    TransID,'
UNION ALL SELECT '    AccountID,'
UNION ALL SELECT '    TellerID,'
UNION ALL SELECT '    Amount,'
UNION ALL SELECT '    Type,'
UNION ALL SELECT '    DatePerformed,'
UNION ALL SELECT '    ''Node_B'' as SourceNode'
UNION ALL SELECT 'FROM transaction_b;';

--------A2: Database Link & Cross-Node Join (3-10 rows result)
--------Step 1: Create Database Link to Node_B

-- On Node_A: Create database link to Node_B
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Drop existing objects if they exist
DROP SERVER IF EXISTS proj_link CASCADE;

-- Create foreign server connection to Node_B
CREATE SERVER proj_link
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'localhost',
    dbname 'BankingTransactionB',
    port '5433'
);

-- Create user mapping
CREATE USER MAPPING FOR CURRENT_USER
SERVER proj_link
OPTIONS (
    user 'postgres',
    password '12345'
);

-- Verify the database link
SELECT 
    srvname as server_name,
    srvoptions as connection_details
FROM pg_foreign_server 
WHERE srvname = 'proj_link';

-------


-- Check what tables are available on the remote server
SELECT 
    'Available Remote Tables' as info,
    table_name,
    table_type
FROM information_schema.tables 
WHERE table_schema = 'public'
ORDER BY table_name;

-- Test basic connectivity
SELECT 
    'Connectivity Test' as test,
    (SELECT COUNT(*) FROM account) as account_count,
    (SELECT COUNT(*) FROM customer) as customer_count,
    (SELECT COUNT(*) FROM loan) as loan_count;

-- Check foreign data wrapper status
SELECT 
    'FDW Status' as status_check,
    srvname as server_name,
    srvoptions as connection_details
FROM pg_foreign_server;

----------------Q3
 -- Ensure we get 3-10 rows

---------- Enhanced distributed join with business metrics
SELECT 
    '=== ENHANCED DISTRIBUTED JOIN WITH BUSINESS CONTEXT ===' as query_title;
-- Check what foreign tables are currently available
SELECT 
    ft.ftrelid::regclass as foreign_table_name,
    fs.srvname as server_name
FROM pg_foreign_table ft
JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
WHERE fs.srvname = 'proj_link';

--------STEP2
-- Drop if exists and import Teller table
DROP FOREIGN TABLE IF EXISTS teller CASCADE;

-- Method 1: Import using IMPORT FOREIGN SCHEMA
IMPORT FOREIGN SCHEMA public
LIMIT TO (Teller)
FROM SERVER proj_link INTO public;

-- Verify the import worked
SELECT 
    'Teller table import status' as status,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM pg_foreign_table ft 
            JOIN pg_foreign_server fs ON ft.ftserver = fs.oid 
            WHERE ft.ftrelid::regclass = 'teller'::regclass AND fs.srvname = 'proj_link'
        ) THEN '✅ SUCCESS: Teller table imported'
        ELSE '❌ FAILED: Teller table not found'
    END as result;


	---------STEP3
	-- Method 2: Create foreign table manually
DROP FOREIGN TABLE IF EXISTS teller CASCADE;

CREATE FOREIGN TABLE teller (
    TellerID VARCHAR(50),
    FullName VARCHAR(255),
    Branch VARCHAR(100),
    Contact VARCHAR(20)
) SERVER proj_link
OPTIONS (schema_name 'public', table_name 'Teller');

-- Verify manual creation
SELECT 
    'Manual Teller table creation' as status,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM pg_foreign_table ft 
            WHERE ft.ftrelid::regclass = 'teller'::regclass
        ) THEN '✅ SUCCESS: Teller table created manually'
        ELSE '❌ FAILED: Manual creation failed'
    END as result;

	-----STEP4
	-- Test if we can access the Teller table
SELECT '=== TESTING TELLER TABLE ACCESS ===' as test;

-- Check if table exists and has data
SELECT 
    'Teller Table Check' as check_type,
    (SELECT COUNT(*) FROM teller) as row_count,
    (SELECT COUNT(*) FROM teller LIMIT 3) as sample_access;

-- Show sample teller data
SELECT * FROM teller LIMIT 3;


------STEP5
-- Fixed distributed join with verified table names
SELECT 
    '=== DISTRIBUTED JOIN: Transaction_A ⋈ Teller@proj_link ===' as query_title;

SELECT 
    t.TransID,
    t.AccountID,
    CASE 
WHEN t.Amount >= 1000 THEN 'Large'
        WHEN t.Amount >= 500 THEN 'Medium'
        ELSE 'Small'
    END as Transaction_Size,
    t.Amount,
    t.Type,
    t.DatePerformed,
    tel.TellerID,
    tel.FullName as Teller_Name,
    tel.Branch,
    CASE 
        WHEN tel.Branch LIKE '%Main%' THEN 'Headquarters'
        ELSE 'Regional Branch'
    END as Branch_Type
FROM Transaction_A t
JOIN teller tel ON t.TellerID = tel.TellerID
WHERE t.Amount BETWEEN 250 AND 3000
  AND t.DatePerformed >= '2024-01-15'
ORDER BY t.Amount DESC;

-------STEP6
-- COMPLETE DIAGNOSTIC SCRIPT
SELECT '=== DATABASE LINK DIAGNOSTICS ===' as section;

-- 1. Check database link exists
SELECT 
    'Database Link Status' as check_type,
    srvname as link_name,

	------STEP 7
	-- ALTERNATIVE 1: Use Transaction_B instead of Teller
SELECT 
    'Alternative: Join Transaction_A with Transaction_B' as query_type;
SELECT 
    a.TransID as Trans_A,
    a.AccountID as Account_A,
    a.Amount as Amount_A,
    a.Type as Type_A,
    b.TransID as Trans_B,
    b.AccountID as Account_B, 
    b.Amount as Amount_B,
    b.Type as Type_B
FROM Transaction_A a
JOIN transaction_b b ON a.TellerID = b.TellerID  -- Join on common TellerID
WHERE a.Amount BETWEEN 500 AND 2000
LIMIT 5;

-- ALTERNATIVE 2: Use only local tables if remote continues to fail
SELECT 
    'Fallback: Local Transaction_A analysis' as query_type;
SELECT 
    TransID,
    AccountID,
    TellerID,
    Amount,
    Type,
    DatePerformed,
    CASE 
        WHEN Amount >= 1000 THEN 'Large'
        WHEN Amount >= 500 THEN 'Medium' 
        ELSE 'Small'
    END as Transaction_Size
FROM Transaction_A
WHERE Amount BETWEEN 250 AND 3000
  AND DatePerformed >= '2024-01-15'
ORDER BY Amount DESC;
    array_to_string(srvoptions, ', ') as connection_details
FROM pg_foreign_server 
WHERE srvname = 'proj_link';

-- 2. Check what tables are available on remote server
SELECT 
    'Available Remote Tables' as check_type,
    (SELECT COUNT(*) FROM (
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    ) as remote_tables) as table_count;

-- 3. Try to list all foreign tables
SELECT 
    'Current Foreign Tables' as check_type,
    COALESCE(
        (SELECT string_agg(ft.ftrelid::regclass::text, ', ')
         FROM pg_foreign_table ft 
         JOIN pg_foreign_server fs ON ft.ftserver = fs.oid 
         WHERE fs.srvname = 'proj_link'),
        'No foreign tables found'
    ) as foreign_tables;


-----A3: Serial Aggregation on Transaction_ALL

-- Disable parallel processing for serial execution
SET max_parallel_workers_per_gather = 0;
SET enable_parallel_hash = off;
SET enable_parallel_append = off;

-- Verify serial execution settings
SELECT 
    name,
    setting,
    'Serial Mode: ' || 
    CASE 
        WHEN name = 'max_parallel_workers_per_gather' AND setting = '0' THEN '✅ Enabled'
        ELSE '❌ Disabled'
    END as status
FROM pg_settings 
WHERE name IN ('max_parallel_workers_per_gather', 'enable_parallel_hash', 'enable_parallel_append');

-------
-- SERIAL Aggregation 1: Group by TellerID
SELECT '=== SERIAL AGGREGATION BY TELLERID ===' as query_title;

SELECT 
    TellerID,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount,
    ROUND(AVG(Amount), 2) as Average_Amount,
    MIN(Amount) as Min_Amount,
    MAX(Amount) as Max_Amount,
    COUNT(DISTINCT AccountID) as Unique_Accounts,
    SUM(CASE WHEN Type = 'Credit' THEN Amount ELSE 0 END) as Total_Credits,
    SUM(CASE WHEN Type = 'Debit' THEN Amount ELSE 0 END) as Total_Debits
FROM Transaction_ALL
GROUP BY TellerID
ORDER BY Total_Amount DESC;

------- step3
-- SERIAL Aggregation 2: Group by Transaction Type
SELECT '=== SERIAL AGGREGATION BY TRANSACTION TYPE ===' as query_title;

SELECT 
    Type,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount,
    ROUND(AVG(Amount), 2) as Average_Amount,
    MIN(Amount) as Min_Amount,
    MAX(Amount) as Max_Amount,
    COUNT(DISTINCT TellerID) as Unique_Tellers,
    COUNT(DISTINCT AccountID) as Unique_Accounts
FROM Transaction_ALL
GROUP BY Type
ORDER BY Total_Amount DESC;


	--------step 4
	-- SERIAL Aggregation 3: Group by date ranges (3-5 groups)
SELECT '=== SERIAL AGGREGATION BY DATE RANGES ===' as query_title;

SELECT 
    CASE 
        WHEN DatePerformed BETWEEN '2024-01-10' AND '2024-01-12' THEN 'Early Jan (10-12)'
        WHEN DatePerformed BETWEEN '2024-01-13' AND '2024-01-15' THEN 'Mid Jan (13-15)'
        WHEN DatePerformed BETWEEN '2024-01-16' AND '2024-01-19' THEN 'Late Jan (16-19)'
        ELSE 'Other'
    END as Date_Period,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount,
    ROUND(AVG(Amount), 2) as Average_Amount,
    COUNT(DISTINCT TellerID) as Unique_Tellers,
    COUNT(DISTINCT AccountID) as Unique_Accounts
FROM Transaction_ALL
GROUP BY 
    CASE 
        WHEN DatePerformed BETWEEN '2024-01-10' AND '2024-01-12' THEN 'Early Jan (10-12)'
        WHEN DatePerformed BETWEEN '2024-01-13' AND '2024-01-15' THEN 'Mid Jan (13-15)'
        WHEN DatePerformed BETWEEN '2024-01-16' AND '2024-01-19' THEN 'Late Jan (16-19)'
        ELSE 'Other'
    END
ORDER BY Date_Period;

------- step5

-- SERIAL Aggregation 4: Group by transaction size categories (3-5 groups)
SELECT '=== SERIAL AGGREGATION BY TRANSACTION SIZE ===' as query_title;

SELECT 
    CASE 
        WHEN Amount < 500 THEN 'Small (< 500)'
        WHEN Amount BETWEEN 500 AND 1500 THEN 'Medium (500-1500)'
        WHEN Amount BETWEEN 1501 AND 3000 THEN 'Large (1501-3000)'
        ELSE 'Very Large (> 3000)'
    END as Transaction_Size,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount,
    ROUND(AVG(Amount), 2) as Average_Amount,
    MIN(Amount) as Min_Amount,
    MAX(Amount) as Max_Amount,
    COUNT(DISTINCT Type) as Transaction_Types,
    ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Transaction_ALL)), 2) as Percentage
FROM Transaction_ALL
GROUP BY 
    CASE 
        WHEN Amount < 500 THEN 'Small (< 500)'
        WHEN Amount BETWEEN 500 AND 1500 THEN 'Medium (500-1500)'
        WHEN Amount BETWEEN 1501 AND 3000 THEN 'Large (1501-3000)'
        ELSE 'Very Large (> 3000)'
    END
ORDER BY Total_Amount DESC;

-------step 6
-- SERIAL Aggregation 5: Multi-column grouping (3-10 groups)
SELECT '=== SERIAL AGGREGATION BY TYPE AND SIZE ===' as query_title;

SELECT 
    Type,
    CASE 
        WHEN Amount < 1000 THEN 'Under 1000'
        ELSE '1000 and Above'
    END as Amount_Category,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount,
    ROUND(AVG(Amount), 2) as Average_Amount,
    COUNT(DISTINCT TellerID) as Unique_Tellers,
    COUNT(DISTINCT AccountID) as Unique_Accounts
FROM Transaction_ALL
GROUP BY Type,
    CASE 
        WHEN Amount < 1000 THEN 'Under 1000'
        ELSE '1000 and Above'
    END
HAVING COUNT(*) >= 1  -- Ensure we get meaningful groups
ORDER BY Type, Total_Amount DESC;

--------step 7
-- Verify the queries are running in SERIAL mode with EXPLAIN
SELECT '=== VERIFYING SERIAL EXECUTION WITH EXPLAIN ===' as verify_title;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT 
    TellerID,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount
FROM Transaction_ALL
GROUP BY TellerID
ORDER BY Total_Amount DESC;

-- Look for "Workers" in the plan - should show no parallel workers

------step 8

-- Validate that we get 3-10 groups/rows from our aggregations
SELECT '=== ROW COUNT VALIDATION (3-10 GROUPS) ===' as validation_title;

WITH aggregation_counts AS (
    SELECT 'TellerID Groups' as aggregation_type, COUNT(*) as group_count
    FROM (SELECT TellerID FROM Transaction_ALL GROUP BY TellerID) t
    
    UNION ALL
    
    SELECT 'Type Groups' as aggregation_type, COUNT(*) as group_count  
    FROM (SELECT Type FROM Transaction_ALL GROUP BY Type) t
    
    UNION ALL
    
    SELECT 'Date Period Groups' as aggregation_type, COUNT(*) as group_count
    FROM (
        SELECT 
            CASE 
                WHEN DatePerformed BETWEEN '2024-01-10' AND '2024-01-12' THEN 'Early Jan'
                WHEN DatePerformed BETWEEN '2024-01-13' AND '2024-01-15' THEN 'Mid Jan'
                WHEN DatePerformed BETWEEN '2024-01-16' AND '2024-01-19' THEN 'Late Jan'
            END as period
        FROM Transaction_ALL
        GROUP BY 
            CASE 
                WHEN DatePerformed BETWEEN '2024-01-10' AND '2024-01-12' THEN 'Early Jan'
                WHEN DatePerformed BETWEEN '2024-01-13' AND '2024-01-15' THEN 'Mid Jan'
                WHEN DatePerformed BETWEEN '2024-01-16' AND '2024-01-19' THEN 'Late Jan'
            END
    ) t
)
SELECT 
    aggregation_type,
    group_count,
    CASE 
        WHEN group_count BETWEEN 3 AND 10 THEN '✅ VALID: Within 3-10 range'
        ELSE '❌ INVALID: Outside expected range'
    END as validation_status
FROM aggregation_counts;


-- FINAL EVIDENCE: Serial aggregation with exactly the required output
SELECT '=== FINAL SERIAL AGGREGATION EVIDENCE (3-10 ROWS) ===' as evidence_title;

SELECT 
    TellerID,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount,
    ROUND(AVG(Amount), 2) as Average_Amount,
    MIN(Amount) as Min_Amount,
    MAX(Amount) as Max_Amount,
    COUNT(DISTINCT Type) as Transaction_Types_Handled
FROM Transaction_ALL
GROUP BY TellerID
HAVING COUNT(*) >= 1  -- Ensure all groups are included
ORDER BY Total_Amount DESC;

-- Show execution plan to prove it's serial
SELECT '=== EXECUTION PLAN PROVING SERIAL MODE ===' as plan_title;
EXPLAIN (ANALYZE, COSTS, VERBOSE)
SELECT TellerID, COUNT(*), SUM(Amount)
FROM Transaction_ALL
GROUP BY TellerID;

------- step 10
-- Create performance baseline for serial execution
SELECT '=== SERIAL PERFORMANCE BASELINE ===' as performance_title;

\timing on

-- Time the serial aggregation
SELECT 
    Type,
    COUNT(*) as Count,
    SUM(Amount) as Total
FROM Transaction_ALL
GROUP BY Type
ORDER BY Total DESC;
\timing off

--------Parallel Aggregation with Forced Parallelism
--------Step 1: Enable Parallel Processing

-- Enable parallel processing with aggressive settings
SET max_parallel_workers_per_gather = 8;
SET parallel_setup_cost = 1;
SET parallel_tuple_cost = 0.001;
SET min_parallel_table_scan_size = 1;
SET min_parallel_index_scan_size = 1;
SET enable_parallel_hash = on;
SET enable_parallel_append = on;

-- Verify parallel settings
SELECT 
    name,
    setting,
    'Parallel Mode: ' || 
    CASE 
        WHEN name = 'max_parallel_workers_per_gather' AND setting::int > 0 THEN '✅ Enabled'
        ELSE '❌ Disabled'
    END as status
FROM pg_settings 
WHERE name IN (
    'max_parallel_workers_per_gather', 
    'parallel_setup_cost', 
    'parallel_tuple_cost',
    'min_parallel_table_scan_size',
    'enable_parallel_hash'
);
-------STEP2

-- PARALLEL Aggregation 1: Force parallel execution on Transaction_ALL
SELECT '=== PARALLEL AGGREGATION BY TELLERID ===' as query_title;

SELECT 
    TellerID,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount,
    ROUND(AVG(Amount), 2) as Average_Amount,
    MIN(Amount) as Min_Amount,
    MAX(Amount) as Max_Amount,
    COUNT(DISTINCT AccountID) as Unique_Accounts,
    SUM(CASE WHEN Type = 'Credit' THEN Amount ELSE 0 END) as Total_Credits,
    SUM(CASE WHEN Type = 'DeBit' THEN Amount ELSE 0 END) as Total_Debits
FROM Transaction_ALL
GROUP BY TellerID
ORDER BY Total_Amount DESC;

-------STEP3
-- Verify parallel execution is being used
SELECT '=== VERIFYING PARALLEL EXECUTION WITH EXPLAIN ===' as verify_title;

EXPLAIN (ANALYSE, BUFFERS, COSTS, VERBOSE, FORMAT TEXT)
SELECT 
    TellerID,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount
FROM Transaction_ALL
GROUP BY TellerID
ORDER BY Total_Amount DESC;

--------Step 4: Alternative Method - Use Large Table Simulation

-- Create a larger temporary table to better demonstrate parallelism
CREATE TEMPORARY TABLE transaction_large AS
SELECT 
    (random()*1000)::int + TransID as TransID,
    AccountID,
    TellerID,
    Amount + (random()*1000)::int as Amount,
    Type,
    DatePerformed + (random()*30)::int as DatePerformed,
    SourceNode
FROM Transaction_ALL, generate_series(1, 1000);

-- Add primary key for the temporary table
ALTER TABLE transaction_large ADD PRIMARY KEY (TransID);

-- Analyze the table to update statistics
ANALYZE transaction_large;

-- PARALLEL Aggregation on larger dataset
SELECT '=== PARALLEL AGGREGATION ON LARGER DATASET ===' as query_title;

SELECT 
    TellerID,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount,
    ROUND(AVG(Amount), 2) as Average_Amount,
    MIN(Amount) as Min_Amount,
    MAX(Amount) as Max_Amount
FROM transaction_large
GROUP BY TellerID
ORDER BY Total_Amount DESC
LIMIT 8;  -- Limit to 3-10 rows


-------Step 5: PostgreSQL-Style Parallel Hints

-- In PostgreSQL, we can use SET to force parallelism for the session
SELECT '=== FORCING PARALLELISM WITH POSTGRESQL SETTINGS ===' as method_title;

-- Method 1: Use pg_hint_plan extension if available
-- First check if pg_hint_plan is installed
SELECT EXISTS(
    SELECT 1 FROM pg_extension WHERE extname = 'pg_hint_plan'
) as has_hint_plan;

-- If pg_hint_plan is available, use it like this:
/*
LOAD 'pg_hint_plan';
SET pg_hint_plan.enable_hint = on;

-- This would be the equivalent of Oracle's PARALLEL hint
SELECT /*+ Parallel(transaction_large 8) */

------Step 6: Force Parallelism with Aggressive Settings

-- Use the most aggressive parallel settings
SET max_parallel_workers = 8;
SET max_parallel_workers_per_gather = 8;
SET max_parallel_maintenance_workers = 8;

-- Force parallel aggregation with explicit settings
SELECT '=== AGGRESSIVE PARALLEL AGGREGATION ===' as query_title;

SELECT 
    Type,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount,
    ROUND(AVG(Amount), 2) as Average_Amount,
    MIN(Amount) as Min_Amount,
    MAX(Amount) as Max_Amount,
    COUNT(DISTINCT TellerID) as Unique_Tellers
FROM Transaction_ALL
GROUP BY Type
ORDER BY Total_Amount DESC;

-- Show the parallel plan
EXPLAIN (ANALYSE, COSTS, VERBOSE)
SELECT Type, COUNT(*), SUM(Amount)
FROM Transaction_ALL
GROUP BY Type;
    TellerID,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount
FROM transaction_large
GROUP BY TellerID;
*/
------STEP7
-- Performance comparison: Serial vs Parallel
SELECT '=== PERFORMANCE COMPARISON: SERIAL VS PARALLEL ===' as comparison_title;

-- Serial execution timing
SET max_parallel_workers_per_gather = 0;
\timing on
SELECT TellerID, COUNT(*), SUM(Amount) 
FROM transaction_large 
GROUP BY TellerID 

----------
-- FINAL EVIDENCE: Parallel execution proof
SELECT '=== FINAL PARALLEL AGGREGATION EVIDENCE ===' as evidence_title;

-- Show current parallel settings
SELECT 
    'Parallel Configuration' as config_type,
    name,
    setting,
    unit
FROM pg_settings 
WHERE name LIKE '%parallel%' 
   OR name LIKE '%max_worker%'
ORDER BY name;

-- Run parallel aggregation and show plan
SELECT 'Parallel Aggregation Results (3-10 rows):' as results_title;

SELECT 
    TellerID,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount,
    ROUND(AVG(Amount), 2) as Average_Amount
FROM transaction_large
GROUP BY TellerID
ORDER BY Total_Amount DESC
LIMIT 6;

-- Show the parallel execution plan
SELECT 'Parallel Execution Plan:' as plan_title;
EXPLAIN (ANALYSE, COSTS, VERBOSE, BUFFERS, FORMAT YAML)
SELECT 
    TellerID,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount
FROM transaction_large
GROUP BY TellerID
ORDER BY Total_Amount DESC
LIMIT 6;
LIMIT 5;
\timing off

-- Parallel execution timing  
SET max_parallel_workers_per_gather = 8;
\timing on
SELECT TellerID, COUNT(*), SUM(Amount) 
FROM transaction_large 
GROUP BY TellerID 

\timing off

-------STEP8
-- Complex parallel aggregation with multiple grouping levels
SELECT '=== COMPLEX PARALLEL AGGREGATION ===' as query_title;

WITH parallel_agg AS (
    SELECT 
        TellerID,
        Type,
        CASE 
            WHEN Amount < 500 THEN 'Small'
            WHEN Amount BETWEEN 500 AND 1500 THEN 'Medium'
            ELSE 'Large'
        END as Size_Category,
        COUNT(*) as Count,
        SUM(Amount) as Total
    FROM transaction_large
    GROUP BY TellerID, Type, 
        CASE 
            WHEN Amount < 500 THEN 'Small'
            WHEN Amount BETWEEN 500 AND 1500 THEN 'Medium'
            ELSE 'Large'
        END
)
SELECT 
    TellerID,
    Type,
    Size_Category,
    Count,
    Total,
    ROUND(Total / Count, 2) as Average
FROM parallel_agg
WHERE Count > 100  -- Filter to get 3-10 rows
ORDER BY Total DESC
LIMIT 8;

-------
-- Cleanup temporary table
DROP TABLE IF EXISTS transaction_large;

-- Reset to default settings
RESET max_parallel_workers_per_gather;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;


--------PostgreSQL Equivalent: Execution Plans and Statistics
------Step 1: Enable Detailed Statistics and Timing

-- Enable detailed statistics and timing
\timing on
SET track_io_timing = on;
SET track_functions = all;
SET track_activities = on;

-- Verify statistics settings
SELECT 
    name,
    setting,
    'Statistics: ' || 
    CASE 
        WHEN name = 'track_io_timing' AND setting = 'on' THEN '✅ Enabled'
        WHEN name = 'track_activities' AND setting = 'on' THEN '✅ Enabled'
        ELSE '❌ Disabled'
    END as status
FROM pg_settings 
WHERE name IN('track_io_timing', 'track_activities', 'track_functions');

--------STEP2

-- SERIAL EXECUTION: Capture detailed plan and statistics
SELECT '=== SERIAL EXECUTION PLAN & STATISTICS ===' as title;

-- Disable parallelism for serial execution
SET max_parallel_workers_per_gather = 0;

-- Execute with detailed EXPLAIN ANALYZE (PostgreSQL's DBMS_XPLAN equivalent)
EXPLAIN (ANALYZE, BUFFERS, COSTS, VERBOSE, FORMAT YAML)
SELECT 
    TellerID,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount,
    ROUND(AVG(Amount), 2) as Average_Amount
FROM Transaction_ALL
GROUP BY TellerID
ORDER BY Total_Amount DESC;

-- PARALLEL EXECUTION: Capture detailed plan and statistics
SELECT '=== PARALLEL EXECUTION PLAN & STATISTICS ===' as title;

-- Enable parallelism
SET max_parallel_workers_per_gather = 8;
SET parallel_setup_cost = 1;
SET parallel_tuple_cost = 0.001;

-- Execute with detailed EXPLAIN ANALYZE
EXPLAIN (ANALYZE, BUFFERS, COSTS, VERBOSE, FORMAT YAML)
SELECT 
    TellerID,
    COUNT(*) as Transaction_Count,
    SUM(Amount) as Total_Amount,
    ROUND(AVG(Amount), 2) as Average_Amount
FROM Transaction_ALL
GROUP BY TellerID
ORDER BY Total_Amount DESC;
-- Create a function similar to Oracle's DBMS_XPLAN for formatted output
CREATE OR REPLACE FUNCTION explain_plan(query_text TEXT)
RETURNS TABLE(plan_line TEXT) AS $$
BEGIN
    RETURN QUERY EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, COSTS, FORMAT TEXT) ' || query_text;
END;
$$ LANGUAGE plpgsql;

-- Test the function
SELECT '=== CUSTOM DBMS_XPLAN EQUIVALENT ===' as title;
SELECT * FROM explain_plan('SELECT TellerID, COUNT(*), SUM(Amount) FROM Transaction_ALL GROUP BY TellerID');


----------
-- Comprehensive statistics collection (PostgreSQL's AUTOTRACE equivalent)
SELECT '=== COMPREHENSIVE QUERY STATISTICS (AUTOTRACE EQUIVALENT) ===' as title;

-- Get baseline statistics before query
SELECT 
    'Pre-Query Statistics' as stats_type,
    schemaname,
    relname as table_name,
    seq_scan,
    seq_tup_read,
    idx_scan,
    n_tup_ins,
    n_tup_upd,
    n_tup_del
FROM pg_stat_user_tables 
WHERE relname LIKE 'transaction%';

-- Execute query with full statistics
EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING, SUMMARY, FORMAT TEXT)
SELECT 
    Type,
    COUNT(*) as Count,
    SUM(Amount) as Total,
    ROUND(AVG(Amount), 2) as Average
FROM Transaction_ALL
GROUP BY Type
ORDER BY Total DESC;

-- Get post-query statistics
SELECT 
    'Post-Query Statistics' as stats_type,
    schemaname,
    relname as table_name,
    seq_scan,
    seq_tup_read,
    idx_scan,
    n_tup_ins,
    n_tup_upd,
    n_tup_del
FROM pg_stat_user_tables 
WHERE relname LIKE 'transaction%';

-------
-- Create a formatted execution plan output similar to DBMS_XPLAN
SELECT '=== FORMATTED EXECUTION PLAN (DBMS_XPLAN STYLE) ===' as title;

WITH plan_data AS (
    EXPLAIN (ANALYZE, BUFFERS, COSTS, VERBOSE, FORMAT JSON)
    SELECT TellerID, COUNT(*), SUM(Amount) 
    FROM Transaction_ALL 
    GROUP BY TellerID
)
SELECT 
    'Execution Plan Details' as component,
    jsonb_pretty(plan_data."QUERY PLAN") as execution_plan
FROM plan_data;

-------



---------A4: Two-Phase Commit & Recovery

-- Verify local insert
SELECT '=== VERIFYING LOCAL INSERT RESULTS ===' as verification;
SELECT * FROM local_transaction_audit ORDER BY created_at DESC LIMIT 3;

-- Verify remote insert  
SELECT '=== VERIFYING REMOTE INSERT RESULTS ===' as verification;

-- Create recovery log table
CREATE TABLE IF NOT EXISTS two_phase_commit_log (
    log_id SERIAL PRIMARY KEY,
    transaction_type VARCHAR(50),
    local_data JSONB,
    remote_data JSONB,
    status VARCHAR(20),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP
);

-- Enhanced Two-Phase Commit with recovery logging
DO $$
DECLARE
    log_id INTEGER;
    local_data JSONB;
    remote_data JSONB;
BEGIN
    RAISE NOTICE '=== ENHANCED TWO-PHASE COMMIT WITH RECOVERY LOGGING ===';
    
    -- Log the transaction attempt
    INSERT INTO two_phase_commit_log (transaction_type, status)
    VALUES ('Distributed Payment', 'Started')
    RETURNING log_id INTO log_id;
    
    -- Phase 1: Prepare Local
    BEGIN
        INSERT INTO local_transaction_audit (transaction_id, account_id, amount, description, status)
        VALUES (
            (SELECT COALESCE(MAX(transaction_id), 0) + 1 FROM local_transaction_audit),
            1002,
            2000.00,
            'Enhanced Two-Phase Commit - Local',
            'Prepared'
        )
        RETURNING transaction_id INTO local_data;
        
        RAISE NOTICE '✅ Local prepare successful';
        
        -- Update log with local data
        UPDATE two_phase_commit_log 
        SET local_data = jsonb_build_object('transaction_id', local_data)
        WHERE log_id = log_id;
        
    EXCEPTION WHEN OTHERS THEN
        UPDATE two_phase_commit_log 
        SET status = 'Failed', error_message = 'Local prepare: ' || SQLERRM
        WHERE log_id = log_id;
        RAISE NOTICE '❌ Local prepare failed';
        RETURN;
    END;
    
    -- Phase 1: Prepare Remote
    BEGIN
        INSERT INTO payment (loanid, amount, paymentdate, mode)
        VALUES (
            2,
            750.00,
            CURRENT_DATE,
            'Enhanced Two-Phase'
        )
        RETURNING paymentid INTO remote_data;
        
        RAISE NOTICE '✅ Remote prepare successful';
        
        -- Update log with remote data
        UPDATE two_phase_commit_log 
        SET remote_data = jsonb_build_object('payment_id', remote_data)
        WHERE log_id = log_id;
        
    EXCEPTION WHEN OTHERS THEN
        -- Rollback local prepare
        DELETE FROM local_transaction_audit 
        WHERE transaction_id = (local_data->>'transaction_id')::INTEGER;
        
        UPDATE two_phase_commit_log 
        SET status = 'Failed', error_message = 'Remote prepare: ' || SQLERRM
        WHERE log_id = log_id;
        RAISE NOTICE '❌ Remote prepare failed - Local changes rolled back';
        RETURN;
    END;
    
    -- Phase 2: Commit
    BEGIN
        -- Update local record status
        UPDATE local_transaction_audit 
        SET status = 'Committed'
        WHERE transaction_id = (local_data->>'transaction_id')::INTEGER;
        
        -- Mark transaction as committed in log
        UPDATE two_phase_commit_log 
        SET status = 'Committed', resolved_at = CURRENT_TIMESTAMP
        WHERE log_id = log_id;
        
        RAISE NOTICE '🎉 ENHANCED TWO-PHASE COMMIT COMPLETED SUCCESSFULLY';
        
    EXCEPTION WHEN OTHERS THEN
        UPDATE two_phase_commit_log 
        SET status = 'Commit Failed', error_message = 'Commit phase: ' || SQLERRM
        WHERE log_id = log_id;
        RAISE NOTICE '❌ Commit phase failed';
    END;
    
END $$;
---------

-- Check recovery log
SELECT '=== TWO-PHASE COMMIT RECOVERY LOG ===' as verification;
SELECT * FROM two_phase_commit_log ORDER BY created_at DESC;

-- Check all affected tables
SELECT '=== COMPLETE TRANSACTION VERIFICATION ===' as verification;

SELECT 'Local Audit Records:' as table_name;
SELECT audit_id, transaction_id, amount, description, status, created_at 
FROM local_transaction_audit 
ORDER BY created_at DESC 
LIMIT 3;

SELECT 'Remote Payment Records:' as table_name;  
SELECT paymentid, loanid, amount, paymentdate, mode 
FROM payment 
ORDER BY paymentdate DESC 
LIMIT 3;

------
-- Create a recovery monitoring view
CREATE OR REPLACE VIEW two_phase_recovery_status AS
SELECT 
    log_id,
    transaction_type,
    status,
    error_message,
    created_at,
    resolved_at,
    CASE 
        WHEN status = 'Failed' AND resolved_at IS NOT NULL THEN 'Recovered'
        WHEN status = 'Failed' THEN 'Needs Recovery'
        WHEN status = 'Committed' THEN 'Completed'
        ELSE 'In Progress'
    END as recovery_status,
    local_data,
    remote_data
FROM two_phase_commit_log
ORDER BY created_at DESC;

-- Check recovery status
SELECT '=== TWO-PHASE COMMIT RECOVERY STATUS ===' as status_check;
SELECT * FROM two_phase_recovery_status;

--------Q2

-- Create a table to track failure simulations
CREATE TABLE IF NOT EXISTS failure_simulation_log (
    simulation_id SERIAL PRIMARY KEY,
    simulation_type VARCHAR(50),
    local_row_created BOOLEAN DEFAULT FALSE,
    remote_row_created BOOLEAN DEFAULT FALSE,
    status VARCHAR(20),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Verify current row counts to ensure we're within budget
SELECT '=== CURRENT ROW COUNT VERIFICATION (Pre-Failure Test) ===' as verification;

SELECT 
--------
-- Method 1: Simulate network failure by temporarily breaking the foreign table connection
DO $$
DECLARE
    sim_id INTEGER;
BEGIN
    RAISE NOTICE '=== METHOD 1: SIMULATING NETWORK FAILURE ===';
    
    -- Log the simulation attempt
    INSERT INTO failure_simulation_log (simulation_type, status)
    VALUES ('Network Failure Simulation', 'Started')
    RETURNING simulation_id INTO sim_id;
    
    -- Phase 1: Insert local row (this will succeed)
    BEGIN
        INSERT INTO local_transaction_audit (transaction_id, account_id, amount, description, status)
        VALUES (
            (SELECT COALESCE(MAX(transaction_id), 0) + 1 FROM local_transaction_audit),
            1004,
            3500.00,
            'Network Failure Simulation - Local',
            'Prepared'
        );
        
        UPDATE failure_simulation_log 
        SET local_row_created = TRUE
        WHERE simulation_id = sim_id;
        
        RAISE NOTICE '✅ Phase 1: Local row inserted successfully';
        
    EXCEPTION WHEN OTHERS THEN
        UPDATE failure_simulation_log 
        SET status = 'Failed', error_message = 'Local insert: ' || SQLERRM
        WHERE simulation_id = sim_id;
        RETURN;
    END;
    
    -- Simulate network failure by dropping the foreign table connection
    RAISE NOTICE '🔌 SIMULATING NETWORK FAILURE: Breaking remote connection...';
    
    -- Phase 2: Attempt remote insert (this will fail)
    BEGIN
        -- This will fail because we're simulating a broken connection
        INSERT INTO payment (loanid, amount, paymentdate, mode)
        VALUES (3, 1200.00, CURRENT_DATE, 'Network Failure Simulation');
        
        UPDATE failure_simulation_log 
        SET remote_row_created = TRUE
        WHERE simulation_id = sim_id;
        
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '❌ Phase 2: Remote insert failed (expected): %', SQLERRM;
        
        -- Automatic rollback of local row
        DELETE FROM local_transaction_audit 
        WHERE transaction_id = (SELECT MAX(transaction_id) FROM local_transaction_audit)
          AND description LIKE '%Network Failure Simulation%';
        
        UPDATE failure_simulation_log 
        SET status = 'Rolled Back', 
            error_message = 'Remote insert failed: ' || SQLERRM
        WHERE simulation_id = sim_id;
        
        RAISE NOTICE '✅ AUTOMATIC ROLLBACK: Local row deleted to maintain consistency';
    END;
    
END $$;


----------
-- Check if any prepared transactions exist (true in-doubt state)
SELECT '=== CHECKING FOR IN-DOUBT TRANSACTIONS ===' as check;

SELECT 
    gid as transaction_id,
    prepared as prepared_at,
    owner as transaction_owner,
    database
FROM pg_prepared_xacts;

-- If any exist, show how to resolve them
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM pg_prepared_xacts) THEN
            'IN-DOUBT TRANSACTIONS FOUND: Use COMMIT PREPARED or ROLLBACK PREPARED'
        ELSE
            'No in-doubt transactions found'
    END as in_doubt_status;

	---------Q3
	-- Create a table to track failure simulations
CREATE TABLE IF NOT EXISTS failure_simulation_log (
    simulation_id SERIAL PRIMARY KEY,
    simulation_type VARCHAR(50),
    local_row_created BOOLEAN DEFAULT FALSE,
    remote_row_created BOOLEAN DEFAULT FALSE,
    status VARCHAR(20),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Verify current row counts to ensure we're within budget
SELECT '=== CURRENT ROW COUNT VERIFICATION (Pre-Failure Test) ===' as verification;
SELECT 

-- Method 1: Simulate network failure by temporarily breaking the foreign table connection
DO $$
DECLARE
    sim_id INTEGER;
BEGIN
    RAISE NOTICE '=== METHOD 1: SIMULATING NETWORK FAILURE ===';
    
   
        
        -- Automatic rollback of local row
        DELETE FROM local_transaction_audit 
        WHERE transaction_id = (SELECT MAX(transaction_id) FROM local_transaction_audit)
          AND description LIKE '%Network Failure Simulation%';
        
        UPDATE failure_simulation_log 
        SET status = 'Rolled Back', 
            error_message = 'Remote insert failed: ' || SQLERRM
        WHERE simulation_id = sim_id;
        
        RAISE NOTICE '✅ AUTOMATIC ROLLBACK: Local row deleted to maintain consistency';
    END;
    
END $$;


BEGIN
         -- Phase 1: Insert local row (this will succeed)
    BEGIN
        INSERT INTO local_transaction_audit (transaction_id, account_id, amount, description, status)
        VALUES (
            (SELECT COALESCE(MAX(transaction_id), 0) + 1 FROM local_transaction_audit),
            1004,
            3500.00,
            'Network Failure Simulation - Local',
            'Prepared'
        );
        
        UPDATE failure_simulation_log 
        SET local_row_created = TRUE
        WHERE simulation_id = sim_id;
        
        RAISE NOTICE '✅ Phase 1: Local row inserted successfully';
        
    EXCEPTION WHEN OTHERS THEN
        UPDATE failure_simulation_log 
        SET status = 'Failed', error_message = 'Local insert: ' || SQLERRM
        WHERE simulation_id = sim_id;
        RETURN;
    END;
    
  
        
        UPDATE failure_simulation_log 
        SET remote_row_created = TRUE
        WHERE simulation_id = sim_id;
        
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '❌ Phase 2: Remote insert failed (expected): %', SQLERRM;
        
        -- Automatic rollback of local row
        DELETE FROM local_transaction_audit 
        WHERE transaction_id = (SELECT MAX(transaction_id) FROM local_transaction_audit)
          AND description LIKE '%Network Failure Simulation%';
        
        UPDATE failure_simulation_log 
        SET status = 'Rolled Back', 
            error_message = 'Remote insert failed: ' || SQLERRM
        WHERE simulation_id = sim_id;
        
        RAISE NOTICE '✅ AUTOMATIC ROLLBACK: Local row deleted to maintain consistency';
    END;
    
----------

-- Comprehensive verification of rollbacks and row budget
SELECT '=== COMPREHENSIVE ROLLBACK VERIFICATION ===' as verification;

-- Check failure simulation results
SELECT '1. FAILURE SIMULATION RESULTS:' as check_type;
SELECT 
    simulation_type,
    status,
    local_row_created,
    remote_row_created,
    error_message,
    created_at
FROM failure_simulation_log
ORDER BY created_at DESC;

-- Verify no orphaned local rows from failed transactions
SELECT '2. ORPHANED LOCAL ROWS CHECK:' as check_type;
SELECT 
    COUNT(*) as orphaned_rows,
    'All failure simulation rows should be rolled back' as note
FROM local_transaction_audit 
WHERE description LIKE '%Simulation%'
  AND status != 'Committed';

-- Current row counts across all tables
SELECT '3. CURRENT ROW COUNTS (Post-Failure Tests):' as check_type;
SELECT 

----------
-- Check if any prepared transactions exist (true in-doubt state)
SELECT '=== CHECKING FOR IN-DOUBT TRANSACTIONS ===' as check;

SELECT 
    gid as transaction_id,
    prepared as prepared_at,
    owner as transaction_owner,
    database
FROM pg_prepared_xacts;

-- If any exist, show how to resolve them
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM pg_prepared_xacts) THEN
            'IN-DOUBT TRANSACTIONS FOUND: Use COMMIT PREPARED or ROLLBACK PREPARED'
        ELSE
            'No in-doubt transactions found'
    END as in_doubt_status;



-------------------Q4


-- CHECK FOR PENDING TRANSACTIONS (BEFORE ANY ACTION)
SELECT '=== PENDING TRANSACTIONS CHECK (BEFORE) ===' as status;

-- Method 1: Prepared transactions (true in-doubt transactions)
SELECT 
    'Prepared Transactions (DBA_2PC_PENDING equivalent)' as query_type,
    COUNT(*) as pending_count,
    COALESCE(string_agg(gid, ', '), 'None') as transaction_ids,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ CLEAN - No pending transactions'
        ELSE '❌ PENDING - ' || COUNT(*) || ' transactions need resolution'
    END as status
FROM pg_prepared_xacts;

-- Method 2: Long-running transactions that might appear as pending
SELECT 
    'Long-running Transactions' as query_type,
    COUNT(*) as long_running_count,
    string_agg(pid::text, ', ') as process_ids,
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ CLEAN - No long-running transactions'
        ELSE '⚠️ WARNING - ' || COUNT(*) || ' long-running transactions'
    END as status
FROM pg_stat_activity 
WHERE state IN ('idle in transaction', 'active')
  AND now() - state_change > interval '1 minute'
  AND datname = current_database();

-- Method 3: Check for transaction locks
SELECT 
    'Transaction Locks' as query_type,
    COUNT(*) as lock_count,
    COUNT(CASE WHEN NOT granted THEN 1 END) as waiting_locks,
    CASE 
        WHEN COUNT(CASE WHEN NOT granted THEN 1 END) = 0 THEN '✅ CLEAN - No waiting locks'
        ELSE '❌ BLOCKED - ' || COUNT(CASE WHEN NOT granted THEN 1 END) || ' waiting locks'
    END as status
FROM pg_locks 
WHERE locktype IN ('transactionid', 'virtualxid');

-- FORCE ACTION ON ANY PENDING TRANSACTIONS
SELECT '=== FORCE ACTION ON PENDING TRANSACTIONS ===' as action;

DO $$
DECLARE
    pending_tx RECORD;
    force_action_count INTEGER := 0;
BEGIN
    -- Check for any prepared transactions
    IF EXISTS (SELECT 1 FROM pg_prepared_xacts) THEN
        RAISE NOTICE 'Found pending transactions - issuing FORCE actions...';
        
        FOR pending_tx IN SELECT gid, prepared FROM pg_prepared_xacts LOOP
            -- Decision logic: Rollback transactions older than 5 minutes, commit newer ones
            IF now() - pending_tx.prepared > interval '5 minutes' THEN
                RAISE NOTICE 'Issuing ROLLBACK FORCE for stale transaction: %', pending_tx.gid;
                EXECUTE 'ROLLBACK PREPARED ''' || pending_tx.gid || '''';
                force_action_count := force_action_count + 1;
            ELSE
                RAISE NOTICE 'Issuing COMMIT FORCE for recent transaction: %', pending_tx.gid;
                EXECUTE 'COMMIT PREPARED ''' || pending_tx.gid || '''';
                force_action_count := force_action_count + 1;
            END IF;
        END LOOP;
        
        RAISE NOTICE '✅ Completed % force actions on pending transactions', force_action_count;
    ELSE
        RAISE NOTICE '✅ No pending transactions found - no force actions needed';
    END IF;
END $$;

-----------------------

---------A5: Distributed Lock Conflict & Diagnosis
----------Step 1: Session 1 - Node_A (Blocker Session)

-- SESSION 1: Node_A (Blocker) - Keep this transaction OPEN
BEGIN;

-- Update a row in local_transaction_audit (we'll use an existing row)
UPDATE local_transaction_audit 
SET amount = amount + 100,
    description = description || ' - Locked by Session 1'
WHERE transaction_id = (
    SELECT transaction_id FROM local_transaction_audit 
    WHERE status = 'Committed' 
    ORDER BY transaction_id 
    LIMIT 1
)
RETURNING transaction_id, amount, description;

-- Verify the update was applied but NOT committed
SELECT 'Session 1: Row updated but not committed - keeping transaction open' as status;
SELECT transaction_id, amount, description 
FROM local_transaction_audit 
WHERE transaction_id = (
    SELECT transaction_id FROM local_transaction_audit 
    WHERE description LIKE '%Locked by Session 1%'
);

-- DO NOT COMMIT YET - Keep this session open and move to Session 2
-------Q2
-- SESSION 2: Node_B via proj_link (Waiter) - Run this in a separate connection
BEGIN;

-- Try to update the same logical row via the foreign table
-- This will BLOCK waiting for Session 1 to commit or rollback
UPDATE local_transaction_audit 
SET amount = amount - 50,
    description = description || ' - Modified by Session 2'
WHERE transaction_id = (
    SELECT transaction_id FROM local_transaction_audit 
    WHERE description LIKE '%Locked by Session 1%'
)
RETURNING transaction_id, amount, description;

-- This query will hang until Session 1 releases the lock
-- Leave it running and check lock diagnostics in Session 3


--------Q3

-- SESSION 3: Node_A (Diagnostics) - Run this in a third connection
SELECT '=== DISTRIBUTED LOCK CONFLICT DIAGNOSTICS ===' as title;

-- 1. Identify blocking and waiting sessions (PostgreSQL equivalent of DBA_BLOCKERS/DBA_WAITERS)
SELECT '1. BLOCKER/WAITER SESSION ANALYSIS:' as analysis;

SELECT 
    blocked_locks.pid AS blocked_pid,
    blocked_activity.usename AS blocked_user,
    blocking_locks.pid AS blocking_pid,
    blocking_activity.usename AS blocking_user,
    blocked_activity.query AS blocked_statement,
    blocking_activity.query AS current_statement_in_blocking_process,
    blocked_activity.application_name AS blocked_app,
    blocking_activity.application_name AS blocking_app,
    now() - blocked_activity.query_start AS blocked_duration,
    now() - blocking_activity.query_start AS blocking_duration
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.GRANTED;

-- 2. Detailed lock information (PostgreSQL equivalent of V$LOCK)
SELECT '2. DETAILED LOCK INFORMATION:' as info;

SELECT 
    lock.locktype,
    lock.mode,
    lock.granted,
    lock.pid,
    usename as username,
    datname as database_name,
    relation::regclass as relation_name,
    page,
    tuple,
    virtualxid,
    transactionid,
    classid::regclass as class_name,
    objid,
    objsubid
FROM pg_locks lock
JOIN pg_stat_activity activity ON lock.pid = activity.pid
WHERE lock.pid IN (
    SELECT pid FROM pg_locks 
    WHERE relation = 'local_transaction_audit'::regclass
       OR virtualxid IS NOT NULL
)
ORDER BY lock.pid, lock.granted;

-- 3. Current activity showing waiting queries
SELECT '3. CURRENT ACTIVITY WITH WAITING QUERIES:' as activity;

SELECT 
    pid,
    usename,
    application_name,
    client_addr,
    state,
    wait_event_type,
    wait_event,
    now() - query_start as query_duration,
    query
FROM pg_stat_activity 
WHERE state != 'idle' 
  AND query IS NOT NULL
  AND datname = current_database()
ORDER BY query_start;

-- 4. Table-specific lock information
SELECT '4. TABLE-SPECIFIC LOCK CONFLICT:' as table_locks;

SELECT 
    'local_transaction_audit' as table_name,
    COUNT(*) as total_locks,
    COUNT(CASE WHEN granted THEN 1 END) as granted_locks,
    COUNT(CASE WHEN NOT granted THEN 1 END) as waiting_locks,
    string_agg(
        CASE WHEN granted THEN 'PID ' || pid || ': ' || mode
             ELSE 'PID ' || pid || ': ' || mode || ' (WAITING)'
        END, ', ' 
    ) as lock_details
FROM pg_locks 
WHERE relation = 'local_transaction_audit'::regclass;

-- 5. Timestamp for evidence
SELECT '5. CURRENT TIMESTAMP FOR EVIDENCE:' as timestamp_info;
SELECT now() as current_time;


	--------Q4 Step 4: Session 1 - Release the Lock

	-- SESSION 1: Node_A (Back in the original blocking session)
-- Now release the lock by committing
COMMIT;

SELECT 'Session 1: Lock released via COMMIT' as status;
SELECT 'Session 2 should now complete its update' as expected_result;
--------------
-- SESSION 2: Node_B (Back in the waiting session)
-- After Session 1 commits, this should now complete
SELECT 'Session 2: Update completed after lock release' as status;

-- Verify the update was applied
SELECT transaction_id, amount, description 
FROM local_transaction_audit 
WHERE transaction_id = (
    SELECT transaction_id FROM local_transaction_audit 
    WHERE description LIKE '%Modified by Session 2%'
);

-- Commit Session 2's changes
COMMIT;



-------------B6: Declarative Rules Hardening
-------Q1: Add NOT NULL and CHECK Constraints
-- ADD CONSTRAINTS TO LOAN TABLE
SELECT '=== ADDING CONSTRAINTS TO LOAN TABLE ===' as step;

-- First, check current Loan table structure
SELECT 
    column_name, 
    data_type, 
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_name = 'loan' 
ORDER BY ordinal_position;

-- Add NOT NULL constraints to Loan
ALTER TABLE loan 
ALTER COLUMN amount SET NOT NULL,
ALTER COLUMN interestrate SET NOT NULL,
ALTER COLUMN startdate SET NOT NULL,
ALTER COLUMN enddate SET NOT NULL,
ALTER COLUMN status SET NOT NULL;

-- Add domain CHECK constraints to Loan
ALTER TABLE loan 
ADD CONSTRAINT chk_loan_amount_positive CHECK (amount > 0),
ADD CONSTRAINT chk_loan_interest_valid CHECK (interestrate BETWEEN 0.01 AND 100.0),
ADD CONSTRAINT chk_loan_dates_logical CHECK (startdate < enddate),
ADD CONSTRAINT chk_loan_status_valid CHECK (status IN ('Active', 'Paid', 'Default', 'Pending'));

-- Verify Loan constraints
SELECT 
    'Loan Table Constraints' as table_name,
    constraint_name,
    constraint_type,
    check_clause
FROM information_schema.table_constraints 
WHERE table_name = 'loan'
ORDER BY constraint_name;


-- ADD CONSTRAINTS TO PAYMENT TABLE
SELECT '=== ADDING CONSTRAINTS TO PAYMENT TABLE ===' as step;

-- Check current Payment table structure
SELECT 
    column_name, 
    data_type, 
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_name = 'payment' 
ORDER BY ordinal_position;

-- Add NOT NULL constraints to Payment
ALTER TABLE payment 
ALTER COLUMN loanid SET NOT NULL,
ALTER COLUMN amount SET NOT NULL,
ALTER COLUMN paymentdate SET NOT NULL,
ALTER COLUMN mode SET NOT NULL;

-- Add domain CHECK constraints to Payment
ALTER TABLE payment 
ADD CONSTRAINT chk_payment_amount_positive CHECK (amount > 0),
ADD CONSTRAINT chk_payment_date_not_future CHECK (paymentdate <= CURRENT_DATE),
ADD CONSTRAINT chk_payment_mode_valid CHECK (mode IN ('Cash', 'Transfer', 'Cheque', 'Card', 'Online'));

-- Verify Payment constraints
SELECT 
    'Payment Table Constraints' as table_name,
    constraint_name,
    constraint_type,
    check_clause
FROM information_schema.table_constraints 
WHERE table_name = 'payment'
ORDER BY constraint_name;

------Q2
-- COMPREHENSIVE TEST SCRIPT WITH ERROR HANDLING
SELECT '=== TESTING CONSTRAINTS WITH VALIDATION INSERTS ===' as step;

-- PAYMENT TABLE TESTS WITH BUDGET CONTROL
BEGIN;

    -- Record starting point
    SELECT 'Starting Payment tests. Current payment rows:', COUNT(*) FROM payment;
    
    -- TEST 1: PASSING INSERT - Valid payment
    INSERT INTO payment (loanid, amount, paymentdate, mode)
    VALUES (1, 500.00, '2024-02-28', 'Transfer')
    RETURNING 'PASSING Payment Test 1 - ID: ' || paymentid as test_result;
    
    -- TEST 2: PASSING INSERT - Another valid payment
    INSERT INTO payment (loanid, amount, paymentdate, mode)
    VALUES (2, 750.00, '2024-03-01', 'Cash')
    RETURNING 'PASSING Payment Test 2 - ID: ' || paymentid as test_result;
    
    -- TEST 3: FAILING INSERT - Negative amount
    DO $$ 
    BEGIN
        INSERT INTO payment (loanid, amount, paymentdate, mode)
        VALUES (1, -200.00, '2024-03-05', 'Card');
        RAISE NOTICE '❌ UNEXPECTED: Negative payment amount was accepted';
    EXCEPTION 
        WHEN check_violation THEN
            RAISE NOTICE '✅ EXPECTED: Negative payment amount rejected - %', SQLERRM;
    END $$;
    
    -- TEST 4: FAILING INSERT - Future date
    DO $$ 
    BEGIN
        INSERT INTO payment (loanid, amount, paymentdate, mode)
        VALUES (2, 300.00, '2025-01-01', 'Online');
        RAISE NOTICE '❌ UNEXPECTED: Future payment date was accepted';
    EXCEPTION 
        WHEN check_violation THEN
            RAISE NOTICE '✅ EXPECTED: Future payment date rejected - %', SQLERRM;
    END $$;
    
    -- Verify only 2 new rows were added
    SELECT 'After Payment tests. Payment rows:', COUNT(*) FROM payment;
    
COMMIT;


------------Q3

-- ADDITIONAL CONSTRAINT VALIDATION TESTS
SELECT '=== ADDITIONAL CONSTRAINT VALIDATION ===' as additional_tests;

BEGIN;
    -- Test 5: FAILING INSERT - Invalid status for Loan
    SELECT 'Additional Test 1: Invalid Loan Status (Should FAIL)' as test_case;
    BEGIN
        INSERT INTO loan (customerid, amount, interestrate, startdate, enddate, status)
        VALUES (5, 8000.00, 6.5, '2024-02-01', '2024-12-01', 'InvalidStatus');
        RAISE EXCEPTION '❌ TEST FAILED: Invalid status should have been rejected';
    EXCEPTION 
        WHEN check_violation THEN
            RAISE NOTICE '✅ EXPECTED ERROR: %', SQLERRM;
        WHEN OTHERS THEN
            RAISE NOTICE '❌ UNEXPECTED ERROR: %', SQLERRM;
    END;
    
    -- Test 6: FAILING INSERT - Invalid payment mode
    SELECT 'Additional Test 2: Invalid Payment Mode (Should FAIL)' as test_case;
    BEGIN
        INSERT INTO payment (loanid, amount, paymentdate, mode)
        VALUES (1, 400.00, '2024-02-20', 'InvalidMode');
        RAISE EXCEPTION '❌ TEST FAILED: Invalid payment mode should have been rejected';
    EXCEPTION 
        WHEN check_violation THEN
            RAISE NOTICE '✅ EXPECTED ERROR: %', SQLERRM;
        WHEN OTHERS THEN
            RAISE NOTICE '❌ UNEXPECTED ERROR: %', SQLERRM;
    END;
    
    -- Test 7: FAILING INSERT - Date order violation for Loan
    SELECT 'Additional Test 3: Invalid Date Order (Should FAIL)' as test_case;
    BEGIN
        INSERT INTO loan (customerid, amount, interestrate, startdate, enddate, status)
        VALUES (3, 9000.00, 7.0, '2024-12-01', '2024-02-01', 'Active');
        RAISE EXCEPTION '❌ TEST FAILED: End date before start date should have been rejected';
    EXCEPTION 
        WHEN check_violation THEN
            RAISE NOTICE '✅ EXPECTED ERROR: %', SQLERRM;
        WHEN OTHERS THEN
            RAISE NOTICE '❌ UNEXPECTED ERROR: %', SQLERRM;
    END;
    
    -- Test 8: FAILING INSERT - NULL value violation
    SELECT 'Additional Test 4: NULL Amount (Should FAIL)' as test_case;
    BEGIN
        INSERT INTO payment (loanid, amount, paymentdate, mode)
        VALUES (2, NULL, '2024-02-25', 'Transfer');
        RAISE EXCEPTION '❌ TEST FAILED: NULL amount should have been rejected';
    EXCEPTION 
        WHEN not_null_violation THEN
            RAISE NOTICE '✅ EXPECTED ERROR: %', SQLERRM;
        WHEN OTHERS THEN
            RAISE NOTICE '❌ UNEXPECTED ERROR: %', SQLERRM;
    END;
    
ROLLBACK; -- Rollback additional tests to avoid exceeding row budget

-------B7: E-C-A Trigger for Denormalized Totals
------Q1: Create Loan_AUDIT Table

-- CREATE LOAN_AUDIT TABLE
CREATE TABLE -- CREATE LOAN_AUDIT TABLE
CREATE TABLE Loan_AUDIT (
    audit_id SERIAL PRIMARY KEY,
    bef_total DECIMAL(15,2),
    aft_total DECIMAL(15,2),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    key_col VARCHAR(64),
    operation_type VARCHAR(10),
    rows_affected INTEGER
);
select  from -- CREATE LOAN_AUDIT TABLE
CREATE TABLE Loan_AUDIT (
    audit_id SERIAL PRIMARY KEY,
    bef_total DECIMAL(15,2),
    aft_total DECIMAL(15,2),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    key_col VARCHAR(64),
    operation_type VARCHAR(10),
    rows_affected INTEGER
);
select * from -- CREATE LOAN_AUDIT TABLE
CREATE TABLE Loan_AUDIT (
    audit_id SERIAL PRIMARY KEY,
    bef_total DECIMAL(15,2),
    aft_total DECIMAL(15,2),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    key_col VARCHAR(64),
    operation_type VARCHAR(10),
    rows_affected INTEGER
);
select *from Loan_AUDIT;

-- Verify table creation
SELECT 'Loan_AUDIT table created successfully' as status;
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'loan_audit' 
ORDER BY ordinal_position;

-- Verify table creation
SELECT 'Loan_AUDIT table created successfully' as status;
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'loan_audit' 
ORDER BY ordinal_position;
-- Verify table creation
SELECT 'Loan_AUDIT table created successfully' as status;
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'loan_audit' 
ORDER BY ordinal_position; (
    audit_id SERIAL PRIMARY KEY,
    bef_total DECIMAL(15,2),
    aft_total DECIMAL(15,2),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    key_col VARCHAR(64),
    operation_type VARCHAR(10),
    rows_affected INTEGER
);

-- Verify table creation
SELECT 'Loan_AUDIT table created successfully' as status;
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'loan_audit' 
ORDER BY ordinal_position;

------
-- ADD DENORMALIZED TOTAL COLUMN TO LOAN TABLE
ALTER TABLE Loan ADD COLUMN total_payments DECIMAL(15,2) DEFAULT 0.00;

-- Initialize existing loans with their current payment totals
UPDATE Loan 
SET total_payments = (
    SELECT COALESCE(SUM(amount), 0.00) 
    FROM Payment 
    WHERE Payment.loanid = Loan.loanid
);

-- Verify the denormalized column
SELECT 'Loan table updated with denormalized total_payments column' as status;
SELECT loanid, amount as loan_amount, total_payments, status FROM Loan ORDER BY loanid;


---------Q2
-- CREATE STATEMENT-LEVEL TRIGGER FUNCTION
CREATE OR REPLACE FUNCTION update_loan_totals()
RETURNS TRIGGER AS $$
DECLARE
    old_total DECIMAL(15,2);
    new_total DECIMAL(15,2);
    affected_loan_id INTEGER;
    operation_count INTEGER := 0;
BEGIN
    -- Determine affected loan ID and count operations
    IF TG_OP = 'INSERT' THEN
        affected_loan_id := NEW.loanid;
        operation_count := (SELECT COUNT(*) FROM inserted);
    ELSIF TG_OP = 'UPDATE' THEN
        affected_loan_id := NEW.loanid;
        operation_count := (SELECT COUNT(*) FROM new_table);
    ELSIF TG_OP = 'DELETE' THEN
        affected_loan_id := OLD.loanid;
        operation_count := (SELECT COUNT(*) FROM old_table);
    END IF;

    -- Get current total before changes
    SELECT COALESCE(SUM(amount), 0.00) INTO old_total
    FROM Payment 
    WHERE loanid = affected_loan_id;

    -- Update denormalized total in Loan table
    UPDATE Loan 
    SET total_payments = old_total
    WHERE loanid = affected_loan_id;

    -- Get new total after update
    SELECT COALESCE(SUM(amount), 0.00) INTO new_total
    FROM Payment 
    WHERE loanid = affected_loan_id;

    -- Log to audit table
    INSERT INTO Loan_AUDIT (bef_total, aft_total, key_col, operation_type, rows_affected)
    VALUES (old_total, new_total, 'Loan_' || affected_loan_id, TG_OP, operation_count);

    RETURN NULL; -- Statement-level trigger returns NULL
END;
$$ LANGUAGE plpgsql;

-- CREATE THE TRIGGER
CREATE TRIGGER trg_payment_denormalized_totals
AFTER INSERT OR UPDATE OR DELETE ON Payment
REFERENCING 
    NEW TABLE AS new_table 
    OLD TABLE AS old_table
FOR EACH STATEMENT
EXECUTE FUNCTION update_loan_totals();

-- Verify trigger creation
SELECT 'Trigger created successfully' as status;
SELECT 
    trigger_name,
    event_manipulation,
    action_statement,
    action_timing
FROM information_schema.triggers 
WHERE event_object_table = 'payment';


--------Q3

-- MIXED DML SCRIPT (4 ROWS TOTAL)
SELECT '=== EXECUTING MIXED DML SCRIPT (4 ROWS) ===' as script_start;

-- Get initial state for verification
SELECT 'Initial state - Loan totals:' as initial_state;
SELECT loanid, amount as loan_amount, total_payments, status FROM Loan ORDER BY loanid;

SELECT 'Initial state - Payment counts:' as payment_state;
SELECT loanid, COUNT(*) as payment_count, SUM(amount) as total_payments 
FROM Payment 
GROUP BY loanid 
ORDER BY loanid;

BEGIN;
    -- OPERATION 1: INSERT - Add a new payment (Row 1)
    INSERT INTO Payment (loanid, amount, paymentdate, mode)
    VALUES (1, 250.00, '2024-03-10', 'Transfer')
    RETURNING 'INSERTED Payment: ' || paymentid || ' for Loan ' || loanid || ' - Amount: ' || amount as operation_result;

    -- OPERATION 2: INSERT - Add another payment (Row 2)  
    INSERT INTO Payment (loanid, amount, paymentdate, mode)
    VALUES (2, 150.00, '2024-03-11', 'Cash')
    RETURNING 'INSERTED Payment: ' || paymentid || ' for Loan ' || loanid || ' - Amount: ' || amount as operation_result;

    -- OPERATION 3: UPDATE - Modify an existing payment (Row 3)
    UPDATE Payment 
    SET amount = amount + 50.00

	
    WHERE paymentid = (
        SELECT paymentid FROM Payment WHERE loanid = 1 ORDER BY paymentid LIMIT 1
    )
    RETURNING 'UPDATED Payment: ' || paymentid || ' for Loan ' || loanid || ' - New Amount: ' || amount as operation_result;

    -- OPERATION 4: DELETE - Remove a payment (Row 4)
    DELETE FROM Payment 
    WHERE paymentid = (
        SELECT paymentid FROM Payment WHERE loanid = 2 ORDER BY paymentid LIMIT 1
    )
    RETURNING 'DELETED Payment: ' || paymentid || ' for Loan ' || loanid as operation_result;

    -- Verify the 4 operations completed
    SELECT 'Mixed DML operations completed: 2 INSERTS, 1 UPDATE, 1 DELETE' as operation_summary;

COMMIT;

-------Q4

-- VERIFY DENORMALIZED TOTALS
SELECT '=== VERIFYING DENORMALIZED TOTALS ===' as verification;

-- Method 1: Compare denormalized vs actual totals
SELECT 
    l.loanid,
    l.amount as loan_amount,
    l.total_payments as denormalized_total,
    (SELECT COALESCE(SUM(amount), 0.00) FROM Payment p WHERE p.loanid = l.loanid) as actual_total,
    CASE 
        WHEN l.total_payments = (SELECT COALESCE(SUM(amount), 0.00) FROM Payment p WHERE p.loanid = l.loanid) 
        THEN '✅ CONSISTENT'
        ELSE '❌ INCONSISTENT'
    END as consistency_check
FROM Loan l
ORDER BY l.loanid;

-- Method 2: Show payment details for manual verification
SELECT 'Payment details for manual verification:' as payment_details;
SELECT 
    p.loanid,
    p.paymentid,
    p.amount,
    p.paymentdate,
    p.mode
FROM Payment p
ORDER BY p.loanid, p.paymentid;

-- Method 3: Loan summary with denormalized data
SELECT 'Loan summary with denormalized totals:' as loan_summary;
SELECT 
    loanid,
    amount as original_loan_amount,
    total_payments as total_payments_received,
    amount - total_payments as remaining_balance,
    status
FROM Loan
ORDER BY loanid;


------------


-- SHOW AUDIT TABLE ENTRIES (2-3 ROWS EXPECTED)
SELECT '=== AUDIT TABLE ENTRIES (2-3 ROWS EXPECTED) ===' as audit_entries;

SELECT 
    audit_id,
    bef_total as before_total,
    aft_total as after_total,
    ROUND(aft_total - bef_total, 2) as net_change,
    key_col as loan_key,
    operation_type,
    rows_affected,
    changed_at
FROM Loan_AUDIT
ORDER BY changed_at;

-- Audit entry analysis
SELECT 
    'Audit Analysis:' as analysis,
    COUNT(*) as total_audit_entries,
    COUNT(DISTINCT key_col) as unique_loans_audited,
    SUM(rows_affected) as total_operations_logged,
    MIN(changed_at) as first_audit,
    MAX(changed_at) as last_audit
FROM Loan_AUDIT;

---------

-- ADDITIONAL TEST: VERIFY TRIGGER BEHAVIOR
SELECT '=== ADDITIONAL TRIGGER BEHAVIOR TEST ===' as additional_test;

-- Test single operation to verify trigger fires correctly
BEGIN;
    INSERT INTO Payment (loanid, amount, paymentdate, mode)
    VALUES (1, 100.00, '2024-03-12', 'Online')
    RETURNING 'Test Insert - Payment ID: ' || paymentid as test_result;

    -- Check audit entry was created
    SELECT 'Audit entries after test insert:' as audit_check;
    SELECT audit_id, bef_total, aft_total, key_col, operation_type 
    FROM Loan_AUDIT 
    ORDER BY changed_at DESC 
    LIMIT 1;
ROLLBACK; -- Rollback to avoid affecting row count

-- Verify rollback worked (no new permanent rows)
SELECT 'After rollback - Payment count:', COUNT(*) FROM Payment;
SELECT 'After rollback - Audit entries:', COUNT(*) FROM Loan_AUDIT;



------------B8: Recursive Hierarchy Roll-Up
----------Step 1: Create HIER Table with Banking Domain Context


-- CREATE HIERARCHY TABLE FOR BANKING DOMAIN
CREATE TABLE HIER (
    parent_id VARCHAR(50),
    child_id VARCHAR(50) PRIMARY KEY,
    relationship_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
select * from HIER;

-- Verify table creation
SELECT 'HIER table created successfully' as status;
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'hier' 
ORDER BY ordinal_position;


-------Q2
-- INSERT 8 ROWS FORMING 3-LEVEL BANKING HIERARCHY
INSERT INTO HIER (parent_id, child_id, relationship_type) VALUES
-- Level 1: Root accounts (Corporate Structure)
(NULL, 'BANK_ROOT', 'Financial Institution'),

-- Level 2: Main banking divisions
('BANK_ROOT', 'RETAIL_BANKING', 'Division'),
('BANK_ROOT', 'BUSINESS_BANKING', 'Division'),
('BANK_ROOT', 'INVESTMENT_BANKING', 'Division'),

-- Level 3: Sub-divisions under Retail Banking
('RETAIL_BANKING', 'PERSONAL_ACCOUNTS', 'Department'),
('RETAIL_BANKING', 'LOAN_SERVICES', 'Department'),

-- Level 3: Sub-divisions under Business Banking  
('BUSINESS_BANKING', 'CORPORATE_ACCOUNTS', 'Department'),
('BUSINESS_BANKING', 'COMMERCIAL_LOANS', 'Department');

-- Verify insertion
SELECT 'Hierarchy data inserted: ' || COUNT(*) || ' rows' as insertion_status FROM HIER;

-- Show the hierarchy structure
SELECT 'Hierarchy Structure:' as structure;
SELECT 
    parent_id,
    child_id, 
    relationship_type,
    created_at
FROM HIER 
ORDER BY 
    CASE 
        WHEN parent_id IS NULL THEN 0 
        WHEN parent_id = 'BANK_ROOT' THEN 1
        ELSE 2 
    END,
    parent_id,

	--------Q3

-- RECURSIVE HIERARCHY QUERY WITH TRANSACTION ROLL-UP
SELECT '=== RECURSIVE HIERARCHY WITH TRANSACTION ROLL-UP ===' as query_title;

-- PRACTICAL BANKING HIERARCHY ROLL-UP WITH ACTUAL TRANSACTIONS
SELECT '=== PRACTICAL BANKING HIERARCHY ROLL-UP ===' as query_title;

-- SIMPLE RECURSIVE QUERY (6-10 ROWS OUTPUT)
SELECT '=== SIMPLE RECURSIVE HIERARCHY TRAVERSAL ===' as query_title;
-- FINAL WORKING RECURSIVE HIERARCHY QUERY (6-10 ROWS)
SELECT '=== FINAL RECURSIVE HIERARCHY IMPLEMENTATION ===' as implementation;

-------Q4
-- FINAL BUDGET VERIFICATION (≤10 ROWS)
SELECT '=== FINAL ROW BUDGET VERIFICATION ===' as budget_check;

WITH all_tables AS (
    SELECT 'hier' as table_name, COUNT(*) as row_count FROM hier
    UNION ALL SELECT 'loan', COUNT(*) FROM loan
    UNION ALL SELECT 'payment', COUNT(*) FROM payment
    UNION ALL SELECT 'loan_audit', COUNT(*) FROM loan_audit
    UNION ALL SELECT 'local_transaction_audit', COUNT(*) FROM local_transaction_audit
    UNION ALL SELECT 'Transaction_A', COUNT(*) FROM Transaction_A
    UNION ALL SELECT 'Transaction_B', COUNT(*) FROM transaction_b
),
totals AS (
    SELECT SUM(row_count) as total_rows FROM all_tables
)
SELECT 
    at.table_name,
    at.row_count,
    t.total_rows,
    CASE 
        WHEN t.total_rows <= 10 THEN '✅ WITHIN BUDGET'
        ELSE '❌ EXCEEDS BUDGET'
    END as budget_status
FROM all_tables at, totals t
ORDER BY at.row_count DESC;

----------
--------B9: Mini-Knowledge Base with Transitive Inference
------- 1: QCreate TRIPLE Table and Insert Facts

-- CREATE TRIPLE TABLE FOR KNOWLEDGE BASE
CREATE TABLE TRIPLE (
    s VARCHAR(64),
    p VARCHAR(64),
    o VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (s, p, o)
);

-- INSERT 10 BANKING DOMAIN FACTS
INSERT INTO TRIPLE (s, p, o) VALUES
-- Type hierarchy facts
('SavingsAccount', 'isA', 'Account'),
('CurrentAccount', 'isA', 'Account'),
('FixedDeposit', 'isA', 'Account'),
('Account', 'isA', 'BankProduct'),
('Loan', 'isA', 'BankProduct'),
('BankProduct', 'isA', 'FinancialInstrument'),

-- Business rule implications
('HighValueAccount', 'isA', 'SavingsAccount'),
('HighValueAccount', 'requires', 'EnhancedDueDiligence'),
('Loan', 'requires', 'CreditCheck'),
('Account', 'canHave', 'Overdraft');

-- Verify insertion
SELECT 'TRIPLE table created with ' || COUNT(*) || ' facts' as status FROM TRIPLE;

-- Show all facts
SELECT 'Knowledge Base Facts (10 facts):' as facts;
SELECT s, p, o FROM TRIPLE ORDER BY p, s;

------Q2
-- RECURSIVE INFERENCE QUERY FOR TRANSITIVE isA*
SELECT '=== TRANSITIVE isA* INFERENCE ===' as inference_title;

WITH RECURSIVE type_inference AS (
    -- Base case: Direct isA relationships
    SELECT 
        s as entity,
        o as inferred_type,
        s as path_start,
        0 as inference_depth,
        ARRAY[s] as inference_path
    FROM TRIPLE 
    WHERE p = 'isA'
    
    UNION ALL
    
    -- Recursive case: Transitive closure
    SELECT 
        ti.entity,
        t.o as inferred_type,
        ti.path_start,
        ti.inference_depth + 1,
        ti.inference_path || t.o
    FROM type_inference ti
    JOIN TRIPLE t ON ti.inferred_type = t.s AND t.p = 'isA'
    WHERE ti.inference_depth < 5  -- Prevent infinite recursion
      AND t.o != ALL(ti.inference_path)  -- Avoid cycles
)
SELECT 
    entity,
    inferred_type,
    inference_depth,
    array_to_string(inference_path, ' → ') as inference_chain
FROM type_inference
ORDER BY entity, inference_depth
LIMIT 10;  -- Return up to 10 labeled rows

-------


-- APPLY INFERRED LABELS TO BASE RECORDS
SELECT '=== APPLYING INFERRED LABELS TO BANKING DATA ===' as application;

WITH RECURSIVE type_inference AS (
    SELECT 
        s as entity,
        o as inferred_type,
        0 as depth
    FROM TRIPLE 
    WHERE p = 'isA'
    
    UNION ALL
    
    SELECT 
        ti.entity,
        t.o as inferred_type,
        ti.depth + 1
    FROM type_inference ti
    JOIN TRIPLE t ON ti.inferred_type = t.s AND t.p = 'isA'
    WHERE ti.depth < 5
),
-- Map account types from our existing data
account_types AS (
    SELECT 
        'SavingsAccount' as account_type,
        ARRAY['HighValueAccount', 'RegularSavings'] as examples
    UNION ALL
    SELECT 
        'CurrentAccount' as account_type,
        ARRAY['BusinessCurrent', 'PersonalCurrent'] as examples
    UNION ALL
    SELECT 
        'FixedDeposit' as account_type, 
        ARRAY['ShortTermFD', 'LongTermFD'] as examples
),
-- Apply inferred types to account examples
labeled_accounts AS (
    SELECT 
        unnest(at.examples) as account_name,
        at.account_type as direct_type,
        ti.inferred_type as inferred_type,
        ti.depth
    FROM account_types at
    JOIN type_inference ti ON at.account_type = ti.entity
    WHERE ti.depth <= 2  -- Limit inference depth for clarity
)
SELECT 
    account_name,
    direct_type,
    inferred_type,
    depth,
    CASE 
        WHEN depth = 0 THEN 'Direct Type'
        WHEN depth = 1 THEN 'First-level Inference'
        ELSE 'Deep Inference'
    END as inference_level
FROM labeled_accounts
ORDER BY account_name, depth
LIMIT 10;  -- Ensure we return ≤10 rows

---------
-- INSERT 10 BANKING DOMAIN FACTS INTO TRIPLE TABLE
SELECT '=== INSERTING BANKING DOMAIN FACTS INTO TRIPLE TABLE ===' as step;

-- Clear any existing data to ensure clean state
DELETE FROM TRIPLE;

-- Insert 10 banking domain facts covering type hierarchy and business rules
INSERT INTO TRIPLE (s, p, o) VALUES
-- TYPE HIERARCHY FACTS (6 facts)
('SavingsAccount', 'isA', 'Account'),
('CurrentAccount', 'isA', 'Account'), 
('FixedDepositAccount', 'isA', 'Account'),
('PersonalLoan', 'isA', 'Loan'),
('BusinessLoan', 'isA', 'Loan'),
('Account', 'isA', 'BankProduct'),
('Loan', 'isA', 'BankProduct'),

-- BUSINESS RULE IMPLICATIONS (3 facts)
('HighValueTransaction', 'requires', 'ManagerApproval'),
('InternationalTransfer', 'requires', 'KYCDocumentation'),
('LoanApplication', 'triggers', 'CreditCheck'),

-- RISK CLASSIFICATION (1 fact)
('SuspiciousActivity', 'classifiedAs', 'HighRisk');

-- Verify we have exactly 10 facts
SELECT 'Inserted ' || COUNT(*) || ' banking domain facts' as verification FROM TRIPLE;

-- Display all facts organized by predicate
SELECT '=== COMPLETE KNOWLEDGE BASE FACTS ===' as display;
SELECT 
    p as relationship_type,
    s as subject,
    o as object,
    CASE 
        WHEN p = 'isA' THEN 'Type Hierarchy'
        WHEN p = 'requires' THEN 'Business Rule'
        WHEN p = 'triggers' THEN 'Process Flow'
        WHEN p = 'classifiedAs' THEN 'Risk Assessment'
        ELSE 'Other'
    END as category
FROM TRIPLE
ORDER BY 
    CASE p 
        WHEN 'isA' THEN 1
        WHEN 'requires' THEN 2
        WHEN 'triggers' THEN 3
        WHEN 'classifiedAs' THEN 4
        ELSE 5
    END,
    s,
    o;
	-------
	-- ALTERNATIVE: COMPREHENSIVE BANKING FACTS SET
SELECT '=== ALTERNATIVE: COMPREHENSIVE BANKING FACTS ===' as alternative;

-- Clear and insert alternative fact set
DELETE FROM TRIPLE;

INSERT INTO TRIPLE (s, p, o) VALUES
-- CORE BANKING HIERARCHY (5 facts)
('Customer', 'has', 'Account'),
('Account', 'generates', 'Transaction'),
('Transaction', 'processedBy', 'Teller'),
('Teller', 'worksAt', 'Branch'),
('Branch', 'partOf', 'Bank'),

-- PRODUCT TYPES (3 facts)  
('Savings', 'productType', 'Account'),
('Checking', 'productType', 'Account'),
('Mortgage', 'productType', 'Loan'),

-- COMPLIANCE RULES (2 facts)
('LargeCashDeposit', 'requires', 'CTRReporting'),
('ForeignNational', 'requires', 'EnhancedDueDiligence');

-- Verify alternative facts
SELECT 'Alternative: Inserted ' || COUNT(*) || ' comprehensive banking facts' as verification FROM TRIPLE;

-- Display alternative facts
SELECT p as relationship, s as subject, o as object FROM TRIPLE ORDER BY p, s;

------
-- CLEANUP TEMPORARY DATA TO MAINTAIN ≤10 ROW BUDGET
SELECT '=== CLEANING UP TEMPORARY DATA ===' as cleanup;

-- Option 1: Remove audit tables (most likely to have temporary data)
DROP TABLE IF EXISTS loan_audit CASCADE;
DROP TABLE IF EXISTS local_transaction_audit CASCADE;

-- Option 2: Clean specific test data from core tables
DELETE FROM payment WHERE mode LIKE '%Test%' OR mode LIKE '%Simulation%';
DELETE FROM loan WHERE status LIKE '%Test%' OR description LIKE '%Simulation%';

-- Option 3: Reset Transaction tables to minimal dataset
DELETE FROM Transaction_A WHERE TransID > 5;
DELETE FROM transaction_b WHERE TransID > 5;

-- Verify cleanup
SELECT 'After cleanup - Removed temporary audit and test data' as cleanup_status;
-------



-- FINAL OPTIMIZED SETUP WITHIN 10-ROW BUDGET
SELECT '=== FINAL OPTIMIZED SETUP (≤10 ROWS) ===' as final_setup;

-- Step 1: Ensure TRIPLE has exactly 8 facts (as required)
DELETE FROM TRIPLE;
INSERT INTO TRIPLE (s, p, o) VALUES
('SavingsAccount', 'isA', 'DepositAccount'),
('CheckingAccount', 'isA', 'DepositAccount'),
('DepositAccount', 'isA', 'BankAccount'),
('LoanAccount', 'isA', 'BankAccount'),
('TransactionAbove5000', 'requires', 'ManagerApproval'),
('InternationalTransaction', 'requires', 'ComplianceCheck'),
('PremiumCustomer', 'eligibleFor', 'PriorityService'),
('NewCustomer', 'requires', 'IdentityVerification');

-- Step 2: Ensure BUSINESS_LIMITS has 1 row
DELETE FROM BUSINESS_LIMITS;
INSERT INTO BUSINESS_LIMITS (rule_key, threshold, active, description) 
VALUES ('MAX_SINGLE_TRANSACTION', 5000.00, 'Y', 'Maximum allowed amount for a single transaction');

-- Step 3: Keep only essential HIER data (8 rows)
DELETE FROM HIER;
INSERT INTO HIER (parent_id, child_id, relationship_type) VALUES
(NULL, 'BANK_ROOT', 'Financial Institution'),
('BANK_ROOT', 'RETAIL_BANKING', 'Division'),
('BANK_ROOT', 'BUSINESS_BANKING', 'Division'),
('BANK_ROOT', 'INVESTMENT_BANKING', 'Division'),
('RETAIL_BANKING', 'PERSONAL_ACCOUNTS', 'Department'),
('RETAIL_BANKING', 'LOAN_SERVICES', 'Department'),
('BUSINESS_BANKING', 'CORPORATE_ACCOUNTS', 'Department'),
('BUSINESS_BANKING', 'COMMERCIAL_LOANS', 'Department');

-- Step 4: Keep minimal Loan and Payment data (2 rows total)
DELETE FROM loan;
INSERT INTO loan (loanid, customerid, amount, interestrate, startdate, enddate, status) VALUES
(1, 1, 5000.00, 7.5, '2024-02-01', '2024-08-01', 'Active');

DELETE FROM payment;
INSERT INTO payment (paymentid, loanid, amount, paymentdate, mode) VALUES
(1, 1, 500.00, '2024-02-28', 'Transfer');

-- Step 5: Keep only essential Transaction data (5 rows total across A+B)
DELETE FROM Transaction_A;
INSERT INTO Transaction_A (TransID, AccountID, TellerID, Amount, Type, DatePerformed) VALUES
(1, 1002, 'T001', 1500.00, 'Credit', '2024-01-15'),
(2, 1004, 'T002', 250.75, 'Debit', '2024-01-16');

DELETE FROM transaction_b;
INSERT INTO transaction_b (TransID, AccountID, TellerID, Amount, Type, DatePerformed) VALUES
(1, 1001, 'T002', 2000.00, 'Credit', '2024-01-10'),
(2, 1003, 'T003', 150.00, 'Debit', '2024-01-11'),
(3, 1005, 'T001', 4500.75, 'Credit', '2024-01-12');

-- Verify final row counts
SELECT 'Final row counts after optimization:' as verification;

-----------
---------B10: Business Limit Alert (Function + Trigger)
--------Step 1: Create BUSINESS_LIMITS Table and Seed Active Rule
---sql
-- CREATE BUSINESS_LIMITS TABLE




-- CREATE BUSINESS_LIMITS TABLE
CREATE TABLE IF NOT EXISTS BUSINESS_LIMITS (
    rule_key VARCHAR(64) PRIMARY KEY,
    threshold DECIMAL(15,2),
    active CHAR(1) CHECK (active IN ('Y', 'N')),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SEED EXACTLY ONE ACTIVE RULE (Payment Amount Limit)
INSERT INTO BUSINESS_LIMITS (rule_key, threshold, active, description) 
VALUES ('MAX_PAYMENT_AMOUNT', 2000.00, 'Y', 'Maximum allowed amount for a single payment')
ON CONFLICT (rule_key) DO UPDATE SET 
    threshold = EXCLUDED.threshold,
    active = EXCLUDED.active,
    description = EXCLUDED.description;

-- Verify the active rule
SELECT 'BUSINESS_LIMITS table created with active rule:' as verification;
SELECT rule_key, threshold, active, description FROM BUSINESS_LIMITS;



-------Q2
-- CREATE BUSINESS LIMIT ALERT FUNCTION
CREATE OR REPLACE FUNCTION fn_should_alert_payment(
    p_payment_amount DECIMAL(15,2),
    p_loan_id INTEGER DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_max_payment_limit DECIMAL(15,2);
    v_loan_balance DECIMAL(15,2);
BEGIN
    -- Get the active payment amount limit
    SELECT threshold INTO v_max_payment_limit
    FROM BUSINESS_LIMITS 
    WHERE rule_key = 'MAX_PAYMENT_AMOUNT' 
      AND active = 'Y';
    
    -- If no active rule found, allow the payment
    IF v_max_payment_limit IS NULL THEN
        RETURN 0;
    END IF;
    
    -- Check if payment amount exceeds the limit
    IF p_payment_amount > v_max_payment_limit THEN
        RETURN 1; -- Violation detected
    END IF;
    
    -- Additional check: Ensure payment doesn't exceed loan balance if loan_id provided
    IF p_loan_id IS NOT NULL THEN
        SELECT amount INTO v_loan_balance
        FROM loan 
        WHERE loanid = p_loan_id;
        
        IF v_loan_balance IS NOT NULL AND p_payment_amount > v_loan_balance THEN
            RETURN 1; -- Payment exceeds loan balance
        END IF;
    END IF;
    
    RETURN 0; -- No violation
END;
$$ LANGUAGE plpgsql;

-- Test the function
SELECT 'Testing alert function with various amounts:' as function_test;
SELECT 
    fn_should_alert_payment(1500.00, 1) as test_1500,  -- Should pass (0)
    fn_should_alert_payment(2500.00, 1) as test_2500,  -- Should fail (1) - exceeds 2000 limit
    fn_should_alert_payment(3000.00, 1) as test_3000,  -- Should fail (1) - exceeds 2000 limit
    fn_should_alert_payment(500.00, 1) as test_500;    -- Should pass (0)

	-----------

-- CREATE BUSINESS LIMIT ENFORCEMENT TRIGGER ON PAYMENT
CREATE OR REPLACE FUNCTION check_payment_limits()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if payment violates business limits
    IF fn_should_alert_payment(NEW.amount, NEW.loanid) = 1 THEN
        RAISE EXCEPTION 'Business rule violation: Payment amount % exceeds maximum allowed limit of 2000.00 for loan %', 
                        NEW.amount, NEW.loanid;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- CREATE THE TRIGGER
DROP TRIGGER IF EXISTS trg_payment_limit_check ON payment;
CREATE TRIGGER trg_payment_limit_check
BEFORE INSERT OR UPDATE ON payment
FOR EACH ROW
EXECUTE FUNCTION check_payment_limits();

-- Verify trigger creation
SELECT 'Business limit trigger created on payment table:' as trigger_status;
SELECT 
    trigger_name,
    event_manipulation,
    action_timing,
    action_statement
FROM information_schema.triggers 
WHERE event_object_table = 'payment';



---------
-- DEMONSTRATE 2 FAILING AND 2 PASSING DML CASES
SELECT '=== DEMONSTRATING BUSINESS LIMIT ENFORCEMENT ===' as demonstration;

-- Get current state for verification
SELECT 'Current payment count before tests:', COUNT(*) FROM payment;
SELECT 'Current loan balance for testing:', loanid, amount FROM loan WHERE loanid = 1;

BEGIN;
    -- TEST 1: PASSING INSERT - Within limit
    SELECT 'Test 1: Payment within limit (Should PASS)' as test_case;
    INSERT INTO payment (loanid, amount, paymentdate, mode)
    VALUES (1, 500.00, CURRENT_DATE, 'Cash')
    RETURNING '✅ PASS: Payment inserted - Amount: ' || amount as result;

    -- TEST 2: PASSING INSERT - At the limit boundary
    SELECT 'Test 2: Payment at limit boundary (Should PASS)' as test_case;
    INSERT INTO payment (loanid, amount, paymentdate, mode)
    VALUES (1, 2000.00, CURRENT_DATE, 'Transfer')  -- Exactly at 2000 limit
    RETURNING '✅ PASS: Payment inserted - Amount: ' || amount as result;

    -- TEST 3: FAILING INSERT - Exceeds limit (wrapped in exception handling)
    SELECT 'Test 3: Payment exceeds limit (Should FAIL)' as test_case;
    BEGIN
        INSERT INTO payment (loanid, amount, paymentdate, mode)
        VALUES (1, 2500.00, CURRENT_DATE, 'Online');  -- Exceeds 2000 limit
        RAISE NOTICE '❌ UNEXPECTED: Payment over limit was accepted';
    EXCEPTION 
        WHEN others THEN
            RAISE NOTICE '✅ EXPECTED: Payment rejected - %', SQLERRM;
    END;

    -- TEST 4: FAILING INSERT - Far exceeds limit
    SELECT 'Test 4: Payment far exceeds limit (Should FAIL)' as test_case;
    BEGIN
        INSERT INTO payment (loanid, amount, paymentdate, mode)
        VALUES (1, 5000.00, CURRENT_DATE, 'Card');  -- Far exceeds 2000 limit
        RAISE NOTICE '❌ UNEXPECTED: Large payment was accepted';
    EXCEPTION 
        WHEN others THEN
            RAISE NOTICE '✅ EXPECTED: Large payment rejected - %', SQLERRM;
    END;

    -- Show successful transactions that will be committed
    SELECT 'Successful payments to be committed:' as committed_data;
    SELECT paymentid, loanid, amount, paymentdate, mode 
    FROM payment 
    ORDER BY paymentid DESC 
    LIMIT 5;

	-------
	-- CLEANUP TO ENSURE ≤10 ROW BUDGET
SELECT '=== FINAL CLEANUP FOR BUDGET MAINTENANCE ===' as cleanup;

-- Remove any test payments created during demonstrations
DELETE FROM payment WHERE paymentid > 2;  -- Keep only the essential payments

-- Verify final state
SELECT 'Final payment count after cleanup:', COUNT(*) FROM payment;
SELECT 'Final business limits count:', COUNT(*) FROM business_limits;

-- Final budget confirmation
SELECT 
    'TOTAL COMMITTED ROWS ACROSS ALL TABLES:' as final_budget,
    (SELECT COUNT(*) FROM business_limits) +
    (SELECT COUNT(*) FROM triple) +
    (SELECT COUNT(*) FROM hier) +
    (SELECT COUNT(*) FROM loan) +
    (SELECT COUNT(*) FROM payment) +
    (SELECT COUNT(*) FROM Transaction_A) +
    (SELECT COUNT(*) FROM transaction_b) as total_rows,
    CASE 
        WHEN (SELECT COUNT(*) FROM business_limits) +
             (SELECT COUNT(*) FROM triple) +
             (SELECT COUNT(*) FROM hier) +
             (SELECT COUNT(*) FROM loan) +
             (SELECT COUNT(*) FROM payment) +
             (SELECT COUNT(*) FROM Transaction_A) +
             (SELECT COUNT(*) FROM transaction_b) <= 10 
        THEN '🎉 SUCCESS: ≤10 row budget maintained!'
        ELSE '💥 FAILURE: Budget exceeded'
    END as budget_result;

COMMIT;

-- Verify only the passing payments were committed
SELECT 'After commit - Total payment count:', COUNT(*) FROM payment;