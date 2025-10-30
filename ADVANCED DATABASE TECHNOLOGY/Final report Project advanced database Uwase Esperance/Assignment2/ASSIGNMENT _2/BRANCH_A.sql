-- DDL for Customer
CREATE TABLE Customer(
CustomerID varchar(255) PRIMARY KEY,
firtName varchar(255) NOT NULL,
LastName Varchar(255) NOT NULL,
Email Varchar(255) UNIQUE ,
Phone Varchar(255) NOT NULL,
City Varchar(255) NOT NULL
);
select * from Customer ;


-- DDL for Account
CREATE TABLE Account(
AccountID INT PRIMARY KEY,
CustomerID Varchar (255) NOT NULL,
AccountType Varchar(255) NOT NULL,
Balance Decimal(10,2) NOT NULL,
DateOpened Date NOT NULL,
Status Varchar(255)NOT NULL
);
select * from Account ;

-- DDL for Teller
CREATE TABLE Teller(
TellerID Varchar(255) PRIMARY KEY,
FirstName Varchar (255) NOT NULL,
LastName Varchar(255) NOT NULL,
Branch Varchar (255) NOT NULL,
Contract Varchar(255) NOT NULL
);
select *from teller;
----- task 2


 CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Create a foreign server (This defines the connection to FleetOperations)

CREATE SERVER brancha_link
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'localhost',       -- host where  PostgreSQL18 is running
    dbname 'BankingtransationB',  -- remote db to connect to
    port '5432'
);


-- create a user mapping(Map a local user in FleetSupport node  to a user in FleetOperations node)
CREATE USER MAPPING FOR postgres  -- or your local user
SERVER brancha_link
OPTIONS (
    user 'postgres',         -- FleetOperations username
    password '12345'       -- FleetOperations password
);

-- import import  foreign tables from FleetOperations

IMPORT FOREIGN SCHEMA public
LIMIT TO (Transactions, Loan, Payment, LoanRepayments)
FROM SERVER brancha_link INTO public;

INSERT INTO Customer (CustomerID, firtName, LastName, Email, Phone, City)
VALUES
('C001', 'Alice', 'Mukamana', 'alice.mukamana@example.com', '+250788111111', 'Kigali'),
('C002', 'John', 'Habimana', 'john.habimana@example.com', '+250788222222', 'Musanze'),
('C003', 'Grace', 'Uwizeye', 'grace.uwizeye@example.com', '+250788333333', 'Huye'),
('C004', 'David', 'Nshimiyimana', 'david.nshimiyimana@example.com', '+250788444444', 'Rubavu'),
('C005', 'Sarah', 'Uwimana', 'sarah.uwimana@example.com', '+250788555555', 'Rwamagana'),
('C006', 'Eric', 'Mugisha', 'eric.mugisha@example.com', '+250788666666', 'Kayonza');
  
select * from Customer ;
INSERT INTO Account (AccountID, CustomerID, AccountType, Balance, DateOpened, Status)
VALUES
(1001, 'C001', 'Savings', 2500.00, '2024-03-15', 'Active'),
(1002, 'C002', 'Current', 5200.50, '2024-04-10', 'Active'),
(1003, 'C003', 'Fixed Deposit', 10000.00, '2024-05-05', 'Inactive'),
(1004, 'C004', 'Savings', 750.75, '2024-06-12', 'Active'),
(1005, 'C005', 'Business', 8300.00, '2024-07-20', 'Active');

INSERT INTO Teller (TellerID, FirstName, LastName, Branch, Contract)
VALUES
('T001', 'Patrick', 'Niyonzima', 'Kigali Main', 'Full-Time'),
('T002', 'Claudine', 'Uwimana', 'Musanze Branch', 'Part-Time'),
('T003', 'Innocent', 'Habyarimana', 'Rubavu Branch', 'Full-Time'),
('T004', 'Diane', 'Mukeshimana', 'Huye Branch', 'Contract'),
('T005', 'Emmanuel', 'Murenzi', 'Rwamagana Branch', 'Full-Time'),
('T006', 'Alice', 'Uwase', 'Kayonza Branch', 'Part-Time');




SELECT c.firtName, SUM(l.amount) FROM Customer c
INNER JOIN Account ac ON ac.customerid = c.customerid
INNER JOIN Loan l ON l.accountid::INTEGER = ac.accountid
GROUP BY 1

------Task 3 Parallel Query database links

SET max_parallel_workers_per_gather = 0;
SET max_parallel_workers_per_gather = 2;
SET max_parallel_workers_per_gather = 4;   -- Default is 2
SET parallel_setup_cost = 0;               -- Reduce threshold for using parallel
SET parallel_tuple_cost = 0;               -- Encourage parallel plans
SET min_parallel_table_scan_size = '8MB';
SET min_parallel_index_scan_size = '8MB';

SET parallel_setup_cost = 0;
SET max_parallel_workers_per_gather = 8;

EXPLAIN (ANALYZE, BUFFERS)
SELECT SUM(amount)
FROM transactions;
-- ===============================================
-- Compare Serial vs Parallel Query Performance
-- Table example: transactions
-- ===============================================

-- ------------------------------
-- 1️⃣ Serial Execution
-- Disable parallelism
SET max_parallel_workers_per_gather = 0;

-- Optional: reset planner costs to default
RESET parallel_setup_cost;
RESET parallel_tuple_cost;

-- Run EXPLAIN ANALYZE on serial query
EXPLAIN (ANALYZE, BUFFERS)
SELECT SUM(amount) AS total_amount
FROM transactions;


-- ------------------------------
-- 2️⃣ Parallel Execution
-- Enable parallelism
SET max_parallel_workers_per_gather = 8;  -- number of parallel workers

-- Optional: encourage planner to choose parallel scan
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0.1;

-- Run EXPLAIN ANALYZE on parallel query
EXPLAIN (ANALYZE, BUFFERS)
SELECT SUM(amount) AS total_amount
FROM transactions;


-- ------------------------------
-- Notes:
-- 1. Check the "Execution Time" in both EXPLAIN outputs
-- 2. Parallel plan shows "Gather" node with "Workers Planned"
-- 3. Serial plan shows simple "Seq Scan"



-------- TASK 4: Write a PL/SQL block performing inserts on both nodes and committing once

-- In this section we are going to simulate two phase commit 
-- inserts data on both nodes and committing once. Verify atomicity
-- let create a PL block that create a shipments and then report its corresponding payment
-- the whole operation is atomic which mean the operation will be full completed or not compelete at all in case anything goes wrong


CREATE TABLE branch_a_transactions (
  txn_id NUMBER PRIMARY KEY,
  description VARCHAR2(50)
);
CREATE TABLE branch_b_transactions (
  txn_id NUMBER PRIMARY KEY,
  description VARCHAR2(50)
);
SET SERVEROUTPUT ON;

BEGIN
  -- Insert into local table (BranchDB_A)
  INSERT INTO branch_a_transactions VALUES (101, 'Deposit from A');

  -- Insert into remote table (BranchDB_B) using DB Link
  INSERT INTO branch_b_transactions@branchdb_b_link VALUES (201, 'Deposit from A to B');

  -- Commit once (Oracle internally uses 2PC here)
  COMMIT;
  
  DBMS_OUTPUT.PUT_LINE('Distributed transaction committed successfully.');
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('Transaction failed and rolled back.');
END;
/

-- TASK 5: Simulate a remote failure during a distributed transaction. Check unresolved transactions and resolve them using ROLLBACK FORCE

-- 5.1 Simulate a remote failure during a distributed transaction
-- transaction in postgres should be either commited or rolled back automatically
-- to allow manually commit/rollback of transaction which must prepared transaction
-- by default prepared transaction are disable in postgers, therefore to enable this
-- functionality we are required to change max_prepared_transactions config varibale to a value >0 and then restart the server
-- confirm change has reflected by running : SHOW max_prepared_transactions;
-- prepared statement keep transactions in prepared state for manual resolution


-- 5. 1 remote failure is being simulated by inserting 
-- into wrong table from local node(invalid_payment) 
 
 CREATE DATABASE LINK branchdb_b_link
CONNECT TO branch_b IDENTIFIED BY pa$$B
USING 'BRANCHDB_B';

CREATE TABLE branch_a_transactions (
  txn_id NUMBER PRIMARY KEY,
  description VARCHAR2(100)
);

CREATE TABLE branch_b_transactions (
  txn_id NUMBER PRIMARY KEY,
  description VARCHAR2(100)
);

SET SERVEROUTPUT ON;

BEGIN
  -- Start distributed transaction
  INSERT INTO branch_a_transactions VALUES (301, 'Test distributed failure - Local');
  INSERT INTO branch_b_transactions@branchdb_b_link VALUES (401, 'Test distributed failure - Remote');

  -- Simulate failure manually: do NOT commit yet.
  DBMS_OUTPUT.PUT_LINE('Before commit - now simulate failure (disconnect remote node)');
  
  -- (At this point, disconnect or stop the remote BranchDB_B service)
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Commit failed due to network issue: ' || SQLERRM);
END;
/ 

------6 tast 


[02:23, 10/27/2025] Esperance: -- Suppose we have this account
INSERT INTO Account(AccountID, CustomerID, AccountType, Balance, DateOpened, Status)
VALUES (1, 101, 'Savings', 1000, CURRENT_DATE, 'Active');
[02:23, 10/27/2025] Esperance: BEGIN;
UPDATE Account
SET Balance = Balance + 500
WHERE AccountID = 1;
-- Keep the transaction open, do not commit yet
[02:24, 10/27/2025] Esperance: BEGIN;
UPDATE Account
SET Balance = Balance - 200
WHERE AccountID = 1;
-- This will block until Session 1 commits or rolls back
SELECT pid,
       locktype,
       relation::regclass AS table_name,
       mode,
       granted
FROM pg_locks
JOIN pg_class ON pg_locks.relation = pg_class.oid;

----- tast 7

-- Enable parallelism
SET max_parallel_workers_per_gather = 4;

-- Create a large dataset for simulation
CREATE TABLE Transaction_Large AS
SELECT
    generate_series(1, 1000000) AS TransID,
    (random()*1000)::int AS AccountID,
    (random()*10)::int AS TellerID,
    random()*10000 AS Amount,
    CASE WHEN random() > 0.5 THEN 'Credit' ELSE 'Debit' END AS Type,
    CURRENT_DATE - (random()*365)::int AS DatePerformed;

-- Parallel aggregation
EXPLAIN (ANALYZE, BUFFERS)
SELECT AccountID, SUM(Amount)
FROM Transaction_Large
GROUP BY AccountID;

---- tast 8

CREATE TABLE architecture_notes (
    id SERIAL PRIMARY KEY,
    description TEXT
);

INSERT INTO architecture_notes (description) VALUES (
'[Presentation Layer] --> [Application Layer] --> [Database Layer]
1. Presentation: Web/Mobile UI for customers and tellers
2. Application: Python/Java/Node.js app, handles business logic
3. Database: PostgreSQL (distributed nodes for branches);'
);

┌─────────────────────────────────────────────────────────────────────────────┐
│                        PRESENTATION TIER (Tier 1)                           │
└─────────────────────────────────────────────────────────────────────────────┘
         ↓ HTTP/HTTPS/REST API          ↓ HTTP/HTTPS/REST API
┌─────────────────────────────────────────────────────────────────────────────┐
│                      APPLICATION TIER (Tier 2)                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐  │
│  │   Web Server    │  │  API Gateway    │  │   Load Balancer             │  │
│  │  (Nginx/Apache) │  │  (Rate Limiting,│  │  (Distributes Requests)     │  │
│  └─────────────────┘  │  Authentication)│  └─────────────────────────────┘  │
│           │            └─────────────────┘              │                   │
│           │                      │                      │                   │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    BUSINESS LOGIC LAYER                            │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │    │
│  │  │ Customer    │  │ Account     │  │ Transaction │  │ Teller      │ │    │
│  │  │ Service     │  │ Service     │  │ Service     │  │ Service     │ │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘ │    │
│  │          │              │                │               │          │    │
│  │  ┌──────────────────────────────────────────────────────────────┐   │    │
│  │  │              DISTRIBUTED TRANSACTION MANAGER                │   │    │
│  │  │  - Two-Phase Commit Coordinator                             │   │    │
│  │  │  - Connection Pooling                                       │   │    │
│  │  │  - Foreign Data Wrapper Management                          │   │    │
│  │  └──────────────────────────────────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
         ↓ JDBC/ODBC                  ↓ JDBC/ODBC                  ↓ JDBC/ODBC
┌─────────────────────────────────────────────────────────────────────────────┐
│                       DATABASE TIER (Tier 3)                                │
│  ┌─────────────────────────────────┐  ┌─────────────────────────────────┐  │
│  │        BRANCH DB A              │  │        BRANCH DB B              │  │
│  │  ┌─────────────────────────┐    │  │  ┌─────────────────────────┐    │  │
│  │  │ Local Tables:           │    │  │  │ Local Tables:           │    │  │
│  │  │ • Customer              │◄───┼──┼──│ • Transactions          │    │  │
│  │  │ • Account               │    │  │  │ • Loan                  │    │  │
│  │  │ • Teller                │    │  │  │ • Payment               │    │  │
│  │  │ • branch_a_transactions │    │  │  │ • LoanRepayments        │    │  │
│  │  └─────────────────────────┘    │  │  │ • branch_b_transactions │    │  │
│  │                                 │  │  └─────────────────────────┘    │  │
│  │  ┌─────────────────────────┐    │  │                                 │  │
│  │  │ Foreign Tables:         │    │  │  ┌─────────────────────────┐    │  │
│  │  │ • transactions (from B) │    │  │  │ Foreign Tables:         │    │  │
│  │  │ • loan (from B)         │    │  │  │ • customer (from A)     │    │  │
│  │  │ • payment (from B)      │    │  │  │ • account (from A)      │    │  │
│  │  │ • loanrepayments (from B)│   │  │  │ • teller (from A)       │    │  │
│  │  └─────────────────────────┘    │  │  └─────────────────────────┘    │  │
│  └─────────────────────────────────┘  └─────────────────────────────────┘  │
│         ▲                                      ▲                           │
│         │                                      │                           │
└─────────┼──────────────────────────────────────┼───────────────────────────┘
          │              postgres_fdw            │
          └──────────────────────────────────────┘

-----Task 9
EXPLAIN (ANALYZE, BUFFERS)
SELECT c.FullName, a.Balance, t.Amount
FROM Customer c
JOIN Account a ON c.CustomerID = a.CustomerID
JOIN Transaction t ON a.AccountID = t.AccountID;

---- Tast 10
-- 10. Centralized
EXPLAIN (ANALYZE, BUFFERS)
SELECT SUM(Amount)
FROM Transaction_Large;

-- Parallel

SET max_parallel_workers_per_gather = 0;


EXPLAIN (ANALYZE, BUFFERS)
SELECT SUM(Amount)
FROM Transaction_Large;

---- distributed
-- Disable parallel workers for serial execution
SET max_parallel_workers_per_gather = 0;


CREATE SERVER brancha_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'brancha_host',    -- replace with actual host/IP
    dbname 'BranchDB_A',    -- remote database name
    port '5432'
);
CREATE USER MAPPING FOR CURRENT_USER
SERVER brancha_server
OPTIONS (
    user 'branch_user',         
    password 'branch_password'
);

CREATE FOREIGN TABLE foreign_transaction_branchb (
    transaction_id INT,
    amount NUMERIC
)
SERVER brancha_server
OPTIONS (
    schema_name 'public',
    table_name 'foreign_transaction'
);


