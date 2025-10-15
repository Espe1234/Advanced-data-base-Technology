-- NAMES:UWASE Esperancere
-- REG:223027305

-- ADVANCE DATABASE TECHONOLOGY

--1. Create all tables and apply integrity constrains
--2.Apply CASCADE DELETE between Loan->Payment

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
CREATE TABLE Payment(
PaymentID INT PRIMARY KEY,
Amount decimal(10,2) NOT NULL,
PaymentDate Date NOT NULL,
methodmode varchar (255) NOT NULL,
LoanID INT REFERENCES Loan(LoanID) on DELETE CASCADE
);
select *from Payment ;
--- 3.Insert 5 customers and 10 transaction

INSERT INTO Customer(CustomerID,firtName,LastName,Email,Phone,City)
VALUES
(1,'UWASE','Esperance','esperanceuwase12@gmail.com','0784567321','Kigali'),
(2,'SHEMA','Eric','shema4@gmail.com','078654321','Musanze'),
(3,'UWERA','Divine','uweradivine86@gmail.com','0798654321','Kigali'),
(4,'IYIZIRE','Auspice','iyizireau321@gmail.com','0735214578','Huye'),
(5,'UWIMPUWE','Ruth','uwiruth87@gmail.com','0786543206','Rubavu');
select*from transactions ;
INSERT INTO transactions (TransID,AccountID,TellerID,Amount,Classtype,Dataperformed)
VALUES
(11, 'AC001', 'T001', 1500.00, 'Deposit', '2025-10-01'),
(2, 'AC002', 'T002',  200.00, 'Withdrawal', '2025-10-02'),
(3, 'AC003', 'T008',  500.00, 'Deposit', '2025-10-03'),
(4, 'AC004', 'T003', 1000.00, 'Transfer', '2025-10-04'),
(5, 'AC005', 'T001',  700.00, 'Withdrawal', '2025-10-05'),
(6, 'AC006', 'T002', 2500.00, 'Deposit', '2025-10-06'),
(7, 'AC007', 'T004',  300.00, 'Transfer', '2025-10-07'),
(8, 'AC008', 'T002',  450.00, 'Deposit', '2025-10-08'),
(9, 'AC009', 'T003',  800.00, 'Withdrawal', '2025-10-09'),
(10,'AC010', 'T001', 1200.00, 'Deposit', '2025-10-10');
select*from Account;

--- 4. Retrieve customers with highest account balances.

INSERT INTO Account (AccountID, CustomerID, Accounttype, Balance, DateOpened, Status)
VALUES
    (1, 101, 'Savings', 5000, '2025-01-10', 'Active'),
    (2, 102, 'Current', 7500, '2025-02-15', 'Active'),
    (3, 103, 'Savings', 7500, '2025-03-20', 'Active'),
    (4, 104, 'Current', 3000, '2025-04-05', 'Inactive'),
    (5, 105, 'Savings', 6000, '2025-05-12', 'Active');
  
SELECT c.CustomerID, c.LastName, a.AccountID, a.Balance
FROM Customer c
JOIN Account a ON c.CustomerID = a.CustomerID
WHERE a.Balance = (
 SELECT MAX(Balance) FROM Account
);
---5.Update account balance after transaction
 
 UPDATE Account
SET Balance = Balance + 1000  -- deposit amount
WHERE Accountid = 1;          -- target account

UPDATE Account
SET Balance = balance - 500   -- withdrawal amount
WHERE accountid = 1
  AND Balance >= 500;         -- optional check to prevent negative balance
  
INSERT INTO Teller (TellerID, FirstName, LastName, Branch, Contract)
VALUES
    (1, 'Alice', 'Kamanzi', 'Kigali Central', 'Permanent'),
    (2, 'Brian', 'Uwase', 'Remera', 'Contract'),
    (3, 'Charles', 'Habimana', 'Kacyiru', 'Permanent'),
    (4, 'Diane', 'Mukamana', 'Gisozi', 'Contract'),
    (5, 'Eric', 'Niyonsaba', 'Kimironko', 'Permanent'); 
	-- Make both columns integers:


--- 6. Identify tellers handing most transaction
SELECT 
    t.TellerID,
    t.FirstName || ' ' || t.LastName AS Name,
    COUNT(tr.transid) AS TransactionCount
FROM Teller t
JOIN Transactions tr ON t.TellerID = tr.TellerID
GROUP BY t.TellerID, t.FirstName, t.LastName
HAVING COUNT(tr.transid) = (
    SELECT MAX(TxCount) FROM (
        SELECT COUNT(transid) AS TxCount
        FROM Transactions
        GROUP BY TellerID
    ) sub
);

INSERT INTO Loan (LoanID, AccountID, Amount, InterestRate, StartDate, EndDate, Status)
VALUES
(1, 'AC001', 5000.00, 5.5, '2025-09-01', '2026-09-01', 'Active'),
(2, 'AC002', 10000.00, 6.0, '2025-08-15', '2026-08-15', 'Active'),
(3, 'AC003', 7500.00, 4.5, '2025-07-10', '2026-07-10', 'Active'),
(4, 'AC004', 12000.00, 7.0, '2025-06-20', '2026-06-20', 'Active'),
(5, 'AC005', 3000.00, 5.0, '2025-10-01', '2026-10-01', 'Active');
select*from Loan ;

---7.create a view showing total loan repayments per month

CREATE TABLE LoanRepayments (
    RepaymentID SERIAL PRIMARY KEY,
    LoanID INT,
    Amount NUMERIC(12,2),
    RepaymentDate DATE
);
select * from loanRepayments;
   
 ---8.implement a trigger blocking withdrawals execeeding current balance  
   CREATE TABLE Accounts (
    AccountID SERIAL PRIMARY KEY,
    AccountHolder VARCHAR(100),
    Balance NUMERIC(12, 2) NOT NULL
);
select* from Accounts ;

CREATE TABLE Transactions (
    TransactionID SERIAL PRIMARY KEY,
    AccountID INT REFERENCES Accounts(AccountID),
    TransactionType VARCHAR(20),   -- e.g. 'Deposit' or 'Withdrawal'
    Amount NUMERIC(12, 2) NOT NULL,
    TransactionDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
select * from tra
-- Suppose account 1 has balance 500
INSERT INTO Transactions (Transactionid, accountid, amount)
VALUES (1, 1, -600);  -- This will fail


Select *from  transactions ;


INSERT INTO transaction (transactionid, accountid, amount)

VALUES (2, 1, -400);  -- This will succeed



