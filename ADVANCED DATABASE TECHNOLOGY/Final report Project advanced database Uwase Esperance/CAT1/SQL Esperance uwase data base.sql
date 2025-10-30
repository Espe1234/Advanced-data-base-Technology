
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

-- TASK 3: Insert the Data INTO tables

INSERT INTO Customer (CustomerID, firtName, LastName, Email, Phone, City)
VALUES
('C001', 'Alice', 'Mukamana', 'alice.mukamana@example.com', '+250788111111', 'Kigali'),
('C002', 'John', 'Habimana', 'john.habimana@example.com', '+250788222222', 'Musanze'),
('C003', 'Grace', 'Uwizeye', 'grace.uwizeye@example.com', '+250788333333', 'Huye'),
('C004', 'David', 'Nshimiyimana', 'david.nshimiyimana@example.com', '+250788444444', 'Rubavu'),
('C005', 'Sarah', 'Uwimana', 'sarah.uwimana@example.com', '+250788555555', 'Rwamagana'),
('C006', 'Eric', 'Mugisha', 'eric.mugisha@example.com', '+250788666666', 'Kayonza');


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


INSERT INTO Payment (PaymentID, Amount, PaymentDate, methodmode, LoanID)
VALUES
(1, 500.00, '2025-10-10', 'Credit Card', 102),
(2, 750.50, '2025-10-11', 'Cash', 102),
(3, 1200.00, '2025-10-12', 'Mobile Money', 103),
(4, 300.75, '2025-10-13', 'Bank Transfer', 104),
(5, 950.00, '2025-10-14', 'Debit Card', 105),
(6, 1100.25, '2025-10-15', 'Online Payment', 102),
(7, 650.00, '2025-10-16', 'Credit Card', 103),
(8, 400.00, '2025-10-17', 'Cash', 104);


INSERT INTO Transactions (TransID, AccountID, TellerID, Amount, classtype, DataPerformed)
VALUES
(1, '1001', 'T001', 500.00, 'Deposit', '2025-10-10'),
(2, '1002', 'T002', 1200.00, 'Withdrawal', '2025-10-11'),
(3, '1003', 'T003', 2500.00, 'Transfer', '2025-10-12'),
(4, '1004', 'T004', 750.50, 'Deposit', '2025-10-13'),
(5, '1005', 'T005', 1000.00, 'Withdrawal', '2025-10-14');

INSERT INTO Loan (LoanID, AccountID, Amount, InterestRate, StartDate, EndDate, Status)
VALUES
(101, '1001', 5000.00, 8.5, '2024-01-15', '2025-01-15', 'Active'),
(102, '1002', 10000.00, 7.2, '2024-03-01', '2026-03-01', 'Active'),
(103, '1003', 7500.00, 9.0, '2023-10-10', '2025-10-10', 'Closed'),
(104, '1004', 3000.00, 10.0, '2024-06-20', '2025-06-20', 'Active'),
(105, '1005', 15000.00, 6.8, '2023-09-01', '2026-09-01', 'Pending');


INSERT INTO Payment (PaymentID, Amount, PaymentDate, methodmode, LoanID)
VALUES
(1, 500.00, '2024-02-15', 'Credit Card', 103),
(2, 750.00, '2024-04-10', 'Cash', 104),
(3, 1200.00, '2024-06-05', 'Mobile Money', 103),
(4, 300.00, '2024-07-20', 'Bank Transfer', 104),
(5, 950.00, '2024-09-25', 'Debit Card', 105);


INSERT INTO LoanRepayments (LoanID, Amount, RepaymentDate)
VALUES
(101, 500.00, '2024-03-15'),
(102, 750.00, '2024-05-10'),
(103, 1200.00, '2024-07-05'),
(104, 600.00, '2024-08-20');

-- TASK 4: Retrieve customerSELECT c.CustomerID, c.LastName, a.AccountID, a.Balance
FROM Customer c
JOIN Account a ON c.CustomerID = a.CustomerID
WHERE a.Balance = (
 SELECT MAX(Balance) FROM Account
); with highest account balance




--- TASK 5.Update account balance after transaction
 
 UPDATE Account
SET Balance = Balance + 1000  -- deposit amount
WHERE Accountid = 1;          -- target account

UPDATE Account
SET Balance = balance - 500   -- withdrawal amount
WHERE accountid = 1
  AND Balance >= 500;         -- optional check to prevent negative balance
  


--- TASK 6. Identify tellers handing most transaction
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


--- TASK 7.create a view showing total loan repayments per month
CREATE OR REPLACE VIEW MonthlyLoanRepayments AS
SELECT
    DATE_TRUNC('month', RepaymentDate) AS RepaymentMonth,
    SUM(Amount) AS TotalRepayments
FROM
    LoanRepayments
GROUP BY
    DATE_TRUNC('month', RepaymentDate)
ORDER BY
    RepaymentMonth;

SELECT * FROM MonthlyLoanRepayments

-- TASK 8: Implement trigger blocking withdrawals exceeding current balance
CREATE OR REPLACE FUNCTION prevent_overdraft()
RETURNS TRIGGER AS $$
DECLARE
    current_balance DECIMAL(10,2);
BEGIN
    -- Only check for withdrawal transactions
    IF NEW.classtype = 'Withdrawal' THEN
        -- Get current balance from Account table
        SELECT Balance INTO current_balance
        FROM Account
        WHERE AccountID = NEW.AccountID;

        -- Check if withdrawal exceeds balance
        IF NEW.Amount > current_balance THEN
            RAISE EXCEPTION 'Withdrawal amount (%.2f) exceeds current balance (%.2f)', NEW.Amount, current_balance;
        ELSE
            -- Update account balance after withdrawal
            UPDATE Account
            SET Balance = Balance - NEW.Amount
            WHERE AccountID = NEW.AccountID;
        END IF;
    END IF;

    -- For deposits, add to balance
    IF NEW.classtype = 'Deposit' THEN
        UPDATE Account
        SET Balance = Balance + NEW.Amount
        WHERE AccountID = NEW.AccountID;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER check_withdrawal
BEFORE INSERT ON Transactions
FOR EACH ROW
EXECUTE FUNCTION prevent_overdraft();



-- Test cascade
DELETE FROM loan 
WHERE loanid = 104








