-- Prerequisite table (PostgreSQL)
-- Note: Replaced Oracle's NUMBER with SERIAL for auto-increment PK and VARCHAR2 with VARCHAR


CREATE TABLE PATIENT (
PATIENTid SERIAL PRIMARY KEY,
PATIENTname VARCHAR(100) NOT NULL
);
-- Corrected PATIENT_MED table (PostgreSQL)
-- Note: Replaced Oracle's NUMBER(6,2) with NUMERIC(6,2) and VARCHAR2 with VARCHAR
CREATE TABLE PATIENT_MED (
PATIENT_medid SERIAL PRIMARY KEY, -- unique id
PATIENTid INTEGER NOT NULL REFERENCES PATIENT(PATIENTid), -- must reference an existing patient
med_name VARCHAR(80) NOT NULL, -- mandatory field
dose_mg NUMERIC(6,2) CHECK (dose_mg >= 0), -- non-negative dose
start_dt DATE,
end_dt DATE,
CONSTRAINT ck_rx_dates CHECK (
        start_dt IS NULL OR end_dt IS NULL OR start_dt <= end_dt) -- sensible date logic
);
select * from PATIENT_MED ;


-- Main bill table (PostgreSQL)
CREATE TABLE BILL (
BILid SERIAL PRIMARY KEY,
total NUMERIC(12,2)
);

-- Items linked to bills (PostgreSQL)
CREATE TABLE BILL_ITEM (
ITEMid SERIAL PRIMARY KEY, -- Added a PK for easier updates/deletes/identity
BILid INTEGER REFERENCES BILL(BILid),
amount NUMERIC(12,2),
updated_at TIMESTAMP WITHOUT TIME ZONE
);


-- Audit log for changes (PostgreSQL)
CREATE TABLE BILL_AUDIT (
AUDIT_id INTEGER PRIMARY KEY,
old_total NUMERIC(12,2),
new_total NUMERIC(12,2),
changed_at TIMESTAMP WITHOUT TIME ZONE
);

-- Staff supervisor table (PostgreSQL)
CREATE TABLE STAFF_SUPERVISOR (
employee VARCHAR(50),
supervisor VARCHAR(50)
);

CREATE TABLE TRIPLE (
    s VARCHAR(100), -- Subject (like 'Influenza' or 'patient1')
    p VARCHAR(50),  -- Predicate (like 'isA' or 'hasDiagnosis')
    o VARCHAR(100)  -- Object (like 'ViralInfection' or 'Malaria')
);


-- Insert initial data into PATIENT (Need this for Foreign Key checks)
INSERT INTO PATIENT (PATIENTid, PATIENTname) VALUES 
(001, 'Eric MUGWANEZA'), 
(002, 'Esperance UWASE'),
(003, 'Pascal TUYISHIME'), 
(004, 'Feza MAHORO'); 

select *from  PATIENT;

-- Insert two placeholder BILLs
INSERT INTO BILL (BILid, total)
VALUES 
(1, 0),
(2, 0),
(3, 2);

select * from BILL

-----Valid PATIENT_MED insertions
INSERT INTO PATIENT_MED (PATIENT_medid, PATIENTid, med_name, dose_mg, start_dt, end_dt)
VALUES
(3, 001, 'Paracetamol', 500, TO_DATE('2025-10-01','YYYY-MM-DD'), TO_DATE('2025-10-05','YYYY-MM-DD')), 
(4, 002, 'buprofene', 500, TO_DATE('2025-10-01','YYYY-MM-DD'), TO_DATE('2025-10-05','YYYY-MM-DD')), 
(2, 001, 'Cetirizine', 10, NULL, NULL); 
select * from PATIENT_MED;

-- STAFF_SUPERVISOR data insertion
INSERT INTO STAFF_SUPERVISOR(employee,supervisor)
VALUES 
('Alice', 'PASCAL'),
('EMMANUEL', 'ALICE'),
('CAROL', 'DIANA'),
('ALICE', 'FRANK'),
('PASCAL', 'EMMANUEL');
select * from STAFF_SUPERVISOR;


-- TRIPLE table data insertion
-- Patient diagnoses
INSERT INTO TRIPLE(s,p,o)
VALUES 
('patient1', 'hasDiagnosis', 'Influenza'),
('patient2', 'hasDiagnosis', 'COVID19'),
('patient3', 'hasDiagnosis', 'Malaria'),
('patient4', 'hasDiagnosis', 'Diabetes');
select * from TRIPLE;

-- Taxonomy edges
INSERT INTO TRIPLE 
VALUES 
('influenza', 'isA', 'ViralInfection'),
('COVID19', 'isA', 'ViralInfection'),
('Malaria', 'isA', 'ParasiticInfection'),
('ViralInfection', 'isA', 'InfectiousDisease'),
('ParasiticInfection', 'isA', 'InfectiousDisease'),
('Diabetes', 'isA', 'ChronicDisease');
select * from  TRIPLE;