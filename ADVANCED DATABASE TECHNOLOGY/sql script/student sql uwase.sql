CREATE TABLE students(
id INT PRIMARY KEY,
firstname varchar(255) NOT NULL,
lastname varchar (255) NOT NULL,
dateofbirth  date ,
created_at date ,
updated_at date
);
select * from students;

ALTER TABLE students ADD national_id varchar(16);

INSERT INTO students(id,firstname,lastname,dateofbirth,created_at,updated_at,national_id)
values(2,'Emmanuel','Hakizimana','1990-3-15','1990-3-15','1990-3-15','1199812345678908');
select * from students ;
INSERT INTO students(id,firstname,lastname,dateofbirth,created_at,updated_at,national_id)
values(3,'Esperance','Uwase','1998-4-12','1998-4-12','1998-4-12','1199809876543210');
select * from students ;
INSERT INTO students(id,firstname,lastname,dateofbirth,created_at,updated_at,national_id)
values
(4,'Emmerance','Uwera','2001-3-17','2001-3-17','2001-3-17','1995666666666968'),
(5,'Eric','Shema','2002-1-25','2002-1-25','2002-1-25','2001657899090097');
