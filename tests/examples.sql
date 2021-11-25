create table student(sno char(15),sname char(20),sage int(4),sgender char(8));
drop table student;

create table teacher(tno char(15),tname char(20),tage int(4),tgender char(8));
create table worker(wno char(15),wname char(20),wage int(4),wgender char(8));

create index studentindex on student(sname);
drop index studentindex;

insert into student values("m002","jim",20,"male");
insert into student values("m003","lucy",22,"female");
insert into teacher values("m0090","mij",2,"female");
insert into teacher values("m0091","cylu",200,"lgbt");
insert into worker values("w01","www1",40,"male");
insert into worker values("w02","www2",41,"lgbt");

select * from student;
select * from student, teacher;
select * from student, teacher, worker;
select * from SYSTABLE;
select * from SYSCOLUMN;

/* TODO */
select sname from student;
select * from student, teachers where student.sage=teachers.id and sname="asdf";
update student set sage=22 where sname=lucy;
