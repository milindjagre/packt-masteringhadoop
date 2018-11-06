use packt_oozie;
create table student
(
gender STRING,
NationalITy STRING,
PlaceofBirth STRING,
StageID STRING,
GradeID STRING,
SectionID STRING,
Topic STRING,
Semester STRING,
Relation STRING,
raisedhands INT,
VisITedResources INT,
AnnouncementsView INT,
Discussion INT,
ParentAnsweringSurvey STRING,
ParentschoolSatisfaction STRING,
StudentAbsenceDays STRING,
Class STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
;
load data local inpath '/home/cloudera/packt/oozie/student/student_grade_data.csv' into table student;
insert overwrite directory 'hdfs://quickstart.cloudera:8020/packt/oozie/student/gender_distribution' select gender, count(gender) as c1 from student group by gender order by c1 desc;
insert overwrite directory 'hdfs://quickstart.cloudera:8020/packt/oozie/student/absenty_distribution' select StudentAbsenceDays, count(StudentAbsenceDays) as c1 from student group by StudentAbsenceDays order by c1 desc;
insert overwrite directory 'hdfs://quickstart.cloudera:8020/packt/oozie/student/topic_distribution' select Topic, count(Topic) as c1 from student group by Topic order by c1 desc;
insert overwrite directory 'hdfs://quickstart.cloudera:8020/packt/oozie/student/parent_school_satisafaction_distribution' select ParentschoolSatisfaction, count(ParentschoolSatisfaction) as c1 from student group by ParentschoolSatisfaction order by c1 desc;
