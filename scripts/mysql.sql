drop table Edge_Movies;

create table Edge_Movies (
 Bid smallint,
 Puid Binary(16) ,
 Cnt smallint,
 Status varchar(4) ,
 ErrMsg varchar(240)
 );
// PARTITION BY HASH(Bid)
//PARTITIONS 4;
 
alter  table Edge_Movies add primary key (Bid,Puid);
 
drop table EdgeChild_Movies;
 create table EdgeChild_Movies (
 Puid Binary(16),
 SortK_Cuid varchar(60) not null,
 Status varchar(4) ,
 ErrMsg varchar(240)
 )
 ;
--   PARTITION BY HASH(Puid)
-- PARTITIONS 4;
 
 alter table  EdgeChild_Movies add primary key (Puid, SortK_Cuid);
 
 drop table Graph;
 create table Graph (
 Id varchar2(3) primary key,
 Graph varchar(30),
 Enabled char(1) not null default "Y")
 
alter table Graph add unique key (Graph);
 
 drop table mtn$Op;
 create table mtn$Op (
 Id varchar(3) primary key,
 Name varchar(80) not null,
 MinIntervalDaySecond varchar(12) , # "D HR:MM:SS"
 Enabled char(1) not null default "Y");
 
 alter table mtn$Op add unique key (Name);
 
insert into mtn$Op (Id,Name,MinIntervalDaySecond) Values ("DP","Double Propagation","0 0:0:30");

 // current operations - test & prod the same. Prod has prod tableName.
 drop table run$Op;
 create table run$Op (
 GraphId varchar(3) not null,
 OpId  varchar(3) not null,
 Created DateTime not null,
 Status varchar(256) not null,
 LastUpdated DateTime,
 RunId Binary(16) not null,
 Error varchar(200));
 
 
 alter table run$Op add primary key (GraphId, OpId, Created );
 alter table run$Op add unique key (RunId);

 create or replace view run$Op_Status_v as
 select r.OpId , r.GraphId, r.Status, r.Created, r.LastUpdated, r.RunId, m.MinIntervalDaySecond
 From run$Op r
 join mtn$Op m on (m.Id=r.OpId)
 order by Created desc
 limit 1;
 #where m.Enabled = "Y";
 
 create or replace view run$Op_Last_v as
 select r.OpId , r.GraphId, r.Status, r.Created, r.LastUpdated, r.RunId, m.MinIntervalDaySecond, m.Enabled
 From run$Op r
 join mtn$Op m on (m.Id=r.OpId)
 where m.Enabled = "Y"
 order by Created desc
 limit 1;
 
 
 create or replace view run$Op_PastInterval_v  as
 select OpId , GraphId, Status, Created, LastUpdated, RunId, MinIntervalDaySecond
 From run$Op_Last_v
 where Enabled = "Y"
 or DATE_SUB(NOW(), Interval MinIntervalDaySecond DAY_SECOND) > LastUpdated;
 
 -- create or replace view run$Op_PastInterval_v as
 -- select r.OpId, r.GraphId GraphId, r.Status, r.Created, r.LastUpdated, r.RunId, r.Error, o.Enabled
 -- From run$Op r
 -- join mtn$Op o on (r.OpId = o.Id)
 -- where DATE_SUB(NOW(), Interval o.MinIntervalDaySecond DAY_SECOND) > r.LastUpdated;

 drop table run$State;
 create table run$State (
 RunId Binary(16),
 Name varchar(30) not null, 
 Value varchar(60) not null, 
 LastUpdated DateTime not null);
 
 alter table run$State add primary key (RunId, Name);
  
 drop table run$Run;
 create table run$Run (
 RunId Binary(16),
 Associated_RunId Binary(16),
 Error varchar(400) not null);
 
 alter table run$Run add primary key (RunId,Associated_RunId);
  
  
  
  # base event related columns
 drop table EV$event;
 create table EV$event (
 ID  Binary(16) not null,
 RunID  Binary(16) not null,
 Event varchar(38) not null,
 Status varchar(12),
 Start Datetime(6),
 Finish Datetime(6),
 Duration varchar(24));
 
 alter table EV$event add primary key (Graph, ID);
 
#tasks related to an event (optional)
create table EV$task (
 ID Binary(16) primary key,
 EvID Binary(16),
 Task varchar(128),
 Start  Datetime(6),
 Finish Datetime(6),
 Duration varchar(24),
 Status varchar(1)
 ErrMsg varchar(500)); # E-errored, C-Complete
 
 
 # specific event columns (1:1 with EV$event)
 create table EV$edge (
 EvID  Binary(16) primary key,
 Puid Binary(16) ,
 Edges Int 
 );
 
 
 drop table State$ES;
  # specific event columns (1:1 with EV$event)
 create table State$ES (
 Graph varchar(8),
 Ty  varchar(12),
 Sortk varchar(16) ,
  Attr varchar(30),
 Status varchar(1),
 ErrMsg varchar(200),
 RunID Binary(16) not null
 );
 
alter  table State$ES add primary key (Graph,Ty,Sortk,Attr);


drop table Log$GoTest;

create table Log$GoTest (
LogDT DateTime(6),
Test varchar(64),
Status varchar(1),
Nodes Int,
ParseET varchar(32),
ExecET varchar(32),
DBread Int,
Levels varchar(128),
Msg varchar(200),
JSON varchar(2000)
);

alter table Log$GoTest add primary key (LogDT,Test);
create index Log$testidx on Log$GoTest (Test);