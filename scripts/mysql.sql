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
 
 drop table Run$Operation;
 create table Run$Operation (
 Graph varchar(8) not null,
 TableName varchar(60) not null,
 Operation varchar(80) not null,
 Status varchar(256) not null,
 Created DateTime,
 LastUpdated DateTime,
 RunId Binary(16));
 
 
 alter table Run$Operation add primary key (Graph, TableName, Operation);
 alter table Run$Operation add unique key (RunId);
 
 drop table Run$State;
 create table Run$State (
 RunId Binary(16),
 Name varchar(30) not null, 
 Value varchar(60) not null, 
 LastUpdated Date not null);
 
 alter table Run$State add primary key (RunId, Name);
  
 drop table Run$Run;
 create table Run$Run (
 RunId Binary(16),
 RunId_ Binary(16),
 Created Date not null);
 
 alter table Run$Run add primary key (RunId,RunId_);
  
  
  
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