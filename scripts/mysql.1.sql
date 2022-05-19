drop table Edge_Relationship;

create table Edge_Relationship (
 Bid smallint,
 Puid Binary(16) ,
 Cnt smallint,
 Status varchar(4) ,
 ErrMsg varchar(240)
 );
// PARTITION BY HASH(Bid)
//PARTITIONS 4;
 
alter  table Edge_Relationship add primary key (Bid,Puid);
 
drop table EdgeChild_Relationship;
 create table EdgeChild_Relationship (
 Puid Binary(16),
 SortK_Cuid varchar(60) not null,
 Status varchar(4) ,
 ErrMsg varchar(240)
 )
 ;
--   PARTITION BY HASH(Puid)
-- PARTITIONS 4;
 
 
 
 alter table  EdgeChild_Relationship add primary key (Puid, SortK_Cuid);
 
 create table State (
 Name varchar(80) primary key,
 Value varchar(256) not null,
 Updated DateTime,
 Run Binary(16));
 
 	e := &Event{eid: eid, event: name, status: Running, TxHandle: tx.New("LogEvent-" + name)}

	e.Add(e.NewMutation(tbl.Event, 0).AddMember("event", e.event))
 
 create table EdgeEv (
 # base event related columns
 ID  Binary(16) primary key,
 RunID  Binary(16) not null,
 Event varchar(38) not null,
 Status varchar(12),
 Start Datetime(6),
# edge related columns
 Puid Binary(16) ,
 Edges Int 
 Finish Datetime(6),
 Duration varchar(24));

create table SubEvent (
 EvID Binary(16),
 Label varchar(128),
 Start  Datetime(6),
 Finish Datetime(6),
 Duration varchar(24),
 Status varchar(1)); # E-errored, C-Complete