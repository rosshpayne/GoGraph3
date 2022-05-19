	`CREATE TABLE Block (
  				PKey BYTES(16) NOT NULL,
				Ty STRING(8) ,
				P BYTES(16),
				) PRIMARY KEY(PKey)`,
			
    `CREATE INDEX NodeTy ON Block(Ty)`,
// Edge: 1 row contains thousands of edges stored in arrays
	`CREATE TABLE Edge (
			PKey BYTES(16) NOT NULL,
			SortK STRING(64) NOT NULL, // A#G#:? + A#G#:?#?    Edge 
			Nd ARRAY<BYTES(16)> , // Child Node or OBlock UID
			Id ARRAY<INT64> ,     // 1 for Child node or 1..n batch id if OBlock 
			XF ARRAY<INT64> ,     // flag: Child node, Deleted, OBlock 
			N INT64,              // no of Child nodes
			P STRING(128),        // predicate name (long). Why is this here?
			) PRIMARY KEY(PKey, SortK)`

// alternate for Edge - 1 row one edge
	`CREATE TABLE EOP (
			PPK BYTES(16) NOT NULL,
            SortK STRING(64) NOT NULL, // A#G#:?
			CPK BYTES(16) NOT NULL,
			XF INT64 , // flag: deleted, Overflow etc.
			) PRIMARY KEY(PKey, SortK)`

	`CREATE TABLE NodeScalar (
			PKey BYTES(16) NOT NULL,
			SortK STRING(64) NOT NULL,
			Ty STRING(8),
			P STRING(32),
			Bl BOOL,
			I INT64,
			F FLOAT64,
			S STRING(MAX),
			B BYTES(MAX),
			DT TIMESTAMP,
			 LS   ARRAY<STRING(MAX)>,
			LI    ARRAY<INT64>,
			LF    ARRAY<FLOAT64>,
             LBl  ARRAY<BOOL>,
             LB   ARRAY<BYTES(MAX)>,
             LDT  ARRAY<TIMESTAMP>,
			) PRIMARY KEY(PKey, SortK),
	INTERLEAVE IN PARENT Block ON DELETE CASCADE`,
	
    `CREATE NULL_FILTERED INDEX NodePredicateF ON NodeScalar(P,F)`,
	`CREATE NULL_FILTERED INDEX NodePredicateS ON NodeScalar(P,S)`,
    `CREATE NULL_FILTERED INDEX NodePredicateI ON NodeScalar(P,I)`,
	`CREATE NULL_FILTERED INDEX NodePredicateBl ON NodeScalar(P,Bl)`,
	`CREATE NULL_FILTERED INDEX NodePredicateB  ON NodeScalar(P,B)`,
	`CREATE NULL_FILTERED INDEX NodePredicateDT ON NodeScalar(P,DT)`,

	`Create table PropagatedScalar(
			PKey   BYTES(16) NOT NULL,
			SortK  STRING(64) NOT NULL,
			LS     ARRAY<STRING(MAX)>,
			LI     ARRAY<INT64>,
			LF     ARRAY<FLOAT64>,
			LBl    ARRAY<BOOL>,
			LB     ARRAY<BYTES(MAX)>,
			PBS    ARRAY<BYTES(128)>, 
			BS     ARRAY<BYTES(128)>,
			) PRIMARY KEY (PKey, SortK),
	INTERLEAVE IN PARENT Block ON DELETE CASCADE`,
	
    `CREATE TABLE ReverseEdge (
			PKey BYTES(16) NOT NULL, 
			SortK BYTES(64) NOT NULL, // "R#:<UID-PRED-shortname>
			pUID BYTES(16) NOT NULL,
			Batch INT64 ,  
			) PRIMARY KEY(PKey,SortK),
			INTERLEAVE IN PARENT Block ON DELETE CASCADE`,
	`CREATE INDEX Reverse ON ReverseEdge(PKey,pUID)`,

    `Create table Graph (
		GId INT64 NOT NULL,
		Name STRING(MAX) NOT NULL,    
		SName STRING(MAX) NOT NULL,
		) PRIMARY KEY(GId)`,

	`create table Type (
		GId INT64 NOT NULL,
		Name STRING(MAX) NOT NULL,
		SName STRING(MAX) NOT NULL,
  		) PRIMARY KEY(GId, Name),
    INTERLEAVE IN PARENT Graph ON DELETE CASCADE`,
	
    `create table Attribute (
		GId INT64 NOT NULL,
		TSName STRING(MAX) NOT NULL,
		Name STRING(MAX) NOT NULL,
		Ty  STRING(MAX) NOT NULL,
		SName STRING(MAX) NOT NULL, 
		Nullable BOOL ,
		AttributesPropagate ARRAY<STRING(MAX)>,
		Facet  ARRAY<STRING(MAX)>,
		Ix STRING(MAX) ,
		Part STRING(8),
		Propagate BOOL,
		) PRIMARY KEY(GId,TSName, Name),
    INTERLEAVE IN PARENT Graph ON DELETE CASCADE`,

	`create unique NULL_FILTERED  index GraphName on Graph (GId,SName)`,
	`create unique NULL_FILTERED  index TypeName on Type (GId,SName)`,
	`create unique NULL_FILTERED  index AttrName on Attribute (GId,TSName, SName)`,
	
    `create table eventlog (
        eid  BYTES(16) NOT NULL,
        event STRING(16) NOT NULL,
        status STRING(1) NOT NULL,
        start  Timestamp NOT NULL,
        finish Timestamp ,
        dur    STRING(30),
        err    STRING(MAX),
    ) PRIMARY KEY (eid)`,

	`create table NodeAttachDetachEvent (
        eid  BYTES(16) NOT NULL,
        PUID BYTES(16) NOT NULL,
        CUID BYTES(16) NOT NULL,
        SORTK STRING(MAX) NOT NULL,
        ) PRIMARY KEY (eid),
    INTERLEAVE IN PARENT eventlog ON DELETE CASCADE`,

    			
    `Create table dual (
				d bool,
			) PRIMARY KEY (d)`,

	`Create table TestArray (
				idx INT64 NOT NULL,
				Num  numeric,
				f64 float64,
				i64 int64,
				numarray ARRAY<NUMERIC>,
				intarray ARRAY<INT64>,
				Strarray ARRAY<STRING(100)>,
				Bytearray ARRAY<BYTES(16)>,
			) PRIMARY KEY (idx)`,

    



    ==========


    `CREATE TABLE Block (
  				PKey BYTES(16) NOT NULL,
				Ty STRING(8) ,
				IsNode STRING(1), 
				SortK STRING(MAX),
				P BYTES(16),
				) PRIMARY KEY(PKey)`,
			`CREATE INDEX NodeTy ON Block(Ty)`,
			`CREATE NULL_FILTERED INDEX IsNode ON Block(IsNode)`,
			`CREATE TABLE EOP (
			PKey BYTES(16) NOT NULL,
			SortK STRING(64) NOT NULL,
			Nd ARRAY<BYTES(16)> ,
			Id ARRAY<INT64> ,
			XF ARRAY<INT64> ,
			N INT64,
			P STRING(128),
			LS     ARRAY<STRING(MAX)>,
			LI    ARRAY<INT64>,
			LF    ARRAY<FLOAT64>,
			LBl    ARRAY<BOOL>,
			LB     ARRAY<BYTES(MAX)>,
			XBl	ARRAY<BOOL>,
			) PRIMARY KEY(PKey, SortK),
			INTERLEAVE IN PARENT Block ON DELETE CASCADE`,
			`CREATE TABLE NodeScalar (
			PKey BYTES(16) NOT NULL,
			SortK STRING(64) NOT NULL,
			Ty STRING(8),
			P STRING(32),
			Bl BOOL,
			I INT64,
			F FLOAT64,
			S STRING(MAX),
			B BYTES(MAX),
			DT TIMESTAMP,
			 LS   ARRAY<STRING(MAX)>,
			LI    ARRAY<INT64>,
			LF    ARRAY<FLOAT64>,
             LBl  ARRAY<BOOL>,
             LB   ARRAY<BYTES(MAX)>,
             LDT  ARRAY<TIMESTAMP>,
			) PRIMARY KEY(PKey, SortK),
			INTERLEAVE IN PARENT Block ON DELETE CASCADE`,
			`CREATE NULL_FILTERED INDEX NodePredicateF ON NodeScalar(P,F)`,
			`CREATE NULL_FILTERED INDEX NodePredicateS ON NodeScalar(P,S)`,
			`CREATE NULL_FILTERED INDEX NodePredicateI ON NodeScalar(P,I)`,
			`CREATE NULL_FILTERED INDEX NodePredicateBl ON NodeScalar(P,Bl)`,
			`CREATE NULL_FILTERED INDEX NodePredicateB ON NodeScalar(P,B)`,
			`CREATE NULL_FILTERED INDEX NodePredicateDT ON NodeScalar(P,DT)`,
			`Create table dual (
				d bool,
			) PRIMARY KEY (d)`,
			`CREATE TABLE ReverseEdge (
			PKey BYTES(16) NOT NULL, 
			SortK BYTES(64) NOT NULL,
			pUID BYTES(16) NOT NULL,
			Status BYTES(1) ,  
			) PRIMARY KEY(PKey,SortK),
			INTERLEAVE IN PARENT Block ON DELETE CASCADE`,

            create table testlog (
                ID STRING(64) NOT NULL,
                Test STRING(32) NOT NULL,
                Status STRING(2) NOT NULL,
                Nodes INT64 NOT NULL,
                Levels INT64 NOT NULL,
                ParseET FLOAT64 NOT NULL,
                ExecET FLOAT64 NOT NULL,
                JSON STRING(MAX) ,
                DBread INT64 ,
                Msg STRING(256)
            ) Primary Key (ID,Test)
            create index testi on testlog (Test) 
