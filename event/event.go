package event

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/run"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/uuid"
)

type evStatus string

const (
	Complete evStatus = "C" // completed
	Running           = "R" // running
	Fail              = "F" // failed
	//
)

func newUID() uuid.UID {

	// eventlock = new(eventLock)
	// eventlock.Lock()

	// create event UID
	uid, err := uuid.MakeUID()
	if err != nil {
		panic(fmt.Errorf("Failed to make event UID for Event New(): %w", err))
	}
	return uid
}

// interface unecessary as event is only type.
//
// type Event interface {
// 	LogStart(...*mut.Mutation) error
// 	LogEvent(error, ...time.Time) error
// 	// Tx and mutation related
// 	NewMutation(tbl tbl.Name) *mut.Mutation
// 	//AddMember(string, interface{} )
// 	Add(*mut.Mutation)
// 	Persist() error
// 	Reset()
// }
type Task struct {
	parent *Event
	start  time.Time
	label  string
}

// event represents a staging area before data written to storage. Not all event related data
// is necessarily held in the struct.
type Event struct {
	*tx.TxHandle
	EID           uuid.UID
	event         string   // event name e.g "AN" (attachnode), "DN" (detachnode)
	eid           uuid.UID //pk
	start         time.Time
	loggedAtStart bool
}

func New(name string) *Event {
	eid := newUID()
	tz, _ := time.LoadLocation(param.TZ)
	et := time.Now().In(tz)

	// create event and assign transaction handle
	e := &Event{eid: eid, event: name, start: et, TxHandle: tx.NewTx("EV-" + name).DB("mysql-GoGraph")}

	e.EID = eid
	m := e.NewInsert(tbl.Name(tbl.Event)) //"EV$event"
	m.AddMember("ID", e.eid).AddMember("Event", e.event).AddMember("RunID", run.GetRunId())
	e.Add(m)

	return e
}

func NewContext(ctx context.Context, name string) *Event {
	eid := newUID()
	tz, _ := time.LoadLocation(param.TZ)
	et := time.Now().In(tz)

	// create event and assign transaction handle
	e := &Event{eid: eid, event: name, start: et, TxHandle: tx.NewTxContext(ctx, "EV-"+name).DB("mysql-GoGraph")}

	e.EID = eid
	m := e.NewInsert(tbl.Name(tbl.Event)) //"EV$event"
	m.AddMember("ID", e.eid).AddMember("Event", e.event).AddMember("RunID", run.GetRunId())
	e.Add(m)

	return e
}

// func (e *Event) Start(start ...time.Time) {
// 	//
// 	tz, _ := time.LoadLocation(param.TZ)
// 	if len(start) > 0 {
// 		e.start = start[0].In(tz)
// 	} else {
// 		e.start = time.Now().In(tz)
// 	}
// 	m0, _ := e.GetMutation(0)
// 	m0.ReplaceMember("Start", e.start.String())

// }

func (e *Event) NewTask(label string) *Task {
	//
	tz, _ := time.LoadLocation(param.TZ)
	et := time.Now().In(tz)

	t := &Task{parent: e, start: et, label: label}

	return t
}

var evID uuid.UID

func (se *Task) Finish(e error) {
	tz, _ := time.LoadLocation(param.TZ)
	et := time.Now().In(tz)
	//
	m := se.parent.NewInsert(tbl.TaskEv).AddMember("ID", newUID()).AddMember("EvID", se.parent.eid).AddMember("Task", se.label)
	m.AddMember("Finish", et.Format(param.TimeNanoFormat)).AddMember("Duration", et.Sub(se.start).String()).AddMember("Start", se.start.Format(param.TimeNanoFormat))
	if e != nil {
		m.AddMember("Status", Fail)
	} else {
		m.AddMember("Status", Complete)
	}
}

func (e *Event) EventStart(t ...time.Time) error {
	return e.LogStart(t...)
}

func (e *Event) LogStart(t ...time.Time) (err error) {
	//
	e.loggedAtStart = true
	tz, _ := time.LoadLocation(param.TZ)
	if len(t) > 0 {
		e.start = t[0].In(tz)
	} else {
		e.start = time.Now().In(tz)
	}
	m0, _ := e.GetMutation(0)
	m0.AddMember("Start", e.start.Format(param.TimeNanoFormat)).AddMember("Status", Running)

	return e.Persist()
}

func (e *Event) EventCommit(err error, ef ...time.Time) error {
	return e.LogComplete(err, ef...)
}

func (e *Event) LogComplete(err error, ef ...time.Time) error {
	//
	var (
		m *mut.Mutation
	)
	//TODO: keep same API but write to Cloudwatch logs instead of Dynamodb. How to specify Cloudwatch instead. Must be in Tx e.g. NewLog()
	tz, _ := time.LoadLocation(param.TZ)
	et := time.Now().In(tz)

	if e.loggedAtStart {

		var s strings.Builder
		s.WriteString(e.eid.String())
		s.WriteString("#0")

		// as mutation(0) has already been written to the db, an update must be performed.
		m = e.NewUpdate(tbl.Event).AddMember("RunID", run.GetRunId(), mut.IsKey).AddMember("ID", s.String(), mut.IsKey)

		// fs represents start time
		if len(ef) > 0 {
			m.AddMember("Duration", ef[0].Sub(e.start).String())
			m.AddMember("Finish", ef[0].Format(param.TimeNanoFormat))
		} else {
			m.AddMember("Duration", et.Sub(e.start).String())
			m.AddMember("Finish", et.Format(param.TimeNanoFormat))
		}

	} else {

		m, _ = e.GetMutation(0)
		m.AddMember("Start", e.start.Format(param.TimeNanoFormat))

		if len(ef) > 0 {
			m.AddMember("Finish", ef[0].Format(param.TimeNanoFormat))
			m.AddMember("Duration", ef[0].Sub(e.start).String())
		} else {
			m.AddMember("Finish", et.Format(param.TimeNanoFormat))
			m.AddMember("Duration", et.Sub(e.start).String())
		}

	}

	if err != nil {
		m.AddMember("Status", Fail).AddMember("ErrMsg", err.Error())
	} else {
		m.AddMember("Status", Complete)
	}

	return e.Persist()

}
func (e *Event) GetStartTime() time.Time {
	return e.start
}

func (e *Event) NewMutation(t tbl.Name, id int) *mut.Mutation {
	var s strings.Builder
	s.WriteString(e.eid.String())
	s.WriteByte('#')
	s.WriteString(strconv.Itoa(id))
	return mut.NewInsert(t).AddMember("RunID", run.GetRunId()).AddMember("EventID", s.String())

}
