package throttle

type Throttler interface {
	Up()
	Down()
}
