package throttle

import (
 	"github.com/GoGraph/grmgr"
 )
 
type throttle int

var Control throttle

func (t throttle) Up() {
    grmgr.ThrottleUpCh <- struct{}{}
    }
    
func (t throttle) Down() {
    grmgr.ThrottleDownCh <- struct{}{}
}

func (t throttle) Stop() {}

func (t throttle) String() {}
