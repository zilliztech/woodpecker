package conc

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
)

type SingleflightSuite struct {
	suite.Suite
}

func (s *SingleflightSuite) TestDo() {
	counter := atomic.Int32{}

	sf := Singleflight[any]{}
	ready := make(chan struct{})
	ch := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			defer wg.Done()
			<-ready // ensure all goroutines start before any proceeds
			sf.Do("test_do", func() (any, error) {
				<-ch
				counter.Add(1)
				return struct{}{}, nil
			})
		}(i)
	}
	close(ready) // release all goroutines at once
	// Give goroutines time to enter sf.Do before unblocking the function
	time.Sleep(10 * time.Millisecond)
	close(ch)
	wg.Wait()
	s.Less(counter.Load(), int32(10), "singleflight should deduplicate concurrent calls")
}

func (s *SingleflightSuite) TestDoChan() {
	counter := atomic.Int32{}

	sf := Singleflight[any]{}
	ready := make(chan struct{})
	ch := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			defer wg.Done()
			<-ready // ensure all goroutines start before any proceeds
			resCh := sf.DoChan("test_dochan", func() (any, error) {
				<-ch
				counter.Add(1)
				return struct{}{}, nil
			})
			<-resCh
		}(i)
	}
	close(ready) // release all goroutines at once
	// Give goroutines time to enter sf.DoChan before unblocking the function
	time.Sleep(10 * time.Millisecond)
	close(ch)
	wg.Wait()
	s.Less(counter.Load(), int32(10), "singleflight should deduplicate concurrent DoChan calls")
}

func (s *SingleflightSuite) TestForget() {
	sf := Singleflight[any]{}
	ch := make(chan struct{})
	submitted := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sf.Do("test_forget", func() (any, error) {
			defer wg.Done()
			close(submitted)
			<-ch
			return struct{}{}, nil
		})
	}()

	<-submitted
	flag := false
	sf.Forget("test_forget")
	sf.Do("test_forget", func() (any, error) {
		flag = true
		return struct{}{}, nil
	})

	close(ch)
	wg.Wait()

	s.True(flag, "new job shall be executed after forget")
}

func TestSingleFlight(t *testing.T) {
	suite.Run(t, new(SingleflightSuite))
}
