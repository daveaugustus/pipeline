//
// Companion code to https://medium.com/statuscode/pipeline-patterns-in-go-a37bb3a7e61d
//
// To run:
//   go get github.com/pkg/errors
//   go run -race pipeline_demo.go
//

package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// MergeErrors merges multiple channels of errors.
// Based on https://blog.golang.org/pipelines.
func MergeErrors(cs ...<-chan error) <-chan error {
	var wg sync.WaitGroup
	// We must ensure that the output channel has the capacity to hold as many errors
	// as there are error channels. This will ensure that it never blocks, even
	// if WaitForPipeline returns early.
	out := make(chan error, len(cs))

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan error) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// WaitForPipeline waits for results from all error channels.
// It returns early on the first error.
func WaitForPipeline(errs ...<-chan error) error {
	errc := MergeErrors(errs...)
	for err := range errc {
		if err != nil {
			return err
		}
	}
	return nil
}

// minimalPipelineStage shows the elements that every pipeline stage should have.
// All stages should accept a context for cancellation.
// All stages should return a channel of errors to report any error produced after this function returns.
// All stages should return an error to report any error produced before this function returns.
// Any required input parameters should follow ctx and any required outputs should precede
// the errors channel.
// Inputs can be ordinary objects (e.g. a list of strings), channels of objects, or gRPC input streams.
// Outputs can be ordinary objects, channels of objects, or gRPC output streams.
func minimalPipelineStage(ctx context.Context) (<-chan error, error) {
	errc := make(chan error, 1)
	go func() {
		defer close(errc)
		// Do something useful here.
	}()
	return errc, nil
}

func lineListSource(ctx context.Context, lines ...string) (<-chan string, <-chan error, error) {
	if len(lines) == 0 {
		// Handle an error that occurs before the goroutine begins.
		return nil, nil, errors.Errorf("no lines provided")
	}
	out := make(chan string)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)
		for lineIndex, line := range lines {
			if line == "" {
				// Handle an error that occurs during the goroutine.
				errc <- errors.Errorf("line %v is empty", lineIndex+1)
				return
			}
			// Send the data to the output channel but return early
			// if the context has been cancelled.
			select {
			case out <- line:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, errc, nil
}

func lineParser(ctx context.Context, base int, in <-chan string) (
	<-chan int64, <-chan error, error) {
	if base < 2 {
		// Handle an error that occurs before the goroutine begins.
		return nil, nil, errors.Errorf("invalid base %v", base)
	}
	out := make(chan int64)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)
		for line := range in {
			n, err := strconv.ParseInt(line, base, 64)
			if err != nil {
				// Handle an error that occurs during the goroutine.
				errc <- err
				return
			}
			// Send the data to the output channel but return early
			// if the context has been cancelled.
			select {
			case out <- n:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, errc, nil
}

func splitter(ctx context.Context, in <-chan int64) (
	<-chan int64, <-chan int64, <-chan error, error) {
	out1 := make(chan int64)
	out2 := make(chan int64)
	errc := make(chan error, 1)
	go func() {
		defer close(out1)
		defer close(out2)
		defer close(errc)
		for n := range in {
			// Send the data to the output channel 1 but return early
			// if the context has been cancelled.
			select {
			case out1 <- n:
			case <-ctx.Done():
				return
			}
			// Send the data to the output channel 2 but return early
			// if the context has been cancelled.
			select {
			case out2 <- n:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out1, out2, errc, nil
}

func squarer(ctx context.Context, in <-chan int64) (<-chan int64, <-chan error, error) {
	out := make(chan int64)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)
		for n := range in {
			// Send the data to the output channel but return early
			// if the context has been cancelled.
			select {
			case out <- n * n:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, errc, nil
}

func sink(ctx context.Context, in <-chan int64) (
	<-chan error, error) {
	errc := make(chan error, 1)
	go func() {
		defer close(errc)
		for n := range in {
			if n >= 100 {
				// Handle an error that occurs during the goroutine.
				errc <- errors.Errorf("number %v is too large", n)
				return
			}
			fmt.Printf("sink: %v\n", n)
		}
	}()
	return errc, nil
}

func runSimplePipeline(base int, lines []string) error {
	fmt.Printf("runSimplePipeline: base=%v, lines=%v\n", base, lines)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	var errcList []<-chan error

	// Source pipeline stage.
	linec, errc, err := lineListSource(ctx, lines...)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	// Transformer pipeline stage.
	numberc, errc, err := lineParser(ctx, base, linec)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	// Sink pipeline stage.
	errc, err = sink(ctx, numberc)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	fmt.Println("Pipeline started. Waiting for pipeline to complete.")

	return WaitForPipeline(errcList...)
}

func runComplexPipeline(base int, lines []string) error {
	fmt.Printf("runComplexPipeline: base=%v, lines=%v\n", base, lines)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	var errcList []<-chan error

	// Source pipeline stage.
	linec, errc, err := lineListSource(ctx, lines...)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	// Transformer pipeline stage 1.
	numberc, errc, err := lineParser(ctx, base, linec)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	// Transformer pipeline stage 2.
	numberc1, numberc2, errc, err := splitter(ctx, numberc)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	// Transformer pipeline stage 3.
	numberc3, errc, err := squarer(ctx, numberc1)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	// Sink pipeline stage 1.
	errc, err = sink(ctx, numberc3)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	// Sink pipeline stage 2.
	errc, err = sink(ctx, numberc2)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	fmt.Println("Pipeline started. Waiting for pipeline to complete.")

	return WaitForPipeline(errcList...)
}

func randomNumberSource(ctx context.Context, seed int64) (<-chan string, <-chan error, error) {
	out := make(chan string)
	errc := make(chan error, 1)
	random := rand.New(rand.NewSource(seed))
	go func() {
		defer close(out)
		defer close(errc)
		for {
			n := random.Intn(100)
			line := fmt.Sprintf("%v", n)
			// Send the data to the output channel but return if the context has been cancelled.
			select {
			case out <- line:
			case <-ctx.Done():
				return
			}
			time.Sleep(1 * time.Second)
		}
	}()
	return out, errc, nil
}

func runPipelineWithTimeout() error {
	fmt.Printf("runPipelineWithTimeout\n")

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	var errcList []<-chan error

	// Source pipeline stage.
	linec, errc, err := randomNumberSource(ctx, 3)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	// Transformer pipeline stage.
	numberc, errc, err := lineParser(ctx, 10, linec)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	// Sink pipeline stage.
	errc, err = sink(ctx, numberc)
	if err != nil {
		return err
	}
	errcList = append(errcList, errc)

	fmt.Println("Pipeline started. Waiting for pipeline to complete.")

	// Start a goroutine that will cancel this pipeline in 10 seconds.
	go func() {
		time.Sleep(10 * time.Second)
		fmt.Println("Cancelling context.")
		cancelFunc()
	}()

	return WaitForPipeline(errcList...)
}

func main() {
	// if err := runSimplePipeline(10, []string{"3", "2", "1"}); err != nil {
	// 	fmt.Println(err)
	// }
	// if err := runSimplePipeline(1, []string{"3", "2", "1"}); err != nil {
	// 	fmt.Println(err)
	// }
	// if err := runSimplePipeline(2, []string{"1010", "1100", "1000"}); err != nil {
	// 	fmt.Println(err)
	// }
	// if err := runSimplePipeline(2, []string{"1010", "1100", "2000", "1111"}); err != nil {
	// 	fmt.Println(err)
	// }
	// if err := runSimplePipeline(10, []string{"1", "10", "100", "1000"}); err != nil {
	// 	fmt.Println(err)
	// }
	if err := runComplexPipeline(10, []string{"5", "4", "3"}); err != nil {
		fmt.Println(err)
	}
	// if err := runPipelineWithTimeout(); err != nil {
	// 	fmt.Println(err)
	// }
}
