package lxmf

import (
	crypto_rand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"runtime"
	"sync"
	"time"

	"github.com/svanichkin/go-reticulum/rns"
	"github.com/svanichkin/go-reticulum/rns/cryptography"
	umsgpack "github.com/svanichkin/go-reticulum/rns/vendor"
)

const (
	WorkblockExpandRounds        = 3000
	WorkblockExpandRoundsPN      = 1000
	WorkblockExpandRoundsPeering = 25
	StampSize                    = rns.HashLengthBytes
	PNValidationPoolMinSize      = 256
)

type stampJob struct {
	cancel     chan struct{}
	result     chan []byte
	cancelOnce sync.Once
	resultOnce sync.Once
}

var (
	activeJobsMu sync.Mutex
	activeJobs   = map[string]*stampJob{}
)

func StampWorkblock(material []byte, expandRounds int) []byte {
	if expandRounds <= 0 {
		return []byte{}
	}

	workblock := make([]byte, 0, expandRounds*256)
	for n := 0; n < expandRounds; n++ {
		nonce, err := umsgpack.Packb(n)
		if err != nil {
			continue
		}
		salt := rns.FullHash(append(append([]byte{}, material...), nonce...))
		block, err := cryptography.HKDF(256, material, salt, nil)
		if err != nil {
			continue
		}
		workblock = append(workblock, block...)
	}
	return workblock
}

func StampValue(workblock, stamp []byte) int {
	value := 0
	bits := 256
	material := rns.FullHash(append(append([]byte{}, workblock...), stamp...))
	i := new(big.Int).SetBytes(material)
	for i.Bit(bits-1) == 0 {
		i.Lsh(i, 1)
		value++
	}
	return value
}

func StampValid(stamp []byte, targetCost int, workblock []byte) bool {
	result := rns.FullHash(append(append([]byte{}, workblock...), stamp...))
	limit := new(big.Int).Lsh(big.NewInt(1), uint(256-targetCost))
	res := new(big.Int).SetBytes(result)
	return res.Cmp(limit) <= 0
}

func ValidatePeeringKey(peeringID, peeringKey []byte, targetCost int) bool {
	workblock := StampWorkblock(peeringID, WorkblockExpandRoundsPeering)
	return StampValid(peeringKey, targetCost, workblock)
}

func ValidatePNStamp(transientData []byte, targetCost int) ([]byte, []byte, int, []byte) {
	if len(transientData) <= LXMFOverhead+StampSize {
		return nil, nil, 0, nil
	}
	lxmData := transientData[:len(transientData)-StampSize]
	stamp := transientData[len(transientData)-StampSize:]
	transientID := rns.FullHash(lxmData)
	workblock := StampWorkblock(transientID, WorkblockExpandRoundsPN)
	if !StampValid(stamp, targetCost, workblock) {
		return nil, nil, 0, nil
	}
	value := StampValue(workblock, stamp)
	return transientID, lxmData, value, stamp
}

func ValidatePNStampsJobSimple(transientList [][]byte, targetCost int) [][]any {
	validated := make([][]any, 0)
	for _, transientData := range transientList {
		transientID, lxmData, value, stampData := ValidatePNStamp(transientData, targetCost)
		if transientID != nil {
			validated = append(validated, []any{transientID, lxmData, value, stampData})
		}
	}
	return validated
}

func ValidatePNStampsJobMultip(transientList [][]byte, targetCost int) [][]any {
	cores := runtime.NumCPU()
	poolCount := int(math.Min(float64(cores), math.Ceil(float64(len(transientList))/PNValidationPoolMinSize)))
	if poolCount <= 0 {
		poolCount = 1
	}

	rns.Log(fmt.Sprintf("Validating %d stamps using %d workers...", len(transientList), poolCount), rns.LOG_VERBOSE)

	type stampJobItem struct {
		index int
		data  []byte
	}
	type stampJobResult struct {
		index int
		entry []any
	}

	jobs := make(chan stampJobItem)
	results := make(chan stampJobResult, poolCount)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for data := range jobs {
			transientID, lxmData, value, stampData := ValidatePNStamp(data.data, targetCost)
			if transientID != nil {
				results <- stampJobResult{
					index: data.index,
					entry: []any{transientID, lxmData, value, stampData},
				}
			}
		}
	}

	wg.Add(poolCount)
	for i := 0; i < poolCount; i++ {
		go worker()
	}

	go func() {
		for i, data := range transientList {
			jobs <- stampJobItem{index: i, data: data}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	validated := make([][]any, len(transientList))
	for res := range results {
		validated[res.index] = res.entry
	}

	filtered := make([][]any, 0, len(validated))
	for _, entry := range validated {
		if entry != nil {
			filtered = append(filtered, entry)
		}
	}
	rns.Log(fmt.Sprintf("Validation pool completed for %d stamps", len(transientList)), rns.LOG_VERBOSE)
	return filtered
}

func ValidatePNStamps(transientList [][]byte, targetCost int) [][]any {
	nonMPPlatform := runtime.GOOS == "android"
	if len(transientList) <= PNValidationPoolMinSize || nonMPPlatform {
		return ValidatePNStampsJobSimple(transientList, targetCost)
	}
	return ValidatePNStampsJobMultip(transientList, targetCost)
}

func GenerateStamp(messageID []byte, stampCost int, expandRounds int) ([]byte, int) {
	rns.Log(fmt.Sprintf("Generating stamp with cost %d for %s...", stampCost, rns.PrettyHexRep(messageID)), rns.LOG_DEBUG)
	workblock := StampWorkblock(messageID, expandRounds)

	start := time.Now()
	var (
		stamp  []byte
		rounds int
		value  int
	)

	switch runtime.GOOS {
	case "windows", "darwin", "android":
		if runtime.GOOS == "android" {
			stamp, rounds = jobAndroid(stampCost, workblock, messageID)
		} else {
			stamp, rounds = jobSimple(stampCost, workblock, messageID)
		}
	default:
		stamp, rounds = jobConcurrent(stampCost, workblock, messageID)
	}

	duration := time.Since(start)
	if stamp != nil {
		value = StampValue(workblock, stamp)
	}
	speed := float64(rounds) / duration.Seconds()
	rns.Log(fmt.Sprintf("Stamp with value %d generated in %s, %d rounds, %d rounds per second", value, rns.PrettyTime(duration.Seconds(), false, false), rounds, int(speed)), rns.LOG_DEBUG)

	return stamp, value
}

func CancelWork(messageID []byte) {
	key := string(messageID)
	activeJobsMu.Lock()
	job := activeJobs[key]
	if job != nil {
		job.cancelOnce.Do(func() { close(job.cancel) })
		if job.result != nil {
			job.resultOnce.Do(func() {
				select {
				case job.result <- nil:
				default:
				}
			})
		}
		delete(activeJobs, key)
	}
	activeJobsMu.Unlock()
}

func jobSimple(stampCost int, workblock []byte, messageID []byte) ([]byte, int) {
	platform := runtime.GOOS
	rns.Log(fmt.Sprintf("Running stamp generation on %s, work limited to single CPU core. This will be slower than ideal.", platform), rns.LOG_WARNING)

	rounds := 0
	pstamp := make([]byte, 256/8)
	_, _ = crypto_rand.Read(pstamp)
	start := time.Now()

	job := &stampJob{cancel: make(chan struct{})}
	key := string(messageID)
	activeJobsMu.Lock()
	activeJobs[key] = job
	activeJobsMu.Unlock()

	for !StampValid(pstamp, stampCost, workblock) {
		select {
		case <-job.cancel:
			pstamp = nil
			activeJobsMu.Lock()
			delete(activeJobs, key)
			activeJobsMu.Unlock()
			return nil, rounds
		default:
		}

		_, _ = crypto_rand.Read(pstamp)
		rounds++
		if rounds%2500 == 0 {
			speed := float64(rounds) / time.Since(start).Seconds()
			rns.Log(fmt.Sprintf("Stamp generation running. %d rounds completed so far, %d rounds per second", rounds, int(speed)), rns.LOG_DEBUG)
		}
	}

	activeJobsMu.Lock()
	delete(activeJobs, key)
	activeJobsMu.Unlock()

	return pstamp, rounds
}

func jobConcurrent(stampCost int, workblock []byte, messageID []byte) ([]byte, int) {
	var stamp []byte
	totalRounds := 0
	start := time.Now()
	cores := runtime.NumCPU()
	jobs := cores
	if jobs > 12 {
		jobs = cores / 2
	}
	if jobs < 1 {
		jobs = 1
	}

	job := &stampJob{
		cancel: make(chan struct{}),
		result: make(chan []byte, 1),
	}
	key := string(messageID)
	activeJobsMu.Lock()
	activeJobs[key] = job
	activeJobsMu.Unlock()

	roundsCh := make(chan int, jobs)
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		rounds := 0
		pstamp := make([]byte, 256/8)
		_, _ = crypto_rand.Read(pstamp)

		for {
			select {
			case <-job.cancel:
				roundsCh <- rounds
				return
			default:
			}
			if StampValid(pstamp, stampCost, workblock) {
				job.resultOnce.Do(func() {
					job.result <- append([]byte{}, pstamp...)
				})
				job.cancelOnce.Do(func() { close(job.cancel) })
				roundsCh <- rounds
				return
			}

			_, _ = crypto_rand.Read(pstamp)
			rounds++
			if rounds%2500 == 0 {
				speed := float64(rounds) / time.Since(start).Seconds()
				rns.Log(fmt.Sprintf("Stamp generation running. %d rounds completed so far, %d rounds per second", rounds, int(speed)), rns.LOG_DEBUG)
			}
		}
	}

	rns.Log(fmt.Sprintf("Starting %d stamp generation workers", jobs), rns.LOG_DEBUG)
	wg.Add(jobs)
	for i := 0; i < jobs; i++ {
		go worker()
	}

	stamp = <-job.result
	wg.Wait()
	close(roundsCh)
	for rounds := range roundsCh {
		totalRounds += rounds
	}

	activeJobsMu.Lock()
	delete(activeJobs, key)
	activeJobsMu.Unlock()

	return stamp, totalRounds
}

func jobAndroid(stampCost int, workblock []byte, messageID []byte) ([]byte, int) {
	stamp := []byte(nil)
	start := time.Now()
	totalRounds := 0
	roundsPerWorker := 1000
	jobs := runtime.NumCPU()
	if jobs < 1 {
		jobs = 1
	}

	job := &stampJob{cancel: make(chan struct{})}
	key := string(messageID)
	activeJobsMu.Lock()
	activeJobs[key] = job
	activeJobsMu.Unlock()

	rns.Log(fmt.Sprintf("Dispatching %d workers for stamp generation...", jobs), rns.LOG_DEBUG)

	for stamp == nil {
		canceled := false
		select {
		case <-job.cancel:
			canceled = true
		default:
		}
		if canceled {
			break
		}

		type workerResult struct {
			index  int
			stamp  []byte
			rounds int
		}

		results := make(chan workerResult, jobs)
		var wg sync.WaitGroup

		worker := func(procnum int) {
			defer wg.Done()
			rounds := 0
			foundStamp := []byte(nil)

			for {
				canceled := false
				select {
				case <-job.cancel:
					canceled = true
				default:
				}
				if canceled {
					break
				}

				pstamp := make([]byte, 256/8)
				_, _ = crypto_rand.Read(pstamp)
				rounds++

				if StampValid(pstamp, stampCost, workblock) {
					foundStamp = append([]byte{}, pstamp...)
					break
				}

				if rounds >= roundsPerWorker {
					break
				}
			}

			results <- workerResult{index: procnum, stamp: foundStamp, rounds: rounds}
		}

		wg.Add(jobs)
		for i := 0; i < jobs; i++ {
			go worker(i)
		}

		go func() {
			wg.Wait()
			close(results)
		}()

		for res := range results {
			totalRounds += res.rounds
			if res.stamp != nil {
				stamp = res.stamp
			}
		}

		if stamp == nil {
			canceled := false
			select {
			case <-job.cancel:
				canceled = true
			default:
			}
			if canceled {
				break
			}
			elapsed := time.Since(start).Seconds()
			if elapsed > 0 {
				speed := float64(totalRounds) / elapsed
				rns.Log(fmt.Sprintf("Stamp generation running. %d rounds completed so far, %d rounds per second", totalRounds, int(speed)), rns.LOG_DEBUG)
			}
		}
	}

	activeJobsMu.Lock()
	delete(activeJobs, key)
	activeJobsMu.Unlock()

	if job != nil {
		select {
		case <-job.cancel:
			if stamp == nil {
				return nil, totalRounds
			}
		default:
		}
	}
	return stamp, totalRounds
}
