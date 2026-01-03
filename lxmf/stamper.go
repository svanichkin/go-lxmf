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
	cancelOnce sync.Once
}

var (
	activeJobsMu sync.Mutex
	activeJobs   = map[string]*stampJob{}
)

func StampWorkblock(material []byte, expandRounds int) []byte {
	if expandRounds <= 0 {
		expandRounds = WorkblockExpandRounds
	}

	start := time.Now()
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
	_ = start

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
	if targetCost <= 0 {
		return true
	}
	if targetCost >= 256 {
		return false
	}
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

	jobs := make(chan []byte)
	results := make(chan []any)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for data := range jobs {
			transientID, lxmData, value, stampData := ValidatePNStamp(data, targetCost)
			if transientID != nil {
				results <- []any{transientID, lxmData, value, stampData}
			}
		}
	}

	wg.Add(poolCount)
	for i := 0; i < poolCount; i++ {
		go worker()
	}

	go func() {
		for _, data := range transientList {
			jobs <- data
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	validated := make([][]any, 0)
	for res := range results {
		validated = append(validated, []any{res[0], res[1], res[2], res[3]})
	}
	return validated
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
	if duration > 0 {
		speed := float64(rounds) / duration.Seconds()
		rns.Log(fmt.Sprintf("Stamp with value %d generated in %s, %d rounds, %d rounds per second", value, rns.PrettyTime(duration.Seconds(), false, false), rounds, int(speed)), rns.LOG_DEBUG)
	}

	return stamp, value
}

func CancelWork(messageID []byte) {
	key := string(messageID)
	activeJobsMu.Lock()
	job := activeJobs[key]
	if job != nil {
		job.cancelOnce.Do(func() { close(job.cancel) })
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
	stamp := make([]byte, 0)
	totalRounds := 0
	cores := runtime.NumCPU()
	jobs := cores
	if jobs > 12 {
		jobs = int(math.Ceil(float64(cores) / 2))
	}
	if jobs < 1 {
		jobs = 1
	}

	job := &stampJob{cancel: make(chan struct{})}
	key := string(messageID)
	activeJobsMu.Lock()
	activeJobs[key] = job
	activeJobsMu.Unlock()

	resultCh := make(chan []byte, 1)
	roundsCh := make(chan int, jobs)
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		rounds := 0
		pstamp := make([]byte, 256/8)
		_, _ = crypto_rand.Read(pstamp)

		for !StampValid(pstamp, stampCost, workblock) {
			select {
			case <-job.cancel:
				roundsCh <- rounds
				return
			default:
			}
			_, _ = crypto_rand.Read(pstamp)
			rounds++
		}

		select {
		case resultCh <- pstamp:
			job.cancelOnce.Do(func() { close(job.cancel) })
		default:
		}
		roundsCh <- rounds
	}

	rns.Log(fmt.Sprintf("Starting %d stamp generation workers", jobs), rns.LOG_DEBUG)
	wg.Add(jobs)
	for i := 0; i < jobs; i++ {
		go worker()
	}

	select {
	case stamp = <-resultCh:
	case <-job.cancel:
		stamp = nil
	}
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

	for stamp == nil {
		var (
			wg     sync.WaitGroup
			mu     sync.Mutex
			found  []byte
			rounds int
		)
		wg.Add(jobs)
		for i := 0; i < jobs; i++ {
			go func() {
				defer wg.Done()
				pstamp := make([]byte, 256/8)
				for n := 0; n < roundsPerWorker; n++ {
					select {
					case <-job.cancel:
						return
					default:
					}
					_, _ = crypto_rand.Read(pstamp)
					mu.Lock()
					rounds++
					mu.Unlock()
					if StampValid(pstamp, stampCost, workblock) {
						mu.Lock()
						if found == nil {
							found = append([]byte{}, pstamp...)
						}
						mu.Unlock()
						return
					}
				}
			}()
		}

		wg.Wait()
		totalRounds += rounds

		select {
		case <-job.cancel:
			activeJobsMu.Lock()
			delete(activeJobs, key)
			activeJobsMu.Unlock()
			return nil, totalRounds
		default:
		}

		if found != nil {
			stamp = found
			break
		}
		elapsed := time.Since(start).Seconds()
		if elapsed > 0 {
			speed := float64(totalRounds) / elapsed
			rns.Log(fmt.Sprintf("Stamp generation running. %d rounds completed so far, %d rounds per second", totalRounds, int(speed)), rns.LOG_DEBUG)
		}
	}

	activeJobsMu.Lock()
	delete(activeJobs, key)
	activeJobsMu.Unlock()

	return stamp, totalRounds
}
