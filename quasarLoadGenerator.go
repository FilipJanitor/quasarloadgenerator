package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BTrDB/btrdb"
	cparse "github.com/SoftwareDefinedBuildings/sync2_quasar/configparser"
	"github.com/pborman/uuid"
)

var (
	TOTAL_RECORDS           int64
	TCP_CONNECTIONS         int
	POINTS_PER_MESSAGE      uint32
	NANOS_BETWEEN_POINTS    int64
	NUM_SERVERS             int
	NUM_STREAMS             int
	FIRST_TIME              int64
	RAND_SEED               int64
	PERM_SEED               int64
	MAX_TIME_RANDOM_OFFSET  float64
	DETERMINISTIC_KV        bool
	GET_MESSAGE_TIMES       bool
	MAX_CONCURRENT_MESSAGES uint64
	STATISTICAL_PW          uint8

	orderBitlength          uint
	orderBitmask            uint64
	statistical             bool
	statisticalBitmaskLower int64
	statisticalBitmaskUpper int64
)

var (
	VERIFY_RESPONSES = false
	PRINT_ALL        = false
)

var points_sent uint32 = 0

var points_received uint32 = 0

var points_verified uint32 = 0

type TransactionData struct {
	sendTime int64
	respTime int64
}

type ConnectionID struct {
	serverIndex     int
	connectionIndex int
}

type ConnStream struct {
	conn   *btrdb.BTrDB
	stream *btrdb.Stream
}

var get_time_value func(int64, *rand.Rand) float64

func getRandValue(time int64, randGen *rand.Rand) float64 {
	// We technically don't need time anymore, but if we switch back to a sine wave later it's useful to keep it around as a parameter
	return randGen.NormFloat64()
}

var sines [100]float64

var sinesIndex = 100

func getSinusoidValue(time int64, randGen *rand.Rand) float64 {
	sinesIndex = (sinesIndex + 1) % 100
	return sines[sinesIndex]
}

func insert_data(uuid []byte, start *int64, s *btrdb.Stream,
	sendLock *sync.Mutex, connID ConnectionID, response chan ConnectionID,
	streamID int, cont chan uint32, randGen *rand.Rand,
	permutation []int64, numMessages uint64, history []TransactionData) {
	var currTime int64 = *start
	var j uint64

	for j = 0; j < numMessages; j++ {
		currTime = permutation[j]

		data := make([]btrdb.RawPoint, POINTS_PER_MESSAGE)
		var i int
		for i = 0; uint32(i) < POINTS_PER_MESSAGE; i++ {
			if DETERMINISTIC_KV {
				data[i] = btrdb.RawPoint{Time: currTime, Value: get_time_value(currTime, randGen)}
			} else {
				data[i] = btrdb.RawPoint{Time: (currTime + int64(randGen.Float64()*MAX_TIME_RANDOM_OFFSET)), Value: get_time_value(currTime, randGen)}
			}
			currTime += NANOS_BETWEEN_POINTS
		}

		sendLock.Lock()
		sendErr := s.Insert(context.Background(), data)
		sendLock.Unlock()
		if GET_MESSAGE_TIMES { // write send time to history
			history[j].sendTime = time.Now().UnixNano()
		}

		if sendErr != nil {
			fmt.Printf("Error in sending request: %v\n", sendErr)
			return
		}
		atomic.AddUint32(&points_sent, POINTS_PER_MESSAGE)
	}
	response <- connID
}

func getIntFromConfig(key string, config map[string]interface{}) int64 {
	elem, ok := config[key]
	if !ok {
		fmt.Printf("Could not read %v from config file\n", key)
		os.Exit(1)
	}
	intval, err := strconv.ParseInt(elem.(string), 0, 64)
	if err != nil {
		fmt.Printf("Could not parse %v to an int64: %v\n", elem, err)
		os.Exit(1)
	}
	return intval
}

func getServer(uuid []byte) int {
	return int(uint(uuid[0]) % uint(NUM_SERVERS))
}

func bitLength(x int64) uint {
	var times uint = 0
	for x != 0 {
		x >>= 1
		times++
	}
	return times
}

func main() {
	args := os.Args[1:]
	var send_messages func([]byte, *int64, *btrdb.Stream, *sync.Mutex, ConnectionID, chan ConnectionID, int, chan uint32, *rand.Rand, []int64, uint64, []TransactionData)

	if len(args) > 0 && args[0] == "-i" {
		fmt.Println("Insert mode")
		send_messages = insert_data
	} else {
		fmt.Println("Usage: use -i to insert data. To get a CPU profile, add a file name after -i.")
		return
	}

	/* Check if the user has requested a CPU Profile. */
	if len(args) > 1 {
		f, err := os.Create(args[1])
		if err != nil {
			fmt.Println(err)
			return
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	/* Read the configuration file. */

	configfile, err := ioutil.ReadFile("loadConfig.ini")
	if err != nil {
		fmt.Printf("Could not read loadConfig.ini: %v\n", err)
		return
	}

	config, isErr := cparse.ParseConfig(string(configfile))
	if isErr {
		fmt.Println("There were errors while parsing loadConfig.ini. See above.")
		return
	}

	TOTAL_RECORDS = getIntFromConfig("TOTAL_RECORDS", config)
	TCP_CONNECTIONS = int(getIntFromConfig("TCP_CONNECTIONS", config))
	POINTS_PER_MESSAGE = uint32(getIntFromConfig("POINTS_PER_MESSAGE", config))
	NANOS_BETWEEN_POINTS = getIntFromConfig("NANOS_BETWEEN_POINTS", config)
	NUM_SERVERS = int(getIntFromConfig("NUM_SERVERS", config))
	NUM_STREAMS = int(getIntFromConfig("NUM_STREAMS", config))
	FIRST_TIME = getIntFromConfig("FIRST_TIME", config)
	RAND_SEED = getIntFromConfig("RAND_SEED", config)
	PERM_SEED = getIntFromConfig("PERM_SEED", config)
	var maxConcurrentMessages int64 = getIntFromConfig("MAX_CONCURRENT_MESSAGES", config)
	var timeRandOffset int64 = getIntFromConfig("MAX_TIME_RANDOM_OFFSET", config)
	var pw int64 = getIntFromConfig("STATISTICAL_PW", config)
	if TOTAL_RECORDS <= 0 || TCP_CONNECTIONS <= 0 || POINTS_PER_MESSAGE <= 0 || NANOS_BETWEEN_POINTS <= 0 || NUM_STREAMS <= 0 || maxConcurrentMessages <= 0 {
		fmt.Println("TOTAL_RECORDS, TCP_CONNECTIONS, POINTS_PER_MESSAGE, NANOS_BETWEEN_POINTS, NUM_STREAMS, and MAX_CONCURRENT_MESSAGES must be positive.")
		os.Exit(1)
	}
	if pw < -1 {
		fmt.Println("STATISTICAL_PW cannot be less than -1.")
		os.Exit(1)
	}
	if (TOTAL_RECORDS % int64(POINTS_PER_MESSAGE)) != 0 {
		fmt.Println("TOTAL_RECORDS must be a multiple of POINTS_PER_MESSAGE.")
		os.Exit(1)
	}
	if timeRandOffset >= NANOS_BETWEEN_POINTS {
		fmt.Println("MAX_TIME_RANDOM_OFFSET must be less than NANOS_BETWEEN_POINTS.")
		os.Exit(1)
	}
	if timeRandOffset > (1 << 53) { // must be exactly representable as a float64
		fmt.Println("MAX_TIME_RANDOM_OFFSET is too large: the maximum value is 2 ^ 53.")
		os.Exit(1)
	}
	if timeRandOffset < 0 {
		fmt.Println("MAX_TIME_RANDOM_OFFSET must be nonnegative.")
		os.Exit(1)
	}
	if VERIFY_RESPONSES && maxConcurrentMessages != 1 {
		fmt.Println("WARNING: MAX_CONCURRENT_MESSAGES is always 1 when verifying responses.")
		maxConcurrentMessages = 1
	}
	var nanosPerMessage uint64 = uint64(NANOS_BETWEEN_POINTS) * uint64(POINTS_PER_MESSAGE)
	if VERIFY_RESPONSES && statistical && ((nanosPerMessage&uint64(statisticalBitmaskLower)) != 0 || (FIRST_TIME&statisticalBitmaskLower) != 0) {
		fmt.Println("ERROR: When verifying statistical responses, NANOS_BETWEEN_POINTS * POINTS_PER_MESSAGE (the ns in each query) and FIRST_TIME must be multiples of 2 ^ STATISTICAL_PW.")
		return
	}
	MAX_CONCURRENT_MESSAGES = uint64(maxConcurrentMessages)
	MAX_TIME_RANDOM_OFFSET = float64(timeRandOffset)
	DETERMINISTIC_KV = (config["DETERMINISTIC_KV"].(string) == "true")
	if VERIFY_RESPONSES && PERM_SEED != 0 && !DETERMINISTIC_KV {
		fmt.Println("ERROR: PERM_SEED must be set to 0 when verifying nondeterministic responses.")
		return
	}
	GET_MESSAGE_TIMES = (config["GET_MESSAGE_TIMES"].(string) == "true")
	if DETERMINISTIC_KV {
		get_time_value = getSinusoidValue
		for r := 0; r < 100; r++ {
			sines[r] = math.Sin(2 * math.Pi * float64(r) / 100)
		}
	} else {
		get_time_value = getRandValue
	}

	var remainder int64 = 0
	if TOTAL_RECORDS%int64(POINTS_PER_MESSAGE) != 0 {
		remainder = 1
	}
	var perm_size = (TOTAL_RECORDS / int64(POINTS_PER_MESSAGE)) + remainder
	orderBitlength = bitLength(perm_size - 1)
	if orderBitlength+bitLength(int64(NUM_STREAMS-1)) > 64 {
		fmt.Println("The number of bits required to store (number of messages - 1) plus the number of bits required to store (NUM_STREAMS - 1) cannot exceed 64.")
		os.Exit(1)
	}
	orderBitmask = (1 << orderBitlength) - 1

	var seedGen *rand.Rand = rand.New(rand.NewSource(RAND_SEED))
	var permGen *rand.Rand = rand.New(rand.NewSource(PERM_SEED))
	var randGens []*rand.Rand = make([]*rand.Rand, NUM_STREAMS)

	var j int
	var ok bool
	var dbAddrStr interface{}
	var dbAddrs []string = make([]string, NUM_SERVERS)
	for j = 0; j < NUM_SERVERS; j++ {
		dbAddrStr, ok = config[fmt.Sprintf("DB_ADDR%v", j+1)]
		if !ok {
			break
		}
		dbAddrs[j] = dbAddrStr.(string)
	}
	_, ok = config[fmt.Sprintf("DB_ADDR%v", j+1)]
	if j != NUM_SERVERS || ok {
		fmt.Println("The number of specified DB_ADDRs must equal NUM_SERVERS.")
		os.Exit(1)
	}

	var uuids [][]byte = make([][]byte, NUM_STREAMS)

	var uuidStr interface{}
	var uuidParsed uuid.UUID
	for j = 0; j < NUM_STREAMS; j++ {
		uuidStr, ok = config[fmt.Sprintf("UUID%v", j+1)]
		if !ok {
			break
		}
		uuidParsed = uuid.Parse(uuidStr.(string))
		if uuidParsed == nil {
			fmt.Printf("Invalid UUID %v\n", uuidStr)
			os.Exit(1)
		}
		uuids[j] = []byte(uuidParsed)
	}
	_, ok = config[fmt.Sprintf("UUID%v", j+1)]
	if j != NUM_STREAMS || ok {
		fmt.Println("The number of specified UUIDs must equal NUM_STREAMS.")
		os.Exit(1)
	}
	fmt.Printf("Using UUIDs ")
	for j = 0; j < NUM_STREAMS; j++ {
		fmt.Printf("%s ", uuid.UUID(uuids[j]).String())
	}
	fmt.Printf("\n")

	runtime.GOMAXPROCS(runtime.NumCPU())
	var connections [][]ConnStream = make([][]ConnStream, NUM_SERVERS)
	var sendLocks [][]*sync.Mutex = make([][]*sync.Mutex, NUM_SERVERS)
	var recvLocks [][]*sync.Mutex = make([][]*sync.Mutex, NUM_SERVERS)

	d, err := btrdb.Connect(context.TODO(), dbAddrs[0])
	if err != nil {
		fmt.Printf("Could not connect to database: %s\n", err)
		os.Exit(1)
	}
	for j = 0; j < NUM_STREAMS; j++ {
		_, err := d.Create(context.Background(), uuid.UUID(uuids[j]), uuid.UUID(uuids[j]).String(), nil, nil)
		if err != nil {
			fmt.Printf("Could not create stream: %s\n", err)
			os.Exit(1)

		}
	}

	for s := range dbAddrs {
		connections[s] = make([]ConnStream, TCP_CONNECTIONS)
		sendLocks[s] = make([]*sync.Mutex, TCP_CONNECTIONS)
		recvLocks[s] = make([]*sync.Mutex, TCP_CONNECTIONS)
		for i := range connections[s] {
			db, err := btrdb.Connect(context.TODO(), dbAddrs[s])
			if err != nil {
				fmt.Printf("Could not connect to database: %s\n", err)
				os.Exit(1)
			}
			fmt.Printf("Creating connections to %v...\n", dbAddrs[s])
			connections[s][i] = ConnStream{conn: db, stream: db.StreamFromUUID(uuid.UUID(uuids[i%NUM_STREAMS]))}
			sendLocks[s][i] = &sync.Mutex{}
			recvLocks[s][i] = &sync.Mutex{}

		}
	}
	fmt.Println("Finished creating connections")

	var serverIndex int = 0
	var streamCounts []int = make([]int, NUM_SERVERS)
	var connIndex int

	var sig chan ConnectionID = make(chan ConnectionID)
	var usingConn [][]int = make([][]int, NUM_SERVERS)
	for y := 0; y < NUM_SERVERS; y++ {
		usingConn[y] = make([]int, TCP_CONNECTIONS)
	}
	var idToChannel []chan uint32 = make([]chan uint32, NUM_STREAMS)
	var cont chan uint32
	var randGen *rand.Rand
	var startTimes []int64 = make([]int64, NUM_STREAMS)
	var perm [][]int64 = make([][]int64, NUM_STREAMS)

	var transactionHistories [][]TransactionData = make([][]TransactionData, NUM_STREAMS)
	for p := range transactionHistories {
		if GET_MESSAGE_TIMES {
			transactionHistories[p] = make([]TransactionData, perm_size)
		} else {
			transactionHistories[p] = nil
		}
	}

	var f int64
	for e := 0; e < NUM_STREAMS; e++ {
		perm[e] = make([]int64, perm_size)
		if PERM_SEED == 0 {
			for f = 0; f < perm_size; f++ {
				perm[e][f] = FIRST_TIME + NANOS_BETWEEN_POINTS*int64(POINTS_PER_MESSAGE)*f
			}
		} else {
			x := permGen.Perm(int(perm_size))
			for f = 0; f < perm_size; f++ {
				perm[e][f] = FIRST_TIME + NANOS_BETWEEN_POINTS*int64(POINTS_PER_MESSAGE)*int64(x[f])
			}
		}
	}
	fmt.Println("Finished generating insert/query order")

	var finished bool = false

	var startTime int64 = time.Now().UnixNano()

	for z := 0; z < NUM_STREAMS; z++ {
		cont = make(chan uint32, maxConcurrentMessages)
		idToChannel[z] = cont
		randGen = rand.New(rand.NewSource(seedGen.Int63()))
		randGens[z] = randGen
		startTimes[z] = FIRST_TIME
		serverIndex = getServer(uuids[z])
		connIndex = streamCounts[serverIndex] % TCP_CONNECTIONS
		go send_messages(uuids[z], &startTimes[z], connections[serverIndex][connIndex].stream, sendLocks[serverIndex][connIndex], ConnectionID{serverIndex, connIndex}, sig, z, cont, randGen, perm[z], uint64(perm_size), transactionHistories[z])
		usingConn[serverIndex][connIndex]++
		streamCounts[serverIndex]++
	}

	/* Handle ^C */
	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt // block until an interrupt happens
		fmt.Println("\nDetected ^C. Abruptly ending program...")
		fmt.Println("The following are the start times of the messages that are currently being inserted/queried:")
		for i := 0; i < NUM_STREAMS; i++ {
			fmt.Printf("%v: %v\n", uuid.UUID(uuids[i]).String(), startTimes[i])
		}
		os.Exit(0)
	}()

	go func() {
		for !finished {
			time.Sleep(time.Second)
			fmt.Printf("Sent %v, ", points_sent)
			atomic.StoreUint32(&points_sent, 0)
			points_received = 0
		}
	}()

	var response ConnectionID
	for k := 0; k < NUM_STREAMS; k++ {
		response = <-sig
		serverIndex = response.serverIndex
		connIndex = response.connectionIndex
		usingConn[serverIndex][connIndex]--
		if usingConn[serverIndex][connIndex] == 0 {
			connections[serverIndex][connIndex].conn.Disconnect()
			fmt.Printf("Closed connection %v to server %v\n", connIndex, dbAddrs[serverIndex])
		}
	}

	var deltaT int64 = time.Now().UnixNano() - startTime

	// I used to close unused connections here, but now I don't bother

	finished = true
	fmt.Printf("Sent %v, Received %v\n", points_sent, points_received)

	fmt.Println("Finished")

	var numResPoints uint64 = uint64(TOTAL_RECORDS) * uint64(NUM_STREAMS)
	fmt.Printf("Total time: %d nanoseconds for %d points\n", deltaT, numResPoints)
	var average uint64 = 0
	if numResPoints != 0 {
		average = uint64(deltaT) / numResPoints
	}
	fmt.Printf("Average: %d nanoseconds per point (floored to integer value)\n", average)
	fmt.Println(deltaT)

	if GET_MESSAGE_TIMES {
		file, err := os.Create("stats.json")
		if err != nil {
			fmt.Println("Could not write stats to file")
			os.Exit(1)
		}
		writeSafe(file, "{\n")
		for q := range transactionHistories {
			writeSafe(file, fmt.Sprintf("\"%v\": [\n", uuid.UUID(uuids[q]).String()))
			terminator := ","
			for r := range transactionHistories[q] {
				if r == len(transactionHistories[q])-1 {
					terminator = ""
				}
				writeSafe(file, fmt.Sprintf("[%v,%v]%s\n", transactionHistories[q][r].sendTime, transactionHistories[q][r].respTime, terminator))
			}
			if q == len(transactionHistories)-1 {
				writeSafe(file, "]\n")
			} else {
				writeSafe(file, "],\n")
			}
		}
		writeSafe(file, "}\n")
	}
}

func writeSafe(file *os.File, str string) {
	written, err := io.WriteString(file, str)
	if written != len(str) || err != nil {
		fmt.Println("Could not write to file")
		os.Exit(1)
	}
}
