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
	"time"

	pb "github.com/BTrDB/btrdb/grpcinterface"
	cparse "github.com/SoftwareDefinedBuildings/sync2_quasar/configparser"
	"github.com/pborman/uuid"
	"google.golang.org/grpc"
)

// remember to add EXTERNAL_ADDRESS to apifrontend as env variable to avoid calling localhost
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

type Endpoint struct {
	g    pb.BTrDBClient
	conn *grpc.ClientConn
}

//Error() implements the error interface
func (ce *CodedError) Error() string {
	return fmt.Sprintf("[%d] %s", ce.Code, ce.Msg)
}

//ToCodedError can be used to convert any error into a CodedError. If the
//error object is actually not coded, it will receive code 501.
func ToCodedError(e error) *CodedError {
	ce, ok := e.(*CodedError)
	if ok {
		return ce
	}
	s := pb.Status{Code: 501, Msg: e.Error()}
	return &CodedError{&s}
}

var points_sent uint32 = 0

var get_time_value func(int64, *rand.Rand) float64

func getRandValue(time int64, randGen *rand.Rand) float64 {
	// We technically don't need time anymore, but if we switch back to a sine wave later it's useful to keep it around as a parameter
	return randGen.NormFloat64()
}

type CodedError struct {
	*pb.Status
}

var sines [100]float64

var sinesIndex = 100

func getSinusoidValue(time int64, randGen *rand.Rand) float64 {
	sinesIndex = (sinesIndex + 1) % 100
	return sines[sinesIndex]
}

func insert_data(datas [][]*pb.RawPoint, sig chan int, c pb.BTrDBClient, uu uuid.UUID) {
	for _, d := range datas {
		sendErr := inserter(c, uu, d)
		if sendErr != nil {
			fmt.Printf("Error in sending request: %v\n", sendErr)
			os.Exit(1)
		}
	}
	sig <- 0
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

	if len(args) > 0 && args[0] == "-i" {
		fmt.Println("Insert mode")
	} else {
		fmt.Println("Usage: use -i to insert data. To get a CPU profile, add a file name after -i.")
		return
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
	POINTS_PER_MESSAGE = uint32(getIntFromConfig("POINTS_PER_MESSAGE", config))
	NANOS_BETWEEN_POINTS = getIntFromConfig("NANOS_BETWEEN_POINTS", config)
	NUM_STREAMS = int(getIntFromConfig("NUM_STREAMS", config))
	TCP_CONNECTIONS = int(getIntFromConfig("TCP_CONNECTIONS", config))
	FIRST_TIME = getIntFromConfig("FIRST_TIME", config)
	RAND_SEED = getIntFromConfig("RAND_SEED", config)
	PERM_SEED = getIntFromConfig("PERM_SEED", config)
	var maxConcurrentMessages int64 = getIntFromConfig("MAX_CONCURRENT_MESSAGES", config)
	var timeRandOffset int64 = getIntFromConfig("MAX_TIME_RANDOM_OFFSET", config)
	if TOTAL_RECORDS <= 0 || TCP_CONNECTIONS <= 0 || POINTS_PER_MESSAGE <= 0 || NANOS_BETWEEN_POINTS <= 0 || NUM_STREAMS <= 0 || maxConcurrentMessages <= 0 {
		fmt.Println("TOTAL_RECORDS, TCP_CONNECTIONS, POINTS_PER_MESSAGE, NANOS_BETWEEN_POINTS, NUM_STREAMS, and MAX_CONCURRENT_MESSAGES must be positive.")
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

	MAX_CONCURRENT_MESSAGES = uint64(maxConcurrentMessages)
	MAX_TIME_RANDOM_OFFSET = float64(timeRandOffset)
	DETERMINISTIC_KV = (config["DETERMINISTIC_KV"].(string) == "true")
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
	if perm_size%int64(TCP_CONNECTIONS) != 0 {
		fmt.Println("Number of connections needs to divide number of messages")
		os.Exit(1)
	}
	n_mes := perm_size / int64(TCP_CONNECTIONS)

	orderBitlength = bitLength(perm_size - 1)
	if orderBitlength+bitLength(int64(NUM_STREAMS-1)) > 64 {
		fmt.Println("The number of bits required to store (number of messages - 1) plus the number of bits required to store (NUM_STREAMS - 1) cannot exceed 64.")
		os.Exit(1)
	}
	orderBitmask = (1 << orderBitlength) - 1

	seedGen := rand.New(rand.NewSource(RAND_SEED))
	permGen := rand.New(rand.NewSource(PERM_SEED))

	var j int
	var ok bool
	var dbAddrStr interface{}
	dbAddrStr, ok = config["DB_ADDR"]
	if !ok {
		fmt.Println("DB_ADDR cannot be found")
		os.Exit(1)
	}
	dbAddr := dbAddrStr.(string)

	uuids := make([]uuid.UUID, NUM_STREAMS)

	for j = 0; j < NUM_STREAMS; j++ {
		uuids[j] = uuid.NewRandom()
	}

	fmt.Printf("Using UUIDs ")
	for j = 0; j < NUM_STREAMS; j++ {
		fmt.Printf("%s ", uuids[j].String())
	}
	fmt.Printf("\n")

	runtime.GOMAXPROCS(runtime.NumCPU())
	connections := make([][]*Endpoint, NUM_STREAMS)

	datas := make([][]*pb.RawPoint, uint64(perm_size))

	d := connector(dbAddr)
	for j = 0; j < NUM_STREAMS; j++ {
		rv, err := d.g.Create(context.Background(), &pb.CreateParams{
			Uuid:        uuids[j],
			Collection:  uuids[j].String(),
			Tags:        nil,
			Annotations: nil,
		})

		if err != nil {
			fmt.Printf("Could not create stream: %s\n", err)
			os.Exit(1)
		}
		if rv.GetStat() != nil {
			fmt.Printf("Could not create stream: %s\n", &CodedError{rv.GetStat()})
			os.Exit(1)
		}
	}
	d.conn.Close()

	fmt.Println("Finished creating streams")

	for z := 0; z < NUM_STREAMS; z++ {
		connections[z] = make([]*Endpoint, TCP_CONNECTIONS)
		for i := 0; i < TCP_CONNECTIONS; i++ {
			connections[z][i] = connector(dbAddr)
		}
	}

	fmt.Println("Finished creating connections")

	sig := make(chan int)
	var randGen *rand.Rand
	randGen = rand.New(rand.NewSource(seedGen.Int63()))

	perm := make([][]int64, NUM_STREAMS)

	var f int64
	var u int64
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
	//fmt.Println(perm)

	for u = 0; u < perm_size; u++ {
		currTime := perm[0][u]
		datas[u] = make([]*pb.RawPoint, POINTS_PER_MESSAGE)
		var i int
		for i = 0; uint32(i) < POINTS_PER_MESSAGE; i++ {
			if DETERMINISTIC_KV {
				datas[u][i] = &pb.RawPoint{Time: currTime, Value: get_time_value(currTime, randGen)}
			} else {
				datas[u][i] = &pb.RawPoint{Time: (currTime + int64(randGen.Float64()*MAX_TIME_RANDOM_OFFSET)), Value: get_time_value(currTime, randGen)}
			}
			currTime += NANOS_BETWEEN_POINTS
		}
	}

	fmt.Println("Finished generating data")

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

	startTime := time.Now().UnixNano()

	for z := 0; z < NUM_STREAMS; z++ {
		var sta int64 = 0
		for i := 0; i < TCP_CONNECTIONS; i++ {
			// datas sig cont
			go insert_data(datas[sta:sta+n_mes], sig, connections[z][i].g, uuids[z])
			sta += n_mes
		}
	}

	/* Handle ^C */
	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt // block until an interrupt happens
		fmt.Println("\nDetected ^C. Abruptly ending program...")
		os.Exit(0)
	}()

	for k := 0; k < NUM_STREAMS*TCP_CONNECTIONS; k++ {
		<-sig
	}

	deltaT := time.Now().UnixNano() - startTime

	// I used to close unused connections here, but now I don't bother

	fmt.Println("Finished")

	numResPoints := uint64(TOTAL_RECORDS) * uint64(NUM_STREAMS)

	var resultcount uint64 = 0
	for z := 0; z < NUM_STREAMS; z++ {

		rv, err := connections[z][0].g.Windows(context.Background(), &pb.WindowsParams{
			Uuid:         uuids[z],
			Start:        FIRST_TIME,
			End:          TOTAL_RECORDS*NANOS_BETWEEN_POINTS + 100,
			Width:        uint64(TOTAL_RECORDS*NANOS_BETWEEN_POINTS + 50),
			Depth:        0,
			VersionMajor: 0,
		})
		if err != nil {
			fmt.Println("Failed reading")
			os.Exit(1)
		}
		for {
			rawv, err := rv.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {

				fmt.Printf("Failed reading %s\n", err)
				os.Exit(1)
			}
			if rawv.Stat != nil {

				fmt.Printf("Failed reading %s\n", &CodedError{rawv.Stat})
				os.Exit(1)
			}
			for _, x := range rawv.Values {
				resultcount += x.Count
			}
		}
		for i := 0; i < TCP_CONNECTIONS; i++ {
			connections[z][i].conn.Close()
		}
	}
	if resultcount != numResPoints {
		fmt.Printf("POINTS WERE NOT WRITTEN EXPECTED %d GOT %d\n", numResPoints, resultcount)
	} else {
		fmt.Println("ALL GOOD")
	}

	fmt.Printf("Total time: %d nanoseconds for %d points\n", deltaT, numResPoints)
	var average uint64
	if numResPoints != 0 {
		average = uint64(deltaT) / numResPoints
	}
	fmt.Printf("Average: %d nanoseconds per point (floored to integer value)\n", average)
	fmt.Println(deltaT)

}

//connector
func connector(a string) *Endpoint {
	var tmt time.Duration
	tmt = 2 * time.Second

	dc := grpc.NewGZIPDecompressor()
	dialopts := []grpc.DialOption{
		grpc.WithTimeout(tmt),
		grpc.FailOnNonTempDialError(true),
		grpc.WithBlock(),
		grpc.WithDecompressor(dc),
		grpc.WithInitialWindowSize(1 * 1024 * 1024),
		grpc.WithInitialConnWindowSize(1 * 1024 * 1024)}

	dialopts = append(dialopts, grpc.WithInsecure())
	conn, err := grpc.Dial(a, dialopts...)
	if err != nil {
		fmt.Printf("endpoint error: err=%v a=%v\n", err, a)
		os.Exit(0)
	}
	client := pb.NewBTrDBClient(conn)
	rv := &Endpoint{g: client, conn: conn}
	return rv
}

func inserter(c pb.BTrDBClient, uu uuid.UUID, values []*pb.RawPoint) error {
	rv, err := c.Insert(context.Background(), &pb.InsertParams{
		Uuid:   uu,
		Sync:   false,
		Values: values,
	})
	if err != nil {
		return err
	}
	if rv.GetStat() != nil {
		return &CodedError{rv.GetStat()}
	}
	return nil
}
