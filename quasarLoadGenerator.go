package main

import (
	"fmt"
	"math"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	capnp "github.com/glycerine/go-capnproto"
	uuid "code.google.com/p/go-uuid/uuid"
)

const (
	TOTAL_RECORDS = 100
	TCP_CONNECTIONS = 1
	POINTS_PER_MESSAGE = 11
	NANOS_BETWEEN_POINTS = 9000000
	DB_ADDR = "localhost:4410"
	NUM_STREAMS = 1
)

var (
	FIRST_TIME = time.Now().UnixNano()
)

var points_sent uint32 = 0

var points_received uint32 = 0

type MessagePart struct {
	segment *capnp.Segment
	request *Request
	insert *CmdInsertValues
	recordList *Record_List
	pointerList *capnp.PointerList
	record *Record
}

var messagePool sync.Pool = sync.Pool{
	New: func () interface{} {
		var seg *capnp.Segment = capnp.NewBuffer(nil)
		var req Request = NewRootRequest(seg)
		var insert CmdInsertValues = NewCmdInsertValues(seg)
		insert.SetSync(false)
		var recList Record_List = NewRecordList(seg, POINTS_PER_MESSAGE)
		var pointList capnp.PointerList = capnp.PointerList(recList)
		var record Record = NewRecord(seg)
		return MessagePart{
			segment: seg,
			request: &req,
			insert: &insert,
			recordList: &recList,
			pointerList: &pointList,
			record: &record,
		}
	},
}

func get_time_value (time int64) float64 {
	return math.Sqrt(float64(time))
}

func min64 (x1 int64, x2 int64) int64 {
	if x1 < x2 {
		return x1
	} else {
		return x2
	}
}

func send_messages(uuid []byte, start int64, connection net.Conn, sendLock *sync.Mutex, connLock *sync.Mutex, connID int, response chan int) {
	var id uint64 = 0
	var time int64 = start
	var endTime int64
	var numPoints uint32 = POINTS_PER_MESSAGE
	if TOTAL_RECORDS < 0 {
		endTime = 0x7FFFFFFFFFFFFFFF
	} else {
		endTime = min64(start + TOTAL_RECORDS * NANOS_BETWEEN_POINTS, 0x7FFFFFFFFFFFFFFF)
	}
	for time < endTime {
		var mp MessagePart = messagePool.Get().(MessagePart)
		
		segment := mp.segment
		request := *mp.request
		insert := *mp.insert
		recordList := *mp.recordList
		pointerList := *mp.pointerList
		record := *mp.record
		
		request.SetEchoTag(id)
		insert.SetUuid(uuid)

		if endTime - time < POINTS_PER_MESSAGE * NANOS_BETWEEN_POINTS {
			numPoints = uint32((endTime - time) / NANOS_BETWEEN_POINTS)
			recordList = NewRecordList(segment, int(numPoints))
			pointerList = capnp.PointerList(recordList)
		}

		var i int
		for i = 0; uint32(i) < numPoints; i++ {
			record.SetTime(time)
			record.SetValue(get_time_value(time))
			pointerList.Set(i, capnp.Object(record))
			time += NANOS_BETWEEN_POINTS
		}
		insert.SetValues(recordList)
		request.SetInsertValues(insert)
		
		var sendErr error
		
		(*sendLock).Lock()
		_, sendErr = segment.WriteTo(connection)
		(*sendLock).Unlock()

		messagePool.Put(mp)
		
		if sendErr != nil {
			fmt.Printf("Error in sending request: %v\n", sendErr)
			return
		}
		atomic.AddUint32(&points_sent, uint32(numPoints))

		(*connLock).Lock()
		responseSegment, respErr := capnp.ReadFromStream(connection, nil)
		(*connLock).Unlock()
		
		if respErr != nil {
			fmt.Printf("Error in receiving response: %v\n", respErr)
			response <- -1
			return
		}
		
		response := ReadRootResponse(responseSegment)
		status := response.StatusCode()
		if status == STATUSCODE_OK {
			atomic.AddUint32(&points_received, uint32(numPoints))
		} else {
			fmt.Printf("Quasar returns status code %s!\n", status)
		}
		id++
	}
	response <- connID
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var connections []net.Conn = make([]net.Conn, TCP_CONNECTIONS)
	var sendLocks []*sync.Mutex = make([]*sync.Mutex, TCP_CONNECTIONS)
	var recvLocks []*sync.Mutex = make([]*sync.Mutex, TCP_CONNECTIONS)
	var err error
	fmt.Println("Creating connections...")
	for i := range connections {
		connections[i], err = net.Dial("tcp", DB_ADDR)
		if err == nil {
			fmt.Printf("Created connection %v\n", i)
			sendLocks[i] = &sync.Mutex{}
			recvLocks[i] = &sync.Mutex{}
		} else {
			fmt.Printf("Could not connect to database: %s\n", err)
			return
		}
	}
	fmt.Println("Finished creating connections")

	var connIndex int = 0

	var sig chan int = make(chan int)
	var usingConn []int = make([]int, TCP_CONNECTIONS)
	
	for j := 0; j < NUM_STREAMS; j++ {
		go send_messages([]byte(uuid.NewRandom()), FIRST_TIME, connections[connIndex], sendLocks[connIndex], recvLocks[connIndex], connIndex, sig)
		usingConn[connIndex]++
		connIndex = (connIndex + 1) % TCP_CONNECTIONS
	}

	go func () {
		for {
			time.Sleep(time.Second)
			fmt.Printf("Sent %v, ", points_sent)
			atomic.StoreUint32(&points_sent, 0)
			fmt.Printf("Received %v\n", points_received)
			atomic.StoreUint32(&points_received, 0)
			points_received = 0
		}
	}()

	var response int
	for k := 0; k < NUM_STREAMS; k++ {
		response = <-sig
		if response < 0 {
			for m := 0; m < TCP_CONNECTIONS; m++ {
				if usingConn[m] != 0 {
					connections[m].Close()
				}
			}
			break
		} else {
			usingConn[response]--
			if usingConn[response] == 0 {
				connections[response].Close()
				fmt.Printf("Closed connection %v\n", response)
			}
		}
	}

	for k := NUM_STREAMS; k < TCP_CONNECTIONS; k++ {
		connections[k].Close()
		fmt.Printf("Closed connection %v\n", k)
	}

	fmt.Printf("Sent %v, Received %v\n", points_sent, points_received)
	fmt.Println("Finished")
}
