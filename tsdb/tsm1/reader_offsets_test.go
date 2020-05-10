package tsm1

import (
	"bytes"
	"fmt"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"math/rand"
	"strconv"
	"testing"
)

func TestReaderOffsets(t *testing.T) {
	const numKeys = 100

	check := func(t *testing.T, what string, got, exp interface{}, extra ...interface{}) {
		t.Helper()
		if got != exp {
			args := []interface{}{"incorrect", what, "got:", got, "exp:", exp}
			args = append(args, extra...)
			t.Fatal(args...)
		}
	}

	makeKey := func(i int) string { return fmt.Sprintf("%09d", i) }

	makeRO := func() (readerOffsets, *faultBuffer) {
		var buf []byte
		var ro readerOffsets
		for i := 0; i < numKeys; i++ {
			ro.AddKey(addKey(&buf, makeKey(i)))
		}
		ro.Done()

		return ro, &faultBuffer{b: buf}
	}

	t.Run("Create_SingleKey", func(t *testing.T) {
		var buf []byte
		var ro readerOffsets
		ro.AddKey(addKey(&buf, makeKey(0)))
		ro.Done()

		check(t, "offsets", len(ro.offsets), 1)
		check(t, "prefixes", len(ro.prefixes), 1)
	})

	t.Run("Create", func(t *testing.T) {
		ro, _ := makeRO()

		check(t, "offsets", len(ro.offsets), numKeys)
		check(t, "prefixes", len(ro.prefixes), numKeys/10)
	})

	t.Run("Iterate", func(t *testing.T) {
		ro, fb := makeRO()

		iter := ro.Iterator()
		for i := 0; iter.Next(); i++ {
			check(t, "key", string(iter.Key(fb)), makeKey(i))
		}
	})

	t.Run("Seek", func(t *testing.T) {
		ro, fb := makeRO()
		exact, ok := false, false

		iter := ro.Iterator()
		for i := 0; i < numKeys-1; i++ {
			exact, ok = iter.Seek([]byte(makeKey(i)), fb)
			check(t, "exact", exact, true)
			check(t, "ok", ok, true)
			check(t, "key", string(iter.Key(fb)), makeKey(i))

			exact, ok = iter.Seek([]byte(makeKey(i)+"0"), fb)
			check(t, "exact", exact, false)
			check(t, "ok", ok, true)
			check(t, "key", string(iter.Key(fb)), makeKey(i+1))
		}

		exact, ok = iter.Seek([]byte(makeKey(numKeys-1)), fb)
		check(t, "exact", exact, true)
		check(t, "ok", ok, true)
		check(t, "key", string(iter.Key(fb)), makeKey(numKeys-1))

		exact, ok = iter.Seek([]byte(makeKey(numKeys-1)+"0"), fb)
		check(t, "exact", exact, false)
		check(t, "ok", ok, false)

		exact, ok = iter.Seek([]byte("1"), fb)
		check(t, "exact", exact, false)
		check(t, "ok", ok, false)

		exact, ok = iter.Seek(nil, fb)
		check(t, "exact", exact, false)
		check(t, "ok", ok, true)
		check(t, "key", string(iter.Key(fb)), makeKey(0))
	})

	t.Run("Delete", func(t *testing.T) {
		ro, fb := makeRO()

		iter := ro.Iterator()
		for i := 0; iter.Next(); i++ {
			if i%2 == 0 {
				continue
			}
			iter.Delete()
		}
		iter.Done()

		iter = ro.Iterator()
		for i := 0; iter.Next(); i++ {
			check(t, "key", string(iter.Key(fb)), makeKey(2*i))
		}
	})

	t.Run("Fuzz", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			ro, fb := makeRO()
			deleted := make(map[string]struct{})
			iter := ro.Iterator()

			for i := 0; i < numKeys; i++ {
				// delete a random key. if we seek past, delete the first key.
				_, ok := iter.Seek([]byte(makeKey(rand.Intn(numKeys))), fb)
				if !ok {
					iter.Seek(nil, fb)
				}
				key := string(iter.Key(fb))
				_, ok = deleted[key]
				check(t, "key deleted", ok, false, "for key", key)
				deleted[key] = struct{}{}
				iter.Delete()
				iter.Done()

				// seek to every key that isn't deleted.
				for i := 0; i < numKeys; i++ {
					key := makeKey(i)
					if _, ok := deleted[key]; ok {
						continue
					}

					exact, ok := iter.Seek([]byte(key), fb)
					check(t, "exact", exact, true, "for key", key)
					check(t, "ok", ok, true, "for key", key)
					check(t, "key", string(iter.Key(fb)), key)
				}
			}

			check(t, "amount deleted", len(deleted), numKeys)
			iter = ro.Iterator()
			check(t, "next", iter.Next(), false)
		}
	})
}

func addKey(buf *[]byte, key string) (uint32, []byte) {
	offset := len(*buf)
	*buf = append(*buf, byte(len(key)>>8), byte(len(key)))
	*buf = append(*buf, key...)
	*buf = append(*buf, 0)
	*buf = append(*buf, make([]byte, indexEntrySize)...)

	//fmt.Printf("offset: %d, key: %v \n" , offset, []byte(key))

	return uint32(offset), []byte(key)
}

func TestEncodeName (t *testing.T) {
	orgBucket := tsdb.EncodeNameSlice(influxdb.ID(44), influxdb.ID(11))
	t.Logf("encode: %v", orgBucket)

	tags := models.NewTags(map[string]string{strconv.Itoa(int(44)):strconv.Itoa(int(11))})
	key := models.AppendMakeKey(nil, orgBucket, tags)

	t.Logf("encode key: %v", key)

	s := string(orgBucket)
	bs := []byte(s)

	t.Logf("encode bs: %v", bs)


}


func BenchmarkReaderOffsets(b *testing.B) {

	makeKey := func(org uint64, bucket uint64) string{
		orgBucket := tsdb.EncodeNameSlice(influxdb.ID(org), influxdb.ID(bucket))
		tags := models.NewTags(map[string]string{strconv.Itoa(int(org)):strconv.Itoa(int(bucket))})
		key := models.AppendMakeKey(nil, orgBucket, tags)
		return string(key)
	}

	makeRO := func(orgN uint64, bucketN uint64) (readerOffsets, *faultBuffer) {
		var buf []byte
		var ro readerOffsets
		for i := uint64(0); i < orgN; i++ {
			for j := uint64(0); j < bucketN; j++ {
				k := makeKey(i, j)
				ro.AddKey(addKey(&buf, k))

				//ro.AddKey(addKey(&buf, makeKey(i, j)))
			}
		}
		ro.Done()

		return ro, &faultBuffer{b: buf}
	}

	cases := []int{
		//5,
		//50,

		500,
		//1000,
	}

	// n = 5, prefix = 8
	// 1000000000	         0.00753 ns/op	       0 B/op	       0 allocs/op
	// 1000000000	         0.00657 ns/op	       0 B/op	       0 allocs/op

	// 50, prefix = 8 | 16
	// 1000000000	         0.000197 ns/op	       0 B/op	       0 allocs/op
	// 1000000000	         0.000451 ns/op	       0 B/op	       0 allocs/op

	// 500, prefix = 8 | 16
	// 1000000000	         0.00527 ns/op	       0 B/op	       0 allocs/op
	// 1000000000	         0.0436 ns/op	       0 B/op	       0 allocs/op

	for _, n := range cases {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {

			ro, fb := makeRO(uint64(n), uint64(n))
			exact, ok := false, false

			b.ResetTimer()
			b.ReportAllocs()

			fmt.Printf("--- run case \n")
			//fmt.Printf("--- prefixs: %v \n", ro.prefixes)

			fmt.Printf("--- run case, n: %d \n", n)

			iter := ro.Iterator()
			for i := 0; i < 10000; i++ {
				org := uint64(rand.Intn(n))
				bucket := uint64(rand.Intn(n))

				k := []byte(makeKey(org, bucket))

				exact, ok = iter.Seek(k, fb)

				if !exact || !ok {
					idx := bytes.Index(fb.b, k)
					fmt.Printf("makeKey k: %v \n" , k)
					fmt.Printf("makeKey org: %d, bkt: %d \n" , org, bucket)
					fmt.Printf("makeKey k: %v \n" , k)
					fmt.Printf("makeKey idx: %v \n" , idx)
					fmt.Printf("makeKey key in buf: %v \n" , fb.b[idx:idx + len(k)])

					fmt.Println(exact, ok)
					b.Fatal(exact)
				}

			}

		})
	}
}

//func TOODbenchmarkRingkeys(b *testing.B, r *ring, keys int) {
//	// Add some keys
//	for i := 0; i < keys; i++ {
//		r.add([]byte(fmt.Sprintf("cpu,host=server-%d value=1", i)), new(entry))
//	}
//
//	b.ReportAllocs()
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		strSliceRes = r.keys(false)
//	}
//}

