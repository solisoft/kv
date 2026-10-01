// loadgen: targeted SoliKV scenarios that redis-benchmark cannot express.
// Each scenario prints one JSON line: {"name":..., "value":..., "unit":..., "note":...}
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type conn struct {
	c net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

func dial(addr string) *conn {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	return &conn{c: c, r: bufio.NewReaderSize(c, 1<<20), w: bufio.NewWriterSize(c, 1<<20)}
}

func (c *conn) send(args ...string) {
	fmt.Fprintf(c.w, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(c.w, "$%d\r\n%s\r\n", len(a), a)
	}
}

// reply reads one RESP2 reply and returns it as a Go value.
func (c *conn) reply() (interface{}, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return nil, errors.New(line[1:])
	case ':':
		n, _ := strconv.ParseInt(line[1:], 10, 64)
		return n, nil
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := ioReadFull(c.r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil, nil
		}
		out := make([]interface{}, n)
		for i := 0; i < n; i++ {
			v, err := c.reply()
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	}
	return nil, fmt.Errorf("bad reply %q", line)
}

func ioReadFull(r *bufio.Reader, b []byte) (int, error) {
	n := 0
	for n < len(b) {
		m, err := r.Read(b[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (c *conn) do(args ...string) (interface{}, error) {
	c.send(args...)
	c.w.Flush()
	return c.reply()
}

func must(v interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return v
}

type result struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
	Unit  string  `json:"unit"`
	Note  string  `json:"note,omitempty"`
}

func emit(name string, value float64, unit, note string) {
	b, _ := json.Marshal(result{name, value, unit, note})
	fmt.Println(string(b))
}

// pipelined sends cmds in batches of `depth`, returns elapsed.
func pipelined(c *conn, n, depth int, mk func(i int) []string) time.Duration {
	t := time.Now()
	for i := 0; i < n; i += depth {
		k := depth
		if i+k > n {
			k = n - i
		}
		for j := 0; j < k; j++ {
			c.send(mk(i + j)...)
		}
		c.w.Flush()
		for j := 0; j < k; j++ {
			if _, err := c.reply(); err != nil {
				panic(fmt.Sprintf("%v (cmd %v)", err, mk(i+j)))
			}
		}
	}
	return time.Since(t)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:6390", "server")
	only := flag.String("only", "", "comma-separated scenario names")
	flag.Parse()
	want := map[string]bool{}
	for _, s := range strings.Split(*only, ",") {
		if s != "" {
			want[s] = true
		}
	}
	run := func(name string, f func(c *conn)) {
		if len(want) > 0 && !want[name] {
			return
		}
		c := dial(*addr)
		must(c.do("FLUSHALL"))
		f(c)
		must(c.do("FLUSHALL"))
		c.c.Close()
	}

	// 1. One huge RPUSH (100k elements, ~1.2 MB frame) — decoder cost.
	run("bigframe_rpush", func(c *conn) {
		args := []string{"RPUSH", "biglist"}
		for i := 0; i < 100000; i++ {
			args = append(args, "element-"+strconv.Itoa(i))
		}
		const reps = 10
		t := time.Now()
		for r := 0; r < reps; r++ {
			must(c.do(args...))
		}
		emit("bigframe_rpush", float64(time.Since(t).Microseconds())/1000/reps, "ms/cmd", "RPUSH with 100k elements (~1.3 MB frame)")
	})

	// 2. MSET 10k keys in one command.
	run("bigframe_mset", func(c *conn) {
		args := []string{"MSET"}
		for i := 0; i < 10000; i++ {
			args = append(args, "mk:"+strconv.Itoa(i), "value-"+strconv.Itoa(i))
		}
		const reps = 20
		t := time.Now()
		for r := 0; r < reps; r++ {
			must(c.do(args...))
		}
		emit("bigframe_mset", float64(time.Since(t).Microseconds())/1000/reps, "ms/cmd", "MSET of 10k key/value pairs")
	})

	// 3. APPEND growth: 20k x 512 B onto one key (10 MB).
	run("append_growth", func(c *conn) {
		chunk := strings.Repeat("x", 512)
		d := pipelined(c, 20000, 64, func(i int) []string { return []string{"APPEND", "grow", chunk} })
		emit("append_growth", d.Seconds()*1000, "ms", "20k APPENDs of 512 B to one key (final 10 MB)")
	})

	// 4. SETBIT growth: 100k bits up to offset 80M (10 MB bitmap).
	run("setbit_growth", func(c *conn) {
		d := pipelined(c, 100000, 64, func(i int) []string {
			return []string{"SETBIT", "bm", strconv.Itoa(i * 800), "1"}
		})
		emit("setbit_growth", d.Seconds()*1000, "ms", "100k SETBITs at rising offsets (final 10 MB)")
	})

	// 5. Sorted set rank queries on 200k members.
	run("zset_rank", func(c *conn) {
		pipelined(c, 200000, 256, func(i int) []string {
			return []string{"ZADD", "z", strconv.Itoa(i), "m" + strconv.Itoa(i)}
		})
		rng := rand.New(rand.NewSource(1))
		const n = 20000
		d := pipelined(c, n, 64, func(i int) []string {
			return []string{"ZRANK", "z", "m" + strconv.Itoa(rng.Intn(200000))}
		})
		emit("zset_zrank", float64(n)/d.Seconds(), "ops/s", "ZRANK on a 200k-member zset")
		const m = 5000
		d = pipelined(c, m, 64, func(i int) []string {
			s := 100000 + rng.Intn(90000)
			return []string{"ZRANGE", "z", strconv.Itoa(s), strconv.Itoa(s + 9)}
		})
		emit("zset_zrange_offset", float64(m)/d.Seconds(), "ops/s", "ZRANGE start..start+9 deep inside a 200k zset")
	})

	// 6. Full ZSCAN / HSCAN iteration of 50k entries.
	run("scan_iter", func(c *conn) {
		pipelined(c, 50000, 256, func(i int) []string {
			return []string{"ZADD", "zs", strconv.Itoa(i), "m" + strconv.Itoa(i)}
		})
		pipelined(c, 50000, 256, func(i int) []string {
			return []string{"HSET", "hs", "f" + strconv.Itoa(i), "v"}
		})
		for _, cmd := range []string{"ZSCAN", "HSCAN"} {
			key := map[string]string{"ZSCAN": "zs", "HSCAN": "hs"}[cmd]
			t := time.Now()
			cursor := "0"
			calls := 0
			for {
				v := must(c.do(cmd, key, cursor, "COUNT", "100")).([]interface{})
				calls++
				cursor = v[0].(string)
				if cursor == "0" || calls > 100000 {
					break
				}
			}
			emit(strings.ToLower(cmd)+"_full", time.Since(t).Seconds()*1000, "ms", cmd+" COUNT 100 over 50k entries, full iteration")
		}
	})

	// 7. SPOP drain of 100k members.
	run("spop_drain", func(c *conn) {
		pipelined(c, 100000, 256, func(i int) []string {
			return []string{"SADD", "sp", "m" + strconv.Itoa(i)}
		})
		d := pipelined(c, 100000, 64, func(i int) []string { return []string{"SPOP", "sp"} })
		emit("spop_drain", d.Seconds()*1000, "ms", "SPOP until a 100k set is empty")
	})

	// 8. SINTER of a 100k set with a 100-member set.
	run("sinter", func(c *conn) {
		pipelined(c, 100000, 256, func(i int) []string {
			return []string{"SADD", "{s}big", "m" + strconv.Itoa(i)}
		})
		pipelined(c, 100, 100, func(i int) []string {
			return []string{"SADD", "{s}small", "m" + strconv.Itoa(i*7)}
		})
		const n = 2000
		d := pipelined(c, n, 16, func(i int) []string { return []string{"SINTER", "{s}big", "{s}small"} })
		emit("sinter", float64(n)/d.Seconds(), "ops/s", "SINTER of 100k-member and 100-member sets")
	})

	// 9. DEL 1000 keys per call.
	run("multi_del", func(c *conn) {
		const batches = 200
		var total time.Duration
		for b := 0; b < batches; b++ {
			args := []string{"MSET"}
			for i := 0; i < 1000; i++ {
				args = append(args, "d:"+strconv.Itoa(i), "v")
			}
			must(c.do(args...))
			del := []string{"DEL"}
			for i := 0; i < 1000; i++ {
				del = append(del, "d:"+strconv.Itoa(i))
			}
			t := time.Now()
			must(c.do(del...))
			total += time.Since(t)
		}
		emit("multi_del", float64(total.Microseconds())/batches, "µs/cmd", "DEL of 1000 keys spread across shards")
	})

	// 10. Tail latency of GETs while a snapshot runs (1M keys, 64 B values).
	saveLatency := func(c *conn, saveCmd, prefix, what, during string) {
		val := strings.Repeat("v", 64)
		pipelined(c, 1000000, 512, func(i int) []string {
			return []string{"SET", "k:" + strconv.Itoa(i), val}
		})
		var lat []time.Duration
		var mu sync.Mutex
		var stop atomic.Bool
		var wg sync.WaitGroup
		for w := 0; w < 16; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				cc := dial(*addr)
				defer cc.c.Close()
				rng := rand.New(rand.NewSource(int64(w)))
				local := make([]time.Duration, 0, 100000)
				for !stop.Load() {
					t := time.Now()
					must(cc.do("GET", "k:"+strconv.Itoa(rng.Intn(1000000))))
					local = append(local, time.Since(t))
				}
				mu.Lock()
				lat = append(lat, local...)
				mu.Unlock()
			}(w)
		}
		time.Sleep(300 * time.Millisecond)
		sc := dial(*addr)
		for i := 0; i < 3; i++ {
			sc.do(saveCmd)
			time.Sleep(400 * time.Millisecond)
		}
		sc.c.Close()
		time.Sleep(300 * time.Millisecond)
		stop.Store(true)
		wg.Wait()
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		pct := func(p float64) float64 { return float64(lat[int(float64(len(lat)-1)*p)].Microseconds()) / 1000 }
		emit(prefix+"_p99", pct(0.99), "ms", "GET p99 with 16 clients while 3 "+what+" of 1M keys run")
		emit(prefix+"_p999", pct(0.999), "ms", "GET p99.9 during "+during)
		emit(prefix+"_max", pct(1.0), "ms", "GET max latency during "+during)
	}
	run("save_latency", func(c *conn) { saveLatency(c, "BGSAVE", "save_latency", "BGSAVEs", "BGSAVE") })

	// 10b. The same during SAVE (the caller waits; the others should not).
	run("save_cmd_latency", func(c *conn) { saveLatency(c, "SAVE", "save_cmd_latency", "SAVEs", "SAVE") })

	// 10c. BGSAVE against a --solo server. Opt-in only (-only save_latency_solo):
	// the server must have been started with --solo.
	if want["save_latency_solo"] {
		run("save_latency_solo", func(c *conn) {
			saveLatency(c, "BGSAVE", "save_latency_solo", "BGSAVEs on a --solo server", "BGSAVE (--solo)")
		})
	}

	// 10e. One huge list (BIGKEY_N elements, default 5M) and clients on other
	// keys of the SAME shard ({big} hashtag): their latency while
	// BGREWRITEAOF, then BGSAVE, serializes the shard. Opt-in only
	// (-only bigkey_rewrite).
	if want["bigkey_rewrite"] {
		run("bigkey_rewrite", func(c *conn) {
			n := 5000000
			if v, err := strconv.Atoi(os.Getenv("BIGKEY_N")); err == nil && v > 0 {
				n = v
			}
			const per = 1000
			pipelined(c, n/per, 8, func(i int) []string {
				args := []string{"RPUSH", "{big}:list"}
				for j := 0; j < per; j++ {
					args = append(args, "element-"+strconv.Itoa(i*per+j))
				}
				return args
			})
			for w := 0; w < 8; w++ {
				must(c.do("SET", "{big}:k"+strconv.Itoa(w), "v"))
			}
			inProgress := func(field string) bool {
				info := must(c.do("INFO", "persistence")).(string)
				return strings.Contains(info, field+":1")
			}
			for _, op := range []struct{ cmd, field, name string }{
				{"BGREWRITEAOF", "aof_rewrite_in_progress", "bigkey_bgrewriteaof"},
				{"BGSAVE", "rdb_bgsave_in_progress", "bigkey_bgsave"},
			} {
				// Let an automatic rewrite or earlier save finish first.
				for inProgress("aof_rewrite_in_progress") || inProgress("rdb_bgsave_in_progress") {
					time.Sleep(50 * time.Millisecond)
				}
				var lat []time.Duration
				var mu sync.Mutex
				var stop atomic.Bool
				var wg sync.WaitGroup
				for w := 0; w < 8; w++ {
					wg.Add(1)
					go func(w int) {
						defer wg.Done()
						cc := dial(*addr)
						defer cc.c.Close()
						local := make([]time.Duration, 0, 100000)
						k := "{big}:k" + strconv.Itoa(w)
						l := "{big}:l" + strconv.Itoa(w)
						for i := 0; !stop.Load(); i++ {
							t := time.Now()
							if i%2 == 0 {
								must(cc.do("GET", k))
							} else {
								must(cc.do("LPUSH", l, "x"))
							}
							local = append(local, time.Since(t))
						}
						mu.Lock()
						lat = append(lat, local...)
						mu.Unlock()
					}(w)
				}
				time.Sleep(300 * time.Millisecond)
				t0 := time.Now()
				c.do(op.cmd)
				for {
					time.Sleep(20 * time.Millisecond)
					if !inProgress(op.field) {
						break
					}
				}
				took := time.Since(t0)
				time.Sleep(300 * time.Millisecond)
				stop.Store(true)
				wg.Wait()
				sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
				pct := func(p float64) float64 { return float64(lat[int(float64(len(lat)-1)*p)].Microseconds()) / 1000 }
				note := fmt.Sprintf("GET/LPUSH on other keys of the shard holding a %d-element list, during %s", n, op.cmd)
				emit(op.name+"_p999", pct(0.999), "ms", note+" (p99.9)")
				emit(op.name+"_max", pct(1.0), "ms", note+" (max)")
				emit(op.name+"_took", float64(took.Milliseconds()), "ms", op.cmd+" duration")
			}
		})
	}

	// 10d. Refill right after a FLUSHALL of 1M keys: 16 clients pipelining SETs
	// (depth 16) for 2 s; per-batch latency. Catches stalls from freeing the
	// flushed dataset.
	run("flush_refill", func(c *conn) {
		val := strings.Repeat("v", 64)
		if os.Getenv("NOPREFILL") == "" {
			pipelined(c, 1000000, 512, func(i int) []string {
				return []string{"SET", "k:" + strconv.Itoa(i), val}
			})
		}
		var lat []time.Duration
		var mu sync.Mutex
		var stop atomic.Bool
		var wg sync.WaitGroup
		nc := 16
		if v, err := strconv.Atoi(os.Getenv("REFILL_CLIENTS")); err == nil && v > 0 {
			nc = v
		}
		conns := make([]*conn, nc)
		for w := range conns {
			conns[w] = dial(*addr)
		}
		tf := time.Now()
		must(c.do("FLUSHALL"))
		t0 := time.Now()
		if os.Getenv("SPIKES") != "" {
			fmt.Fprintf(os.Stderr, "flushall took %v\n", t0.Sub(tf))
		}
		for w := 0; w < nc; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				cc := conns[w]
				defer cc.c.Close()
				local := make([]time.Duration, 0, 100000)
				n := 0
				for !stop.Load() {
					t := time.Now()
					for j := 0; j < 16; j++ {
						cc.send("SET", "r:"+strconv.Itoa(w)+":"+strconv.Itoa(n), val)
						n++
					}
					cc.w.Flush()
					for j := 0; j < 16; j++ {
						if _, err := cc.reply(); err != nil {
							panic(err)
						}
					}
					el := time.Since(t)
					local = append(local, el)
					if el > 20*time.Millisecond && os.Getenv("SPIKES") != "" {
						fmt.Fprintf(os.Stderr, "spike w=%d at=%v dur=%v n=%d\n", w, t.Sub(t0), el, n)
					}
				}
				mu.Lock()
				lat = append(lat, local...)
				mu.Unlock()
			}(w)
		}
		dur := 2 * time.Second
		if v, err := time.ParseDuration(os.Getenv("REFILL_DUR")); err == nil {
			dur = v
		}
		time.Sleep(dur)
		stop.Store(true)
		wg.Wait()
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		pct := func(p float64) float64 { return float64(lat[int(float64(len(lat)-1)*p)].Microseconds()) / 1000 }
		emit("flush_refill_p999", pct(0.999), "ms", "pipelined SET batch p99.9, 16 clients, right after FLUSHALL of 1M keys")
		emit("flush_refill_max", pct(1.0), "ms", "pipelined SET batch max latency right after FLUSHALL of 1M keys")
	})

	// 11. Expired-key reclamation on an idle server.
	run("expiry_idle", func(c *conn) {
		pipelined(c, 200000, 512, func(i int) []string {
			return []string{"SET", "e:" + strconv.Itoa(i), "v", "PX", "200"}
		})
		time.Sleep(3 * time.Second)
		n := must(c.do("DBSIZE")).(int64)
		emit("expiry_idle_leftover", float64(n), "keys", "keys still resident 3 s after 200k keys expired (idle server)")
	})
}
