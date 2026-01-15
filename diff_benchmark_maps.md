
### Скорость (sec/op)

| Операция | mutex   | sync.Map | Разница |
  |----------|---------|----------|---------|
| Read (10p) | 38.6ns  | 1.5ns ✅    | **-96%** |
| Read (100p) | 36.9ns  | 1.7ns ✅    | **-95%** |
| Read (1000p) | 36.7ns  | 1.9ns ✅    | **-95%** |
| Write (10p) | 205ns ✅ | 255ns    | +24% |
| Write (100p) | 244ns   | 89ns ✅     | **-63%** |
| Write (1000p) | 303ns   | 44ns ✅     | **-86%** |
| Delete (10p) | 736ns ✅ | 838ns    | +14% |
| Delete (100p) | 11.6µs ✅  | 14.5µs   | +25% |
| Delete (1000p) | 121µs ✅   | 157µs    | +29% |
| Copy (10p) | 219ns ✅   | 271ns    | +24% |
| Copy (100p) | 1.4µs ✅   | 2.6µs    | +86% |
| Copy (1000p) | 29.9µs ✅  | 49.5µs   | +66% |
| Mixed (10p) | 270ns   | 77ns ✅     | **-71%** |
| Mixed (100p) | 1.2µs   | 325ns ✅    | **-73%** |
| Mixed (1000p) | 10.3µs  | 4.7µs ✅    | **-54%** |
| Contention (32g) | 1.4µs   | 315ns ✅    | **-77%** |

### Аллокации (B/op)

| Операция | mutex | sync.Map |
  |----------|-------|----------|
| Write | 0 ✅     | 72       |
| Copy (1000p) | 80KB ✅  | 157KB    |


go test -bench=. -benchmem -count=5 -run=^$ ./... > benchmark_mutex.txt

go test -bench=. -benchmem -count=5 -tags=syncmap -run=^$ ./... > benchmark_syncmap.txt

benchstat benchmark_mutex.txt benchmark_syncmap.txt
