all:
	go build -buildvcs=false -o gopc
	ponyc && ./producer-consumer --ponymaxthreads 1 --ponypin

## make configure runtime-bitcode=yes
## make build
## ponyc --runtimebc
## bitcode is massively slowly, like 3x slower: elapsed nanosec = 1_449_777_223

# on linux, after native build from source:
# ./rel --ponymaxthreads 1 --ponypin
# elapsed nanosec = 443_015_337  # 443 msec
# ./gopc 
# elap: 358.170828ms
# so pony about 10% slower than Go here. not bad.
