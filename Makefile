compile:
	cargo b

echo: compile
	./maelstrom/maelstrom test -w echo --bin ./target/debug/echo --node-count 1 --time-limit 10

unique-ids: compile
	./maelstrom/maelstrom test -w unique-ids --bin ./target/debug/unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

broadcast: compile
	./maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 5 --time-limit 20 --rate 10

efficient-broadcast: compile
	./maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100

broadcast-part: compile
	./maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition

grow-counter: compile
	./maelstrom/maelstrom test -w g-counter --bin ./target/debug/counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition

web:
	./maelstrom/maelstrom serve

fmt :
	cargo fmt

clippy :
	cargo clippy --all-features --all-targets -- -D warnings
