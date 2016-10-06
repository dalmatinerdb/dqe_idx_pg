REBAR = $(shell pwd)/rebar3

all:
	@$(REBAR) compile

eqc_test:
	# Uses the test environment set up with setup_test_db.sh
	rm -rf datadir
	./priv/setup_test_db.sh
	@$(REBAR) as eqc eqc
	./priv/stop_test_db.sh
