REBAR = $(shell pwd)/rebar3

all:
	@$(REBAR) compile

eqc-test:
	# Uses the test environment set up with setup_test_db.sh
	# Sometimes the test fails so we need to cleanup both before and after
	./priv/stop_test_db.sh
	./priv/setup_test_db.sh
	@$(REBAR) as eqc eqc
	./priv/stop_test_db.sh
