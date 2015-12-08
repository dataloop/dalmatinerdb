.PHONY: all tree

all: version compile

include fifo.mk

version:
	@echo "$(shell git symbolic-ref HEAD 2> /dev/null | cut -b 12-)-$(shell git log --pretty=format:'%h, %ad' -1)" > dalmatiner_db.version

version_header: version
	@echo "-define(VERSION, <<\"$(shell cat dalmatiner_db.version)\">>)." > apps/dalmatiner_db/include/dalmatiner_db_version.hrl

clean:
	$(REBAR) clean
	make -C rel/pkg clean

rel: update
	$(REBAR) as prod compile
	$(REBAR) as prod release

package: rel
	make -C rel/pkg package

##
## Developer targets
##

devrel: dev1 dev2 dev3 dev4

devclean:
	rm -rf _build/dev*

dev1 dev2 dev3 dev4: all
	$(REBAR) as $@ release

xref: all
	$(REBAR) xref
