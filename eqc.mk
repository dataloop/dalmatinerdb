ROOT_DIR 	 ?= .
EQC_DOWNLOAD := http://www.quviq.com/wp-content/uploads/2015/09/eqcmini-2.01.0.zip

eqc: $(ROOT_DIR)/_checkouts/eqc

$(ROOT_DIR)/_checkouts/eqc: $(ROOT_DIR)/_checkouts/eqc-2.01.0
	cd $(ROOT_DIR)/_checkouts && ln -s eqc-2.01.0 eqc

$(ROOT_DIR)/_checkouts/eqc-2.01.0: $(ROOT_DIR)/_checkouts/eqcmini-2.01.0.zip
	cd $(ROOT_DIR)/_checkouts && unzip -o eqcmini-2.01.0.zip > /dev/null 2>&1

$(ROOT_DIR)/_checkouts/eqcmini-2.01.0.zip:
	mkdir -p $$(dirname $@)
	wget -O $@ $(EQC_DOWNLOAD)
