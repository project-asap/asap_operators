.PHONY: test
test:
	make -C compiler test
	make -C kmeans test
	make -C tfidf test
	make -C src test

.PHONY: clean
clean:
	make -C compiler clean
	make -C kmeans clean
	make -C tfidf clean
	make -C src clean
