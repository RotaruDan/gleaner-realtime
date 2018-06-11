#!/usr/bin/make -f

.PHONY: all image clean

all: noop

clean:
	rm -f noop *.o

%.o: %.asm
	nasm -f elf $< -o $@

noop: noop.o
	gcc -Os -m32 -Wall -s -nostdlib $< -o $@

image: noop
	docker build -t noop .

