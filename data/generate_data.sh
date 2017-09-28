#!/bin/bash

for I in 1 10 100; do
    dd if=/dev/urandom of=$(pwd)/$I"mb.bin" count=$I bs=1048576
done
