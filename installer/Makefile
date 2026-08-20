CXX ?= c++
CXXFLAGS ?= -O2 -Wall -Wextra -Wpedantic -std=c++17
TARGET := fsp-installer

.PHONY: all clean

all: $(TARGET)

$(TARGET): main.cpp
	$(CXX) $(CXXFLAGS) $< -o $@

clean:
	rm -f $(TARGET)
