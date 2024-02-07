CXX=g++

CXXFLAGS = -O -pthread -std=c++17

SRC_DIR = src

OBJ_DIR = .

TARGET = $(OBJ_DIR)/psrs

SRC = $(SRC_DIR)/main.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm -f $(TARGET)