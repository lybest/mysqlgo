CPP=g++
CPPFLAGS= -g -std=c++11 $(INCLUDE)
LIBS= -L/usr/local/lib/ -L/usr/lib/  -llibgo -lmysqlcppconn -lboost_thread -lboost_system
INCLUDE= -I/usr/local/include/libgo
TARGET=testmysql
OBJECTS = main.o sql.o

all: $(TARGET)

$(TARGET) : $(OBJECTS)
	$(CPP) $(CPPFLAGS)$(TARGET) $(OBJECTS) $(LIBS) 

