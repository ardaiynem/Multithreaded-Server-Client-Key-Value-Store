all: serverk clientk

serverk: serverk.c hashTable
	gcc -Wall -g -o serverk serverk.c hashTable.c hashTable.h
	
clientk: clientk.c
	gcc -Wall -g -o clientk clientk.c

hashTable: hashTable.c hashTable.h
	
clean:
	rm -fr serverk serverk.o clientk clientk.o *~
